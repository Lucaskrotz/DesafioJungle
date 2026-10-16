import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import type { UnitOfWork } from '../../src/application/ports';
import { SubmitWagerTransactionUseCase } from '../../src/application/submit-wager-transaction';
import { createApp } from '../../src/bootstrap';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { setLogDestination } from '../../src/infrastructure/observability/logger';
import type { PromMetrics } from '../../src/infrastructure/observability/metrics';
import { Sqs } from '../../src/infrastructure/sqs/sqs.client';
import { WagerConsumer, type Outcome } from '../../src/infrastructure/sqs/wager-consumer';
import { METRICS, UNIT_OF_WORK } from '../../src/infrastructure/tokens';
import { OutboxPublisher } from '../../src/infrastructure/workers/outbox-publisher';
import { eventually } from '../support/cluster';
import { assertWalletConsistent } from '../support/invariants';
import { deleteQueues, sendMessage, tempFifo, tempQueues, type Queues } from '../support/sqs';

// Requer Postgres migrado e LocalStack. A app sobe sem workers; a DLQ observada pelo gauge é a temporária do teste.
let app: INestApplication;
let base: string;
let orm: Orm;
let sqs: Sqs;
let qs: Queues;
let events: { name: string; url: string };
let metrics: PromMetrics;
let prevDlq: string | undefined;
const logs: string[] = [];

beforeAll(async () => {
  sqs = new Sqs();
  qs = await tempQueues(sqs, { visibility: 30 });
  events = await tempFifo(sqs, 'obs-events');
  prevDlq = process.env.SQS_TRANSACTIONS_DLQ;
  process.env.SQS_TRANSACTIONS_DLQ = qs.dlqName; // lido na criação do módulo de métricas
  setLogDestination({ write: (l: string) => void logs.push(l) });
  app = await createApp();
  await app.listen(0);
  base = (await app.getUrl()).replace('[::1]', 'localhost');
  metrics = app.get<PromMetrics>(METRICS);
  orm = await createOrm();
});
afterAll(async () => {
  await app.close();
  setLogDestination();
  if (prevDlq === undefined) delete process.env.SQS_TRANSACTIONS_DLQ;
  else process.env.SQS_TRANSACTIONS_DLQ = prevDlq;
  await deleteQueues(sqs, [...qs.urls, events.url]);
  sqs.client.destroy();
  await orm.close();
});

const uid = () => crypto.randomUUID();
const money = (amount: string) => ({ amount, currency: 'BRL' });
const q = (sql: string, p: unknown[] = []) => orm.em.getConnection().execute(sql, p) as Promise<any[]>;
const parsedLogs = () => logs.map((l) => JSON.parse(l) as Record<string, any>);

async function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(base + path, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { res, status: res.status, json: (await res.json()) as any };
}

async function newWallet(initial: string) {
  const playerId = uid();
  const { status, json } = await call('POST', '/wallets', { playerId, initialBalance: money(initial) });
  expect(status).toBe(201);
  const walletId = json.id as string;
  const roundId = uid();
  const wager = (kind: string, amount: string, ext: string, ref?: string) => ({
    providerId: 'prov-obs',
    externalTransactionId: `${walletId}:${ext}`,
    playerId,
    walletId,
    roundId,
    gameId: 'g-obs',
    kind,
    money: money(amount),
    ...(ref ? { referenceExternalTransactionId: `${walletId}:${ref}` } : {}),
  });
  return { walletId, playerId, wager };
}

const submit = (body: { providerId: string; externalTransactionId: string; [k: string]: unknown }, headers: Record<string, string> = {}) =>
  call('POST', '/wagering/transactions', body, { 'idempotency-key': `${body.providerId}:${body.externalTransactionId}`, ...headers });
const reconcile = (walletId: string) => call('POST', `/wallets/${walletId}/reconciliation`);

// ---- parse do formato texto do Prometheus
type Sample = { name: string; labels: Record<string, string>; value: number };
async function scrape(): Promise<{ text: string; samples: Sample[] }> {
  const res = await fetch(`${base}/metrics`);
  expect(res.status).toBe(200);
  expect(res.headers.get('content-type')).toContain('text/plain');
  const text = await res.text();
  const samples: Sample[] = [];
  for (const line of text.split('\n')) {
    const m = /^([a-zA-Z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)$/.exec(line);
    if (!m) continue;
    const labels = Object.fromEntries([...(m[2] ?? '').matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)].map((x) => [x[1]!, x[2]!]));
    samples.push({ name: m[1]!, labels, value: Number(m[3]) });
  }
  return { text, samples };
}
/** Faz scrape até `cond` valer (gauges são lidos no scrape). */
const scrapeUntil = (cond: (s: Sample[]) => boolean, timeout = 10_000) =>
  eventually(
    async () => {
      const s = (await scrape()).samples;
      if (!cond(s)) throw new Error('condição de métrica não satisfeita');
      return s;
    },
    { timeout, interval: 200 },
  );

/** Soma das amostras com o nome e (subconjunto de) labels dados. */
const val = (s: Sample[], name: string, labels: Record<string, string> = {}) =>
  s
    .filter((x) => x.name === name && Object.entries(labels).every(([k, v]) => x.labels[k] === v))
    .reduce((n, x) => n + x.value, 0);

describe('reconciliação', () => {
  test('consistente após BET/WIN/LOSS/REFUND/ROLLBACK; checkedEntries conta só lançamentos', async () => {
    const w = await newWallet('211.47');
    const ops = [
      w.wager('BET', '31.17', 'b1'),
      w.wager('WIN', '43.21', 'w1', 'b1'),
      w.wager('BET', '7.77', 'b2'),
      w.wager('LOSS', '7.77', 'l2', 'b2'), // não move saldo: sem lançamento
      w.wager('BET', '12.53', 'b3'),
      w.wager('REFUND', '12.53', 'r3', 'b3'),
      w.wager('ROLLBACK', '43.21', 'rb1', 'w1'), // desfaz o WIN: débito
    ];
    for (const op of ops) {
      const r = await submit(op);
      expect(r.status, JSON.stringify(r.json)).toBe(200);
      expect(r.json.status).toBe('PROCESSED');
    }

    const r = await reconcile(w.walletId);
    expect(r.status).toBe(200);
    expect(r.json).toEqual({
      walletId: w.walletId,
      storedBalance: money('172.53'),
      calculatedBalance: money('172.53'),
      difference: money('0.00'),
      consistent: true,
      checkedEntries: 7, // abertura + 6 movimentos (LOSS não gera lançamento)
    });
    const { entries } = await assertWalletConsistent(orm, w.walletId);
    expect(entries).toBe(7);
  });

  test('divergência provocada por UPDATE direto: sinalizada, logada e contada; nada é corrigido', async () => {
    const ok = await newWallet('88.19');
    const bad = await newWallet('163.09');
    expect((await submit(bad.wager('BET', '19.99', 'b1'))).status).toBe(200);
    const before = val((await scrape()).samples, 'wager_reconciliation_divergences_total');
    const ledgerBefore = await q(`SELECT id, balance_after::text AS ba FROM wallet_ledger_entries WHERE wallet_id = ? ORDER BY seq`, [bad.walletId]);

    // sem trigger em wallets: adulteração direta do saldo armazenado
    await q(`UPDATE wallets SET balance = balance + 5.55 WHERE id = ?`, [bad.walletId]);
    try {
      const r = await reconcile(bad.walletId);
      expect(r.status).toBe(200); // resultado de auditoria, não erro da requisição
      expect(r.json).toEqual({
        walletId: bad.walletId,
        storedBalance: money('148.65'),
        calculatedBalance: money('143.10'),
        difference: money('5.55'),
        consistent: false,
        checkedEntries: 2,
      });

      const line = parsedLogs().find((l) => l.msg === 'wallet reconciliation divergence' && l.walletId === bad.walletId);
      expect(line).toMatchObject({ level: 'error', difference: '5.55', checkedEntries: 2 });
      expect(line!.correlationId).toBe(r.res.headers.get('x-correlation-id'));
      expect(val((await scrape()).samples, 'wager_reconciliation_divergences_total')).toBe(before + 1);

      // nada foi corrigido: saldo segue adulterado e o ledger intacto
      expect((await q(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [bad.walletId]))[0].b).toBe('148.65');
      expect(await q(`SELECT id, balance_after::text AS ba FROM wallet_ledger_entries WHERE wallet_id = ? ORDER BY seq`, [bad.walletId])).toEqual(
        ledgerBefore,
      );
      expect((await reconcile(bad.walletId)).json.consistent).toBe(false);

      // a corrupção de uma wallet não afeta a reconciliação de outra
      const other = await reconcile(ok.walletId);
      expect(other.json).toMatchObject({ consistent: true, difference: money('0.00'), checkedEntries: 1 });
    } finally {
      // limpeza do teste (não do sistema): não deixa wallet adulterada no banco compartilhado
      await q(`UPDATE wallets SET balance = balance - 5.55 WHERE id = ?`, [bad.walletId]);
    }
  });

  test('wallet inexistente (404) e id inválido (400)', async () => {
    expect((await reconcile(uid())).status).toBe(404);
    expect((await reconcile('nao-uuid')).status).toBe(400);
  });
});

describe('/metrics', () => {
  const REQUIRED = [
    'wager_transactions_total',
    'wager_transactions_rejected_total',
    'wager_duplicates_total',
    'wager_idempotency_conflicts_total',
    'wager_retries_total',
    'wager_lock_conflicts_total',
    'wager_dlq_messages_total',
    'wager_dlq_depth',
    'wager_outbox_pending',
    'wager_outbox_lag_seconds',
    'wager_outbox_published_total',
    'wager_outbox_publish_failures_total',
    'wager_processing_duration_seconds',
    'wager_pending_reference_total',
    'wager_reconciliation_divergences_total',
  ];

  test('expõe todas as métricas mínimas, sem autenticação', async () => {
    const { text } = await scrape();
    for (const name of REQUIRED) expect(text).toMatch(new RegExp(`^# TYPE ${name} `, 'm'));
    expect(text).toContain('process_cpu_user_seconds_total'); // métricas default do processo
  });

  test('HTTP: processada, rejeitada, replay e conflito de idempotência', async () => {
    const before = (await scrape()).samples;
    const w = await newWallet('91.13');
    const poor = await newWallet('3.07');
    const bet = w.wager('BET', '17.41', 'b1');
    expect((await submit(bet)).status).toBe(200);
    expect((await submit(poor.wager('BET', '31.17', 'b1'))).status).toBe(422);
    expect((await submit(bet)).json.idempotentReplay).toBe(true);
    expect((await submit({ ...bet, money: money('17.42') })).status).toBe(409);

    const after = (await scrape()).samples;
    const d = (name: string, labels: Record<string, string> = {}) => val(after, name, labels) - val(before, name, labels);
    expect(d('wager_transactions_total', { status: 'PROCESSED', kind: 'BET' })).toBe(1);
    expect(d('wager_transactions_total', { status: 'REJECTED', kind: 'BET' })).toBe(1);
    expect(d('wager_transactions_rejected_total', { failure_code: 'INSUFFICIENT_FUNDS' })).toBe(1);
    expect(d('wager_duplicates_total', { source: 'http' })).toBe(1);
    expect(d('wager_idempotency_conflicts_total')).toBe(1);
    expect(d('wager_processing_duration_seconds_count', { source: 'http' })).toBe(4);
    // nenhum label de alta cardinalidade
    for (const s of after) expect(Object.keys(s.labels)).not.toContain('walletId');
  });

  test('SQS: duplicata via consumidor real, DLQ (contador e profundidade) e correlationId do envelope', async () => {
    const before = (await scrape()).samples;
    const w = await newWallet('57.19');
    const data = { ...w.wager('BET', '8.27', 'b1'), idempotencyKey: `${w.walletId}:b1` };
    const msg = { messageId: uid(), type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), correlationId: `obs-sqs-${uid()}`, data };
    await sendMessage(sqs, qs.url, msg, w.walletId);
    await sendMessage(sqs, qs.url, msg, w.walletId); // mesmo messageId: duplicata do inbox
    await sendMessage(sqs, qs.url, '{"quebrado"', 'g-broken');

    const consumer = new WagerConsumer(app.get(SubmitWagerTransactionUseCase), sqs, {
      queueName: qs.name,
      dlqName: qs.dlqName,
      waitTimeSeconds: 1,
      metrics,
    });
    const out: Outcome[] = [];
    const deadline = Date.now() + 20_000;
    while (out.length < 3 && Date.now() < deadline) out.push(...(await consumer.runOnce()));
    expect(out.sort()).toEqual(['dead-lettered', 'duplicate', 'processed']);

    const after = await scrapeUntil((s) => val(s, 'wager_dlq_depth') >= 1); // ApproximateNumberOfMessages da DLQ
    const d = (name: string, labels: Record<string, string> = {}) => val(after, name, labels) - val(before, name, labels);
    expect(d('wager_duplicates_total', { source: 'sqs' })).toBe(1);
    expect(d('wager_transactions_total', { status: 'PROCESSED', kind: 'BET' })).toBe(1);
    expect(d('wager_dlq_messages_total')).toBe(1);
    expect(d('wager_processing_duration_seconds_count', { source: 'sqs' })).toBe(2);

    const mine = parsedLogs().filter((l) => l.correlationId === msg.correlationId);
    expect(mine.find((l) => l.msg === 'message processed')).toMatchObject({ messageId: msg.messageId, walletId: w.walletId, providerId: 'prov-obs' });
    expect(mine.find((l) => l.msg === 'wager transaction recorded')).toMatchObject({ messageId: msg.messageId, status: 'PROCESSED' });
    expect(mine.find((l) => l.msg.startsWith('duplicate message'))).toMatchObject({ messageId: msg.messageId });
  });

  test('retry do UnitOfWork por deadlock (40P01) alimenta retries e lock conflicts', async () => {
    const before = (await scrape()).samples;
    const uow = app.get<UnitOfWork>(UNIT_OF_WORK);
    let attempts = 0;
    const result = await uow.run(async () => {
      if (++attempts === 1) throw Object.assign(new Error('deadlock detected'), { code: '40P01' });
      return 'ok';
    });
    expect(result).toBe('ok');
    expect(attempts).toBe(2);
    const after = (await scrape()).samples;
    const d = (name: string, labels: Record<string, string> = {}) => val(after, name, labels) - val(before, name, labels);
    expect(d('wager_retries_total', { component: 'unit_of_work', reason: 'deadlock' })).toBe(1);
    expect(d('wager_lock_conflicts_total', { code: '40P01' })).toBe(1);
    expect(parsedLogs().some((l) => l.msg === 'transaction retry' && l.code === '40P01')).toBe(true);
  });

  test('outbox: lag > 0 com evento pendente e 0 depois de publicar (sem workers)', async () => {
    // backlog das outras suítes (sem consumidor) não interessa aqui
    await q(`UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL`);
    let s = (await scrape()).samples;
    expect(val(s, 'wager_outbox_pending')).toBe(0);
    expect(val(s, 'wager_outbox_lag_seconds')).toBe(0);
    const publishedBefore = val(s, 'wager_outbox_published_total');

    await newWallet('42.42'); // abertura: 2 eventos na outbox
    s = await scrapeUntil((x) => val(x, 'wager_outbox_lag_seconds') >= 1, 5_000);
    expect(val(s, 'wager_outbox_pending')).toBe(2);

    const publisher = new OutboxPublisher(orm, sqs, { queueName: events.name, metrics });
    expect(await publisher.runOnce()).toEqual({ published: 2, failed: 0 });
    s = (await scrape()).samples;
    expect(val(s, 'wager_outbox_pending')).toBe(0);
    expect(val(s, 'wager_outbox_lag_seconds')).toBe(0);
    expect(val(s, 'wager_outbox_published_total') - publishedBefore).toBe(2);
  });
});

describe('correlationId e logs', () => {
  test('X-Correlation-Id enviado volta na resposta e aparece nos logs da requisição e do caso de uso', async () => {
    const w = await newWallet('64.31');
    const correlationId = `obs-${uid()}`;
    const r = await submit(w.wager('BET', '9.43', 'b1'), { 'X-Correlation-Id': correlationId });
    expect(r.status).toBe(200);
    expect(r.res.headers.get('x-correlation-id')).toBe(correlationId);

    const mine = parsedLogs().filter((l) => l.correlationId === correlationId);
    expect(mine.find((l) => l.msg === 'http request')).toMatchObject({
      method: 'POST',
      route: '/wagering/transactions',
      status: 200,
      level: 'info',
    });
    expect(mine.find((l) => l.msg === 'http request')!.durationMs).toBeNumber();
    expect(mine.find((l) => l.msg === 'wager transaction recorded')).toMatchObject({
      transactionId: r.json.transactionId,
      walletId: w.walletId,
      providerId: 'prov-obs',
      status: 'PROCESSED',
    });

    // sem header: um UUIDv7 é gerado e devolvido
    const g = await call('GET', `/wallets/${w.walletId}`);
    const generated = g.res.headers.get('x-correlation-id')!;
    expect(generated).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(parsedLogs().find((l) => l.correlationId === generated && l.msg === 'http request')).toMatchObject({
      route: '/wallets/:walletId',
    });
  });

  test('nenhum log capturado leva "amount" nem valores monetários do payload/saldo', () => {
    expect(logs.length).toBeGreaterThan(20);
    const all = logs.join('\n');
    expect(all).not.toContain('"amount"');
    const values = [
      // payloads
      '211.47', '31.17', '43.21', '7.77', '12.53', '88.19', '163.09', '19.99', '91.13', '3.07', '17.41', '17.42',
      '57.19', '8.27', '42.42', '64.31', '9.43',
      // saldos resultantes
      '180.30', '223.51', '215.74', '203.21', '172.53', '143.10', '148.65', '73.72', '48.92', '54.88',
    ];
    for (const v of values) {
      expect(all).not.toContain(`"${v}"`);
      expect(all).not.toMatch(new RegExp(`:${v.replace('.', '\\.')}[,}]`));
    }
  });
});
