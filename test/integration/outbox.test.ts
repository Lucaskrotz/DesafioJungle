import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { ServiceUnavailableException } from '@nestjs/common';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import type { UnitOfWork } from '../../src/application/ports';
import { SubmitWagerTransactionUseCase, type SubmitWagerCommand } from '../../src/application/submit-wager-transaction';
import { createApp } from '../../src/bootstrap';
import { loadConfig } from '../../src/config';
import { HealthController } from '../../src/infrastructure/http/health.controller';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { createSqsClient, Sqs } from '../../src/infrastructure/sqs/sqs.client';
import { CLAIM_SQL, OutboxPublisher } from '../../src/infrastructure/workers/outbox-publisher';

// Requer Postgres migrado e LocalStack com as filas de infra/localstack-init.sh.
let orm: Orm;
let uow: MikroOrmUnitOfWork;
let open: OpenWalletUseCase;
let submit: SubmitWagerTransactionUseCase;
let sqs: Sqs;
let eventsUrl: string;

beforeAll(async () => {
  orm = await createOrm();
  uow = new MikroOrmUnitOfWork(orm);
  open = new OpenWalletUseCase(uow);
  submit = new SubmitWagerTransactionUseCase(uow);
  sqs = new Sqs();
  eventsUrl = await sqs.queueUrl(loadConfig().eventsQueue);
});
afterAll(async () => {
  sqs.client.destroy();
  await orm.close();
});

// A outbox é compartilhada com as outras suítes (que não publicam): o backlog delas não tem consumidor
// e é marcado como publicado para que cada teste enxergue só as próprias mensagens.
beforeEach(() => q(`UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL`));

const uid = () => crypto.randomUUID();
const q = (sql: string, p: unknown[] = []) => orm.em.getConnection().execute(sql, p) as Promise<any[]>;
const outboxOf = (aggregateIds: string[]) =>
  q(
    `SELECT id, event_type, attempts, next_attempt_at, published_at, payload FROM outbox_messages
      WHERE aggregate_id IN (?) ORDER BY seq`,
    [aggregateIds],
  );

async function newWallet(initial = '100.00') {
  const playerId = uid();
  const w = await open.execute({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
  const roundId = uid();
  const cmd = (kind: string, amount: string, ext: string, ref?: string): SubmitWagerCommand => ({
    idempotencyKey: `${w.id}:${ext}`,
    providerId: 'prov-a',
    externalTransactionId: `${w.id}:${ext}`,
    playerId,
    walletId: w.id,
    roundId,
    gameId: 'g1',
    kind,
    money: { amount, currency: 'BRL' },
    referenceExternalTransactionId: ref === undefined ? undefined : `${w.id}:${ref}`,
  });
  return { id: w.id, cmd };
}

type Received = { body: any; groupId?: string; dedupId?: string };
/** Lê da fila só as mensagens dos aggregates do teste (apagando-as); as alheias ficam invisíveis e intactas. */
async function collect(queueUrl: string, aggregateIds: string[], expected: number, timeoutMs = 15_000): Promise<Received[]> {
  const mine = new Set(aggregateIds);
  const got: Received[] = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < expected && Date.now() < deadline) {
    const r = await sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 30,
        MessageSystemAttributeNames: ['MessageGroupId', 'MessageDeduplicationId'],
      }),
    );
    for (const m of r.Messages ?? []) {
      const body = JSON.parse(m.Body!);
      if (!mine.has(body.aggregateId)) continue;
      got.push({ body, groupId: m.Attributes?.MessageGroupId, dedupId: m.Attributes?.MessageDeduplicationId });
      await sqs.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle! }));
    }
  }
  return got;
}

/** Sqs real com espião nos SendMessage (corpo de cada envio). */
function spiedSqs() {
  const s = new Sqs(createSqsClient());
  const sent: { body: any; dedupId: string }[] = [];
  s.client.middlewareStack.add(
    (next) => async (args) => {
      const input = args.input as { MessageBody?: string; MessageDeduplicationId?: string };
      if (input.MessageBody) sent.push({ body: JSON.parse(input.MessageBody), dedupId: input.MessageDeduplicationId! });
      return next(args);
    },
    { step: 'initialize' },
  );
  return { sqs: s, sent };
}

const isMoneyProps = (m: any) => typeof m?.amount === 'string' && /^\d+\.\d{2}$/.test(m.amount) && m.currency === 'BRL';

describe('outbox: gravação atômica', () => {
  test('BET grava eventos na mesma transação; falha depois do insert da outbox não deixa nada', async () => {
    const w = await newWallet();
    expect((await outboxOf([w.id])).map((r) => r.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);

    await submit.execute(w.cmd('BET', '30.00', 'b1'));
    await submit.execute(w.cmd('LOSS', '30.00', 'l1', 'b1'));
    // LOSS não muda saldo: só Processed, sem WalletBalanceChanged
    expect((await outboxOf([w.id])).map((r) => r.event_type)).toEqual([
      'WagerTransactionProcessed', 'WalletBalanceChanged', // abertura
      'WagerTransactionProcessed', 'WalletBalanceChanged', // BET
      'WagerTransactionProcessed', // LOSS
    ]);

    // mesmo caso de uso, com uma falha forçada depois de tudo gravado (inclusive a outbox) e antes do commit
    const failing: UnitOfWork = {
      run: (fn) =>
        uow.run(async (r) => {
          await fn(r);
          expect((await outboxOf([w.id])).length).toBe(5); // fora da transação: ainda não visível
          throw new Error('falha forçada');
        }),
    };
    const sneaky = w.cmd('BET', '10.00', 'b2');
    await expect(new SubmitWagerTransactionUseCase(failing).execute(sneaky)).rejects.toThrow('falha forçada');
    expect(await outboxOf([w.id])).toHaveLength(5);
    expect(await q(`SELECT 1 FROM wager_transactions WHERE external_transaction_id = ?`, [sneaky.externalTransactionId])).toHaveLength(0);
    expect((await q(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [w.id]))[0].b).toBe('70.00');
  });
});

describe('outbox publisher', () => {
  test('publica o envelope no wager-events.fifo, em ordem por wallet, e marca published_at', async () => {
    const w = await newWallet();
    await submit.execute(w.cmd('BET', '30.00', 'b1'));
    await submit.execute(w.cmd('LOSS', '30.00', 'l1', 'b1'));
    const publisher = new OutboxPublisher(orm, sqs);
    expect(await publisher.oldestPendingAgeSeconds()).toBeGreaterThanOrEqual(0);

    expect(await publisher.runOnce()).toEqual({ published: 5, failed: 0 });
    const rows = await outboxOf([w.id]);
    expect(rows.every((r) => r.published_at !== null)).toBe(true);
    expect(await publisher.oldestPendingAgeSeconds()).toBe(0);
    expect(await publisher.runOnce()).toEqual({ published: 0, failed: 0 });

    const got = await collect(eventsUrl, [w.id], 5);
    // FIFO por MessageGroupId = walletId: mesma ordem da outbox (seq)
    expect(got.map((m) => m.body.eventId)).toEqual(rows.map((r) => r.id));
    for (const [i, m] of got.entries()) {
      const b = m.body;
      expect(b).toEqual(rows[i]!.payload);
      expect(m.groupId).toBe(w.id);
      expect(m.dedupId).toBe(b.eventId);
      expect(b.aggregateId).toBe(w.id);
      expect(typeof b.correlationId).toBe('string');
      expect(new Date(b.occurredAt).toISOString()).toBe(b.occurredAt);
      expect(b.version).toBe(1);
      expect(isMoneyProps(b.data.money)).toBe(true);
      if (b.eventType === 'WalletBalanceChanged') {
        expect(isMoneyProps(b.data.balanceBefore) && isMoneyProps(b.data.balanceAfter)).toBe(true);
      }
    }
    expect(got.filter((m) => m.body.eventType === 'WalletBalanceChanged').map((m) => m.body.data.balanceAfter.amount)).toEqual([
      '100.00',
      '70.00',
    ]);
  });

  test('falha de publicação: attempts++, backoff no futuro, ciclo não quebra; depois publica', async () => {
    const w = await newWallet();
    const broken = new OutboxPublisher(orm, sqs, { queueName: `nao-existe-${uid()}.fifo` });
    const before = Date.now();
    expect(await broken.runOnce()).toEqual({ published: 0, failed: 2 });

    let rows = await outboxOf([w.id]);
    for (const r of rows) {
      expect(r.published_at).toBeNull();
      expect(r.attempts).toBe(1);
      expect(new Date(r.next_attempt_at).getTime()).toBeGreaterThanOrEqual(before + 2_000); // outboxBackoffMs(1)
    }
    // ainda não venceu: nem o publisher quebrado nem o correto pegam a linha
    expect(await broken.runOnce()).toEqual({ published: 0, failed: 0 });
    const publisher = new OutboxPublisher(orm, sqs);
    expect(await publisher.runOnce()).toEqual({ published: 0, failed: 0 });

    // segunda falha dobra o backoff
    const later = new Date(Date.now() + 5_000);
    expect(await broken.runOnce(later)).toEqual({ published: 0, failed: 2 });
    rows = await outboxOf([w.id]);
    expect(rows.every((r) => r.attempts === 2 && new Date(r.next_attempt_at).getTime() === later.getTime() + 4_000)).toBe(true);

    expect(await publisher.runOnce(new Date(later.getTime() + 5_000))).toEqual({ published: 2, failed: 0 });
    expect((await outboxOf([w.id])).every((r) => r.published_at !== null)).toBe(true);
    expect((await collect(eventsUrl, [w.id], 2)).map((m) => m.body.eventId)).toEqual(rows.map((r) => r.id));
  });

  test('crash depois do send e antes do commit: segue pendente e é republicada com o mesmo eventId', async () => {
    const w = await newWallet();
    class CrashAfterSend extends OutboxPublisher {
      protected override async markPublished(): Promise<void> {
        throw new Error('processo morreu');
      }
    }
    const spy = spiedSqs();
    await expect(new CrashAfterSend(orm, spy.sqs).runOnce()).rejects.toThrow('processo morreu');
    let rows = await outboxOf([w.id]);
    expect(rows.every((r) => r.published_at === null && r.attempts === 0)).toBe(true); // rollback total

    expect(await new OutboxPublisher(orm, spy.sqs).runOnce()).toEqual({ published: 2, failed: 0 });
    rows = await outboxOf([w.id]);
    expect(rows.every((r) => r.published_at !== null)).toBe(true);

    // 1ª mensagem enviada duas vezes (a 2ª nem chegou a sair antes do crash), com corpo idêntico
    const ids = rows.map((r) => r.id);
    expect(spy.sent.map((s) => s.dedupId)).toEqual([ids[0], ids[0], ids[1]]);
    expect(spy.sent[1]!.body).toEqual(spy.sent[0]!.body);

    // na fila: o FIFO deduplica pelo MessageDeduplicationId; se algo passar, é idêntico por eventId
    const got = await collect(eventsUrl, [w.id], 3, 4_000);
    expect([...new Set(got.map((m) => m.body.eventId))]).toEqual(ids);
    for (const m of got) expect(m.body).toEqual(rows.find((r) => r.id === m.body.eventId)!.payload);
  });

  test('publishers concorrentes: cada mensagem publicada exatamente uma vez, nenhuma perdida', async () => {
    // fila própria: centenas de mensagens sem disputar a fila compartilhada
    const name = `wager-events-test-${uid()}.fifo`;
    const { QueueUrl } = await sqs.client.send(
      new CreateQueueCommand({ QueueName: name, Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' } }),
    );
    try {
      const wallets = await Promise.all([1, 2, 3].map(() => newWallet('1000.00')));
      await Promise.all(
        wallets.map(async (w) => {
          for (let i = 0; i < 50; i++) await submit.execute(w.cmd('BET', '1.00', `b${i}`));
        }),
      );
      const aggIds = wallets.map((w) => w.id);
      const total = (await outboxOf(aggIds)).length;
      expect(total).toBe(3 * 2 + 150 * 2);

      const spy = spiedSqs();
      const pending = async () => Number((await q(`SELECT count(*) AS n FROM outbox_messages WHERE published_at IS NULL`))[0].n);
      const drain = async (p: OutboxPublisher) => {
        let published = 0;
        for (;;) {
          const r = await p.runOnce();
          published += r.published;
          expect(r.failed).toBe(0);
          if (r.published === 0 && (await pending()) === 0) return published;
          if (r.published === 0) await Bun.sleep(5); // linhas travadas por outro publisher
        }
      };
      const perPublisher = await Promise.all(
        Array.from({ length: 4 }, () => drain(new OutboxPublisher(orm, spy.sqs, { queueName: name, batchSize: 10 }))),
      );

      expect(perPublisher.reduce((a, b) => a + b, 0)).toBe(total);
      expect(perPublisher.filter((n) => n > 0).length).toBeGreaterThan(1); // houve disputa real
      const rows = await outboxOf(aggIds);
      expect(rows.every((r) => r.published_at !== null)).toBe(true);
      // nenhum envio duplicado (SKIP LOCKED) e todos os eventIds enviados
      expect(spy.sent).toHaveLength(total);
      expect(new Set(spy.sent.map((s) => s.dedupId))).toEqual(new Set(rows.map((r) => r.id)));

      const got = await collect(QueueUrl!, aggIds, total, 60_000);
      expect(got).toHaveLength(total);
      expect(new Set(got.map((m) => m.body.eventId)).size).toBe(total);
    } finally {
      await sqs.client.send(new DeleteQueueCommand({ QueueUrl }));
    }
  });
});

describe('wiring', () => {
  test('createApp({ workers: true }) publica em segundo plano e para limpo no close', async () => {
    const app = await createApp({ workers: true });
    await app.init();
    try {
      const w = await newWallet();
      const deadline = Date.now() + 10_000;
      while ((await outboxOf([w.id])).some((r) => r.published_at === null) && Date.now() < deadline) await Bun.sleep(100);
      expect((await outboxOf([w.id])).every((r) => r.published_at !== null)).toBe(true);
      expect(await collect(eventsUrl, [w.id], 2)).toHaveLength(2);
    } finally {
      await app.close();
    }
  });
});

describe('readiness com SQS', () => {
  test('SQS inalcançável => 503 com checks.sqs = down', async () => {
    const down = new Sqs(createSqsClient({ ...loadConfig(), sqsEndpoint: 'http://127.0.0.1:1' }));
    const health = new HealthController(orm, down);
    const err = await health.ready().catch((e) => e);
    expect(err).toBeInstanceOf(ServiceUnavailableException);
    expect(err.getResponse()).toEqual({ status: 'unavailable', checks: { postgres: 'up', sqs: 'down' } });
    expect(await new HealthController(orm, sqs).ready()).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });
    down.client.destroy();
  });
});

// Regressão achada no teste de carga: o claim varria outbox_messages_seq_key desde o início, pulando
// todas as linhas publicadas (~200 ms por ciclo com 300 mil linhas). Sem seq scan (tabela pequena em
// banco novo), o planner deve usar o índice parcial em seq, e não o índice único de seq inteiro.
test('claim da outbox usa o índice parcial dos pendentes', async () => {
  const plan = await orm.em.fork().transactional(async (em) => {
    await em.execute(`SET LOCAL enable_seqscan = off`);
    return (await em.execute(`EXPLAIN ${CLAIM_SQL}`, [null, 25])) as { 'QUERY PLAN': string }[];
  });
  const text = plan.map((r) => r['QUERY PLAN']).join('\n');
  expect(text).toContain('outbox_pending_seq_idx');
  expect(text).not.toContain('outbox_messages_seq_key');
});
