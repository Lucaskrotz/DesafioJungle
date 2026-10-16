import { afterAll, beforeAll, expect, test } from 'bun:test';
import { DeleteMessageBatchCommand, ReceiveMessageCommand, SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import type { SubmitWagerCommand } from '../../src/application/submit-wager-transaction';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { Sqs } from '../../src/infrastructure/sqs/sqs.client';
import { CONSUMER_NAME } from '../../src/infrastructure/sqs/wager-consumer';
import { eventually, withCluster, within, type Cluster, type Instance } from '../support/cluster';
import { assertWalletConsistent } from '../support/invariants';
import { deleteQueues, queueCounts, sendMessage, tempFifo, tempQueues, type Queues } from '../support/sqs';

// Processos reais (`bun src/main.ts`) contra Postgres migrado e LocalStack. Cada teste cria filas FIFO
// próprias (principal com redrive para DLQ + eventos) e mata todas as instâncias no fim, inclusive em falha.
let orm: Orm;
let open: OpenWalletUseCase;
let sqs: Sqs;

beforeAll(async () => {
  orm = await createOrm();
  open = new OpenWalletUseCase(new MikroOrmUnitOfWork(orm));
  sqs = new Sqs();
});
afterAll(async () => {
  sqs.client.destroy();
  await orm.close();
});

const uid = () => crypto.randomUUID();
const money = (amount: string) => ({ amount, currency: 'BRL' });
const q = (sql: string, p: unknown[] = []) => orm.em.getConnection().execute(sql, p) as Promise<any[]>;
const balanceOf = async (walletId: string) => (await q(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [walletId]))[0].b as string;
const debitsOf = async (walletId: string) =>
  Number((await q(`SELECT count(*) AS n FROM wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [walletId]))[0].n);
const txByExt = async (ext: string) => (await q(`SELECT * FROM wager_transactions WHERE external_transaction_id = ?`, [ext]))[0];
const pendingOutbox = async (walletIds: string[]) =>
  Number((await q(`SELECT count(*) AS n FROM outbox_messages WHERE aggregate_id IN (?) AND published_at IS NULL`, [walletIds]))[0].n);

type Wallet = Awaited<ReturnType<typeof newWallet>>;
async function newWallet(initial: string) {
  const playerId = uid();
  const { id } = await open.execute({ playerId, initialBalance: money(initial) });
  const roundId = uid();
  const cmd = (kind: string, amount: string, ext: string, ref?: string): SubmitWagerCommand => ({
    idempotencyKey: `${id}:${ext}`,
    providerId: 'prov-p6',
    externalTransactionId: `${id}:${ext}`,
    playerId,
    walletId: id,
    roundId,
    gameId: 'g1',
    kind,
    money: money(amount),
    ...(ref ? { referenceExternalTransactionId: `${id}:${ref}` } : {}),
  });
  return { id, cmd };
}

type Msg = { messageId: string; type: 'WagerTransactionRequested'; occurredAt: string; data: SubmitWagerCommand };
const toMessage = (data: SubmitWagerCommand, messageId = uid()): Msg => ({
  messageId,
  type: 'WagerTransactionRequested',
  occurredAt: new Date().toISOString(),
  data,
});

/** Envia em lotes de 10; dedup id sempre novo (reenvio de um mesmo messageId chega de fato duplicado). */
async function sendBatch(url: string, items: { body: Msg; group: string }[]) {
  for (let i = 0; i < items.length; i += 10) {
    const Entries = items.slice(i, i + 10).map((it, j) => ({
      Id: String(j),
      MessageBody: JSON.stringify(it.body),
      MessageGroupId: it.group,
      MessageDeduplicationId: uid(),
    }));
    const r = await sqs.client.send(new SendMessageBatchCommand({ QueueUrl: url, Entries }));
    expect(r.Failed ?? []).toHaveLength(0);
  }
}

type Res = { status: number; json: any };
async function post(inst: Instance, c: SubmitWagerCommand): Promise<Res> {
  const { idempotencyKey, ...body } = c;
  const res = await fetch(`${inst.base}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  return { status: res.status, json: await res.json() };
}

/** Cliente que reenvia (mesma key) em erro de rede/5xx, para qualquer instância viva: o retry do provedor. */
const postRetrying = (c: Cluster, cmd: SubmitWagerCommand) =>
  eventually(
    async () => {
      const alive = c.alive;
      if (!alive.length) throw new Error('nenhuma instância viva');
      const r = await post(alive[Math.floor(Math.random() * alive.length)]!, cmd);
      if (r.status >= 500) throw new Error(`HTTP ${r.status}`);
      return r;
    },
    { timeout: 60_000, interval: 50 },
  );

const expectNo5xx = (rs: Res[]) => rs.forEach((r) => expect(r.status, JSON.stringify(r.json)).toBeLessThan(500));
const sumLogged = (c: Cluster, msg: string) => c.all.reduce((n, i) => n + i.logged(msg), 0);
const drained = async (url: string) => {
  expect(await queueCounts(sqs, url)).toEqual({ visible: 0, inFlight: 0 });
  return true;
};

type Ctx = { c: Cluster; qs: Queues; events: { name: string; url: string } };
/** Filas próprias + cluster; ao fim mata as instâncias e apaga as filas. */
async function scenario(opts: { visibility?: number; env?: Record<string, string> }, fn: (ctx: Ctx) => Promise<void>) {
  const qs = await tempQueues(sqs, { visibility: opts.visibility ?? 3, maxReceiveCount: 5 });
  const events = await tempFifo(sqs, 'we');
  const env = {
    RUN_WORKERS: '1',
    SQS_TRANSACTIONS_QUEUE: qs.name,
    SQS_TRANSACTIONS_DLQ: qs.dlqName,
    SQS_EVENTS_QUEUE: events.name,
    OUTBOX_POLL_MS: '200',
    PENDING_REFERENCE_POLL_MS: '300',
    DB_POOL_MAX: '10',
    ...opts.env,
  };
  try {
    await withCluster(env, (c) => fn({ c, qs, events }));
  } finally {
    await deleteQueues(sqs, [...qs.urls, events.url]);
  }
}

test('1. quatro instâncias HTTP: 80+80, mesma key x50, hot wallet e 20 wallets em paralelo', () =>
  scenario({ env: { RUN_WORKERS: '0' } }, async ({ c }) => {
    const insts = await c.spawnMany(4);
    let rr = 0;
    const next = () => insts[rr++ % insts.length]!;

    // (a) seção 8, em 5 rodadas simultâneas; cada aposta em uma instância diferente
    await Promise.all(
      Array.from({ length: 5 }, async () => {
        const w = await newWallet('100.00');
        const cs = [w.cmd('BET', '80.00', uid()), w.cmd('BET', '80.00', uid())];
        const rs = await Promise.all(cs.map((x) => post(next(), x)));
        expectNo5xx(rs);
        expect(rs.filter((r) => r.json.status === 'PROCESSED')).toHaveLength(1);
        const rej = rs.filter((r) => r.json.status === 'REJECTED');
        expect(rej).toHaveLength(1);
        expect(rej[0]!.json.failureCode).toBe('INSUFFICIENT_FUNDS');
        // retry das duas em outras instâncias: replay idêntico
        const again = await Promise.all(cs.map((x) => post(next(), x)));
        again.forEach((r, i) => {
          expect(r.status).toBe(rs[i]!.status);
          expect(r.json).toEqual({ ...rs[i]!.json, idempotentReplay: true });
        });
        const inv = await assertWalletConsistent(orm, w.id);
        expect(inv.balance.toFixed(2)).toBe('20.00');
        expect(inv.debits).toBe(1);
      }),
    );

    // (b) mesma aposta 50x espalhada pelas instâncias
    {
      const w = await newWallet('100.00');
      const bet = w.cmd('BET', '30.00', 'same');
      const rs = await Promise.all(Array.from({ length: 50 }, () => post(next(), bet)));
      expectNo5xx(rs);
      expect(rs.every((r) => r.status === 200 && r.json.status === 'PROCESSED')).toBe(true);
      expect(new Set(rs.map((r) => r.json.transactionId)).size).toBe(1);
      expect(rs.filter((r) => !r.json.idempotentReplay)).toHaveLength(1);
      const inv = await assertWalletConsistent(orm, w.id);
      expect(inv.balance.toFixed(2)).toBe('70.00');
      expect(inv.debits).toBe(1);
    }

    // (c) hot wallet: 200 BETs de 1.00 sobre 120.00
    {
      const w = await newWallet('120.00');
      const rs = await Promise.all(Array.from({ length: 200 }, () => post(next(), w.cmd('BET', '1.00', uid()))));
      expectNo5xx(rs);
      expect(rs.filter((r) => r.json.status === 'PROCESSED')).toHaveLength(120);
      const rej = rs.filter((r) => r.json.status === 'REJECTED');
      expect(rej).toHaveLength(80);
      expect(rej.every((r) => r.status === 422 && r.json.failureCode === 'INSUFFICIENT_FUNDS')).toBe(true);
      const inv = await assertWalletConsistent(orm, w.id);
      expect(inv.balance.toFixed(2)).toBe('0.00');
      expect(inv.debits).toBe(120);
    }

    // (d) 20 wallets distintas x 3 BETs, tudo junto
    {
      const ws = await Promise.all(Array.from({ length: 20 }, () => newWallet('100.00')));
      const rs = await Promise.all(ws.flatMap((w) => [1, 2, 3].map(() => post(next(), w.cmd('BET', '10.00', uid())))));
      expectNo5xx(rs);
      expect(rs.every((r) => r.json.status === 'PROCESSED')).toBe(true);
      for (const w of ws) {
        const inv = await assertWalletConsistent(orm, w.id);
        expect(inv.balance.toFixed(2)).toBe('70.00');
        expect(inv.debits).toBe(3);
      }
    }
    expect(insts.every((i) => i.alive)).toBe(true);
  }));

test('2. quatro instâncias consumindo SQS: duplicatas e mesma transação com messageIds novos => efeitos únicos', () =>
  scenario({ visibility: 3 }, async ({ c, qs }) => {
    await c.spawnMany(4);
    const ws = await Promise.all(Array.from({ length: 5 }, () => newWallet('1000.00')));
    const distinct = ws.flatMap((w) => Array.from({ length: 24 }, () => toMessage(w.cmd('BET', '1.00', uid())))); // 120
    const sameMessage = distinct.slice(0, 15); // mesmo messageId reenviado
    const sameTx = distinct.slice(15, 30).map((m) => ({ ...m, messageId: uid() })); // mesma transação, messageId novo
    const all = [...distinct, ...sameMessage, ...sameTx].sort(() => Math.random() - 0.5);
    expect(all).toHaveLength(150);
    // metade agrupada por wallet, metade em grupos variados (mesma wallet em paralelo de verdade)
    await sendBatch(qs.url, all.map((m, i) => ({ body: m, group: i % 2 ? m.data.walletId : `g-${i % 7}` })));

    const messageIds = [...new Set(all.map((m) => m.messageId))];
    expect(messageIds).toHaveLength(135);
    await eventually(async () => sumLogged(c, 'message processed') === 135 && (await drained(qs.url)), { timeout: 60_000, interval: 250 });

    // cada messageId novo processado exatamente uma vez; o mesmo messageId de novo vira duplicata
    expect(sumLogged(c, 'message processed')).toBe(135);
    expect(sumLogged(c, 'duplicate message')).toBeGreaterThanOrEqual(15);
    expect(c.all.filter((i) => i.logged('message processed') > 0).length).toBeGreaterThan(1);

    const inbox = await q(`SELECT message_id, processed_at FROM inbox_messages WHERE consumer_name = ? AND message_id IN (?)`, [
      CONSUMER_NAME,
      messageIds,
    ]);
    expect(inbox).toHaveLength(135);
    expect(inbox.every((r) => r.processed_at !== null)).toBe(true);
    for (const w of ws) {
      const inv = await assertWalletConsistent(orm, w.id);
      expect(inv.debits).toBe(24);
      expect(inv.balance.toFixed(2)).toBe('976.00');
      expect(Number((await q(`SELECT count(*) AS n FROM wager_transactions WHERE wallet_id = ? AND kind = 'BET'`, [w.id]))[0].n)).toBe(24);
    }
    expect(await queueCounts(sqs, qs.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
    console.info(
      `[cenário 2] processadas por instância=${c.all.map((i) => i.logged('message processed')).join('/')} duplicatas=${sumLogged(c, 'duplicate message')}`,
    );
  }));

test('3. instância morre depois do commit e antes do ack: outra recebe a redelivery como duplicata', () =>
  scenario({ visibility: 3 }, async ({ c, qs }) => {
    const a = await c.spawn({ FAULT_EXIT_BEFORE_ACK: '1' });
    const w = await newWallet('100.00');
    const m = toMessage(w.cmd('BET', '30.00', 'b1'));
    await sendMessage(sqs, qs.url, m, w.id);

    expect(await within(a.exited, 20_000, 'A morrer')).toBe(137);
    expect(a.logged('message processed')).toBe(1);
    // commit feito, mensagem não deletada
    expect(await balanceOf(w.id)).toBe('70.00');
    expect(await debitsOf(w.id)).toBe(1);
    const counts = await queueCounts(sqs, qs.url);
    expect(counts.visible + counts.inFlight).toBe(1);

    // B só sobe depois de A morta: a redelivery (após a visibilidade) é dela
    const b = await c.spawn();
    await eventually(async () => b.logged('duplicate message') === 1 && (await drained(qs.url)), { timeout: 20_000, interval: 200 });
    expect(b.logged('message processed')).toBe(0);
    expect(await debitsOf(w.id)).toBe(1);
    const inbox = await q(`SELECT processed_at FROM inbox_messages WHERE consumer_name = ? AND message_id = ?`, [CONSUMER_NAME, m.messageId]);
    expect(inbox).toHaveLength(1);
    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.balance.toFixed(2)).toBe('70.00');
    expect(await queueCounts(sqs, qs.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
  }));

/** Lê (e apaga) a fila de eventos até ver todos os eventIds esperados; devolve só os envelopes deles. */
async function collectEvents(url: string, wanted: Set<string>, timeoutMs = 60_000) {
  const got: any[] = [];
  const seen = new Set<string>();
  const deadline = Date.now() + timeoutMs;
  while (seen.size < wanted.size && Date.now() < deadline) {
    const { Messages = [] } = await sqs.client.send(
      new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30 }),
    );
    for (const m of Messages) {
      const body = JSON.parse(m.Body!);
      if (!wanted.has(body.eventId)) continue;
      got.push(body);
      seen.add(body.eventId);
    }
    if (Messages.length) {
      await sqs.client.send(
        new DeleteMessageBatchCommand({ QueueUrl: url, Entries: Messages.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })) }),
      );
    }
  }
  return got;
}

test('4. publishers em processos distintos sobre a mesma outbox; um morre com SIGKILL no meio', () =>
  scenario({}, async ({ c, events }) => {
    // eventos gerados pela API com nenhum publisher rodando
    const api = await c.spawnMany(2, { RUN_WORKERS: '0' });
    const ws = await Promise.all(Array.from({ length: 4 }, () => newWallet('1000.00')));
    const ids = ws.map((w) => w.id);
    let rr = 0;
    const burst = async (perWallet: number) => {
      const cmds = ws.flatMap((w) => Array.from({ length: perWallet }, () => w.cmd('BET', '1.00', uid())));
      for (let i = 0; i < cmds.length; i += 40) {
        const rs = await Promise.all(cmds.slice(i, i + 40).map((x) => post(api[rr++ % api.length]!, x)));
        expectNo5xx(rs);
      }
    };
    await burst(60);
    const total1 = 4 * 2 + 4 * 60 * 2;
    expect(await pendingOutbox(ids)).toBe(total1);
    // backlog alheio (outras suítes não publicam) não pode ficar à frente na ordem da outbox
    await q(`UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL AND aggregate_id NOT IN (?)`, [ids]);

    // publisher lento morto no meio do trabalho
    const victim = await c.spawn({ OUTBOX_POLL_MS: '400' });
    await eventually(async () => (await pendingOutbox(ids)) <= total1 - 25, { timeout: 20_000, interval: 20 });
    victim.kill('SIGKILL');
    await victim.exited;
    const leftAfterKill = await pendingOutbox(ids);
    expect(leftAfterKill).toBeGreaterThan(0); // morreu de fato no meio

    // dois publishers assumem enquanto a API gera mais eventos
    await c.spawnMany(2);
    await burst(30);
    const total = total1 + 4 * 30 * 2;
    await eventually(async () => (await pendingOutbox(ids)) === 0, { timeout: 60_000, interval: 200 });

    const rows = await q(`SELECT id, payload FROM outbox_messages WHERE aggregate_id IN (?)`, [ids]);
    expect(rows).toHaveLength(total);
    const byId = new Map(rows.map((r) => [r.id as string, r.payload]));
    const got = await collectEvents(events.url, new Set(byId.keys()));
    // todos chegaram; duplicata (republicação após o crash) só com o mesmo eventId e corpo idêntico
    expect(new Set(got.map((e) => e.eventId)).size).toBe(total);
    for (const e of got) expect(e).toEqual(byId.get(e.eventId));
    console.info(`[cenário 4] eventos=${total} pendentes-após-kill=${leftAfterKill} recebidos=${got.length} duplicados=${got.length - total}`);
    for (const id of ids) await assertWalletConsistent(orm, id);
  }));

test('5. referências fora de ordem entre processos: workers de 3 instâncias resolvem sem duplicar', () =>
  scenario({}, async ({ c, qs }) => {
    const [i1, i2, i3] = await c.spawnMany(3);
    const w = await newWallet('100.00');
    const early = [w.cmd('REFUND', '10.00', 'rf', 'b1'), w.cmd('ROLLBACK', '10.00', 'rb', 'b2'), w.cmd('WIN', '25.00', 'wn', 'b3')];
    for (const e of early) {
      const r = await post(i1!, e);
      expect(r.status).toBe(202);
      expect(r.json.status).toBe('PENDING_REFERENCE');
    }
    const viaSqs = w.cmd('REFUND', '10.00', 'rf4', 'b4');
    await sendMessage(sqs, qs.url, toMessage(viaSqs), w.id);
    await eventually(async () => (await txByExt(viaSqs.externalTransactionId))?.status === 'PENDING_REFERENCE', { timeout: 15_000 });
    const orphan = w.cmd('REFUND', '10.00', 'orphan', 'nunca');
    expect((await post(i3!, orphan)).status).toBe(202);

    // as BETs chegam por outras instâncias (e pela fila)
    expect((await post(i2!, w.cmd('BET', '10.00', 'b1'))).json.status).toBe('PROCESSED');
    expect((await post(i3!, w.cmd('BET', '10.00', 'b2'))).json.status).toBe('PROCESSED');
    expect((await post(i2!, w.cmd('BET', '10.00', 'b3'))).json.status).toBe('PROCESSED');
    await sendMessage(sqs, qs.url, toMessage(w.cmd('BET', '10.00', 'b4')), w.id);

    const resolved = [...early, viaSqs];
    await eventually(
      async () => {
        for (const x of resolved) expect((await txByExt(x.externalTransactionId)).status).toBe('PROCESSED');
        return true;
      },
      { timeout: 30_000, interval: 200 },
    );
    for (const x of resolved) {
      const tx = await txByExt(x.externalTransactionId);
      expect(tx.reference_transaction_id).toBe((await txByExt(x.referenceExternalTransactionId!)).id);
      expect(await q(`SELECT 1 FROM wallet_ledger_entries WHERE transaction_id = ?`, [tx.id])).toHaveLength(1);
      const evs = await q(`SELECT event_type FROM outbox_messages WHERE payload->'data'->>'transactionId' = ? ORDER BY seq`, [tx.id]);
      expect(evs.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference', 'WagerTransactionProcessed', 'WalletBalanceChanged']);
    }
    // 100 - 40 (BETs) + 10 + 10 + 25 + 10
    expect(await balanceOf(w.id)).toBe('115.00');
    expect((await assertWalletConsistent(orm, w.id)).entries).toBe(1 + 4 + 4);

    // referência que nunca chega: segue pendente, com tentativas crescendo e próximo vencimento no futuro
    const o = await eventually(
      async () => {
        const tx = await txByExt(orphan.externalTransactionId);
        expect(tx.reference_attempts).toBeGreaterThanOrEqual(1);
        return tx;
      },
      { timeout: 20_000, interval: 200 },
    );
    expect(o.status).toBe('PENDING_REFERENCE');
    expect(new Date(o.next_reference_attempt_at).getTime()).toBeGreaterThan(Date.now());
    expect(await balanceOf(w.id)).toBe('115.00');
    await eventually(() => drained(qs.url), { timeout: 15_000 });
    console.info(
      `[cenário 5] resolvidas=${resolved.length} saldo=115.00 órfã: attempts=${o.reference_attempts} próxima=${new Date(o.next_reference_attempt_at).toISOString()}`,
    );
  }));

test(
  '6. reinício com carga contínua (HTTP + SQS): SIGKILL de uma, de todas, retry total => consistência final',
  () =>
    scenario({ visibility: 3 }, async ({ c, qs }) => {
      await c.spawnMany(3);
      const ws = await Promise.all(Array.from({ length: 6 }, () => newWallet('10000.00')));
      const http: { cmd: SubmitWagerCommand; res?: Res }[] = [];
      const msgs: Msg[] = [];
      let stop = false;
      let n = 0;
      const pick = () => ws[n++ % ws.length]!;
      const load = (async () => {
        while (!stop) {
          const batch = Array.from({ length: 8 }, () => ({ cmd: pick().cmd('BET', '1.00', uid()) }) as { cmd: SubmitWagerCommand; res?: Res });
          const sqsBatch = Array.from({ length: 4 }, () => toMessage(pick().cmd('BET', '1.00', uid())));
          http.push(...batch);
          msgs.push(...sqsBatch);
          await Promise.all([
            ...batch.map(async (op) => (op.res = await postRetrying(c, op.cmd))),
            sendBatch(qs.url, sqsBatch.map((m) => ({ body: m, group: m.data.walletId }))),
          ]);
        }
      })();
      const progress = (k: number) => eventually(() => http.filter((o) => o.res).length >= k, { timeout: 60_000, interval: 50 });
      try {
        await progress(40);
        const victim = c.alive[0]!;
        victim.kill('SIGKILL');
        await victim.exited;
        await progress(80);
        await c.spawn(); // reinício da que caiu
        await progress(120);
        const everyone = c.alive;
        for (const i of everyone) i.kill('SIGKILL'); // queda total
        await Promise.all(everyone.map((i) => i.exited));
        await c.spawnMany(3);
        await progress(200);
      } finally {
        stop = true;
        await load;
      }
      expect(http.every((o) => o.res!.status === 200 && o.res!.json.status === 'PROCESSED')).toBe(true);

      const allMessageIds = msgs.map((m) => m.messageId);
      const inboxDone = async () =>
        Number(
          (
            await q(`SELECT count(*) AS n FROM inbox_messages WHERE consumer_name = ? AND message_id IN (?) AND processed_at IS NOT NULL`, [
              CONSUMER_NAME,
              allMessageIds,
            ])
          )[0].n,
        );
      await eventually(async () => (await inboxDone()) === msgs.length && (await drained(qs.url)), { timeout: 60_000, interval: 250 });

      const expected = new Map(ws.map((w) => [w.id, 0]));
      for (const id of [...http.map((o) => o.cmd.walletId), ...msgs.map((m) => m.data.walletId)]) expected.set(id, expected.get(id)! + 1);
      const check = async () => {
        for (const w of ws) {
          const inv = await assertWalletConsistent(orm, w.id);
          expect(inv.debits).toBe(expected.get(w.id)!);
          expect(inv.balance.toFixed(2)).toBe((10_000 - expected.get(w.id)!).toFixed(2));
        }
      };
      await check();

      // retry total: tudo de novo, mesmas keys e mesmos messageIds
      const dupBefore = sumLogged(c, 'duplicate message');
      const replays = await Promise.all(http.map((o) => postRetrying(c, o.cmd)));
      replays.forEach((r, i) => {
        expect(r.status).toBe(200);
        expect(r.json).toEqual({ ...http[i]!.res!.json, idempotentReplay: true });
      });
      await sendBatch(qs.url, msgs.map((m) => ({ body: m, group: m.data.walletId })));
      await eventually(async () => sumLogged(c, 'duplicate message') >= dupBefore + msgs.length && (await drained(qs.url)), {
        timeout: 60_000,
        interval: 250,
      });
      await check();

      await eventually(async () => (await pendingOutbox(ws.map((w) => w.id))) === 0, { timeout: 30_000, interval: 200 });
      expect(await queueCounts(sqs, qs.dlqUrl)).toEqual({ visible: 0, inFlight: 0 });
      console.info(`[cenário 6] http=${http.length} sqs=${msgs.length} instâncias=${c.all.length}`);
    }),
  180_000,
);

test('7. SIGTERM com mensagens em voo: sai com 0, nada perdido nem duplicado', () =>
  // visibilidade longa: se a instância não devolvesse as mensagens, elas ficariam presas por 30s
  scenario({ visibility: 30 }, async ({ c, qs }) => {
    const x = await c.spawn();
    const w = await newWallet('1000.00');
    const msgs = Array.from({ length: 60 }, () => toMessage(w.cmd('BET', '1.00', uid())));
    await sendBatch(qs.url, msgs.map((m) => ({ body: m, group: w.id }))); // um grupo: em voo dentro da instância

    await eventually(async () => (await debitsOf(w.id)) >= 5, { timeout: 15_000, interval: 20 });
    const t0 = Date.now();
    x.kill('SIGTERM');
    expect(await within(x.exited, 30_000, 'SIGTERM')).toBe(0);
    const elapsed = Date.now() - t0;
    expect(elapsed).toBeLessThan(15_000);
    // nada ficou invisível: o que não foi iniciado voltou para a fila na hora
    expect((await queueCounts(sqs, qs.url)).inFlight).toBe(0);
    const doneByX = await debitsOf(w.id);
    expect(doneByX).toBe(x.logged('message processed'));

    const y = await c.spawn();
    await eventually(async () => (await debitsOf(w.id)) === 60 && (await drained(qs.url)), { timeout: 20_000, interval: 200 });
    expect(x.logged('message processed') + y.logged('message processed')).toBe(60);
    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.balance.toFixed(2)).toBe('940.00');
    expect(inv.debits).toBe(60);
    console.info(`[cenário 7] SIGTERM -> exit 0 em ${elapsed}ms; processadas antes=${doneByX}, depois=${60 - doneByX}`);
  }));
