import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import { TransientInfrastructureError } from '../../src/application/errors';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import type { UnitOfWork } from '../../src/application/ports';
import { SubmitWagerTransactionUseCase, type SubmitResult } from '../../src/application/submit-wager-transaction';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { Sqs } from '../../src/infrastructure/sqs/sqs.client';
import {
  CONSUMER_NAME,
  messageHash,
  WagerConsumer,
  type Outcome,
  type WagerConsumerOptions,
} from '../../src/infrastructure/sqs/wager-consumer';
import { assertWalletConsistent } from '../support/invariants';

// Requer Postgres migrado e LocalStack. Cada teste usa filas FIFO temporárias (principal + DLQ com redrive).
let orm: Orm;
let uow: MikroOrmUnitOfWork;
let open: OpenWalletUseCase;
let submit: SubmitWagerTransactionUseCase;
let sqs: Sqs;
const created: string[] = [];

beforeAll(async () => {
  orm = await createOrm();
  uow = new MikroOrmUnitOfWork(orm);
  open = new OpenWalletUseCase(uow);
  submit = new SubmitWagerTransactionUseCase(uow);
  sqs = new Sqs();
});
afterAll(async () => {
  await Promise.all(created.map((QueueUrl) => sqs.client.send(new DeleteQueueCommand({ QueueUrl })).catch(() => undefined)));
  sqs.client.destroy();
  await orm.close();
});

const uid = () => crypto.randomUUID();
const q = (sql: string, p: unknown[] = []) => orm.em.getConnection().execute(sql, p) as Promise<any[]>;
const balanceOf = async (walletId: string) => (await q(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [walletId]))[0].b;
const debitsOf = async (walletId: string) =>
  Number((await q(`SELECT count(*) AS n FROM wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`, [walletId]))[0].n);
const inboxOf = (messageId: string) =>
  q(`SELECT payload_hash, processed_at FROM inbox_messages WHERE consumer_name = ? AND message_id = ?`, [CONSUMER_NAME, messageId]);

type Queues = { name: string; dlqName: string; url: string; dlqUrl: string };
async function tempQueues({ maxReceiveCount = 3, visibility = 2 } = {}): Promise<Queues> {
  const suffix = uid();
  const name = `wt-${suffix}.fifo`;
  const dlqName = `wt-dlq-${suffix}.fifo`;
  const fifo = { FifoQueue: 'true', ContentBasedDeduplication: 'false' };
  const dlq = await sqs.client.send(new CreateQueueCommand({ QueueName: dlqName, Attributes: fifo }));
  const arn = (await sqs.client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] })))
    .Attributes!.QueueArn!;
  const main = await sqs.client.send(
    new CreateQueueCommand({
      QueueName: name,
      Attributes: {
        ...fifo,
        VisibilityTimeout: String(visibility),
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: String(maxReceiveCount) }),
      },
    }),
  );
  created.push(main.QueueUrl!, dlq.QueueUrl!);
  return { name, dlqName, url: main.QueueUrl!, dlqUrl: dlq.QueueUrl! };
}

const consumer = (qs: Queues, opts: WagerConsumerOptions = {}, useCase = submit) =>
  new WagerConsumer(useCase, sqs, { queueName: qs.name, dlqName: qs.dlqName, waitTimeSeconds: 1, ...opts });

/** MessageDeduplicationId sempre novo: o broker não deduplica, quem deduplica é o inbox. */
const send = (url: string, body: unknown, groupId: string) =>
  sqs.client.send(
    new SendMessageCommand({
      QueueUrl: url,
      MessageBody: typeof body === 'string' ? body : JSON.stringify(body),
      MessageGroupId: groupId,
      MessageDeduplicationId: uid(),
    }),
  );

async function counts(url: string) {
  const a = (
    await sqs.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: url,
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
      }),
    )
  ).Attributes!;
  return { visible: Number(a.ApproximateNumberOfMessages), inFlight: Number(a.ApproximateNumberOfMessagesNotVisible) };
}

async function readDlq(url: string, expected: number, timeoutMs = 10_000): Promise<Message[]> {
  const got: Message[] = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < expected && Date.now() < deadline) {
    const r = await sqs.client.send(
      new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30, MessageAttributeNames: ['All'] }),
    );
    got.push(...(r.Messages ?? []));
  }
  return got;
}

/** Roda runOnce em todos os consumidores, em paralelo, até `done`. */
async function until(cs: WagerConsumer[], done: (o: Outcome[]) => boolean, timeoutMs = 20_000): Promise<Outcome[]> {
  const all: Outcome[] = [];
  const deadline = Date.now() + timeoutMs;
  await Promise.all(
    cs.map(async (c) => {
      while (!done(all) && Date.now() < deadline) all.push(...(await c.runOnce()));
    }),
  );
  return all;
}
const count = (o: Outcome[], k: Outcome) => o.filter((x) => x === k).length;

async function newWallet(initial = '100.00') {
  const playerId = uid();
  const w = await open.execute({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
  const roundId = uid();
  const msg = (kind: string, amount: string, ext: string, over: Record<string, unknown> = {}) => ({
    messageId: uid(),
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: {
      providerId: 'prov-sqs',
      externalTransactionId: `${w.id}:${ext}`,
      idempotencyKey: `${w.id}:${ext}`,
      playerId,
      walletId: w.id,
      roundId,
      gameId: 'g1',
      kind,
      money: { amount, currency: 'BRL' },
    },
    ...over,
  });
  return { id: w.id, msg };
}

describe('consumidor SQS: caminho feliz e idempotência', () => {
  test('BET via fila => PROCESSED; saldo, ledger, outbox e inbox gravados; mensagem removida', async () => {
    const qs = await tempQueues();
    const w = await newWallet();
    const m = w.msg('BET', '25.00', 'b1', { correlationId: 'corr-1' });
    await send(qs.url, m, w.id);

    const results: SubmitResult[] = [];
    const c = consumer(qs, { beforeAck: async (ctx) => void results.push(ctx.result!) });
    expect(await until([c], (o) => o.length >= 1)).toEqual(['processed']);

    expect(results[0]).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });
    const [tx] = await q(`SELECT id, status FROM wager_transactions WHERE external_transaction_id = ?`, [m.data.externalTransactionId]);
    expect(tx.status).toBe('PROCESSED');
    expect(await balanceOf(w.id)).toBe('75.00');
    expect(await debitsOf(w.id)).toBe(1);
    const events = await q(`SELECT event_type, payload FROM outbox_messages WHERE aggregate_id = ? ORDER BY seq`, [w.id]);
    const own = events.filter((e) => e.payload.causationId === m.messageId);
    expect(own.map((e) => e.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    expect(own.every((e) => e.payload.correlationId === 'corr-1')).toBe(true);
    const [inbox] = await inboxOf(m.messageId);
    expect(inbox.payload_hash).toBe(messageHash(m as never));
    expect(inbox.processed_at).not.toBeNull();
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
    expect(c.counters).toMatchObject({ processed: 1, duplicates: 0, retries: 0, deadLettered: 0 });
    await assertWalletConsistent(orm, w.id);
  });

  test('mesma mensagem 2x (messageId igual, dedup do broker diferente) => um efeito, 2ª acked como duplicata', async () => {
    const qs = await tempQueues();
    const w = await newWallet();
    const m = w.msg('BET', '10.00', 'b1');
    await send(qs.url, m, w.id);
    await send(qs.url, m, w.id);

    const c = consumer(qs);
    const out = await until([c], (o) => o.length >= 2);
    expect(out).toEqual(['processed', 'duplicate']);
    expect(c.counters).toMatchObject({ processed: 1, duplicates: 1 });
    expect(await balanceOf(w.id)).toBe('90.00');
    expect(await debitsOf(w.id)).toBe(1);
    expect(await inboxOf(m.messageId)).toHaveLength(1);
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
  });

  test('mesmo externalTransactionId e key, messageId novo => replay do resultado original sem duplicar', async () => {
    const qs = await tempQueues();
    const w = await newWallet();
    const first = w.msg('BET', '10.00', 'b1');
    const again = { ...first, messageId: uid() };
    await send(qs.url, first, w.id);
    await send(qs.url, again, w.id);

    const results: SubmitResult[] = [];
    const c = consumer(qs, { beforeAck: async (ctx) => void results.push(ctx.result!) });
    expect(await until([c], (o) => o.length >= 2)).toEqual(['processed', 'processed']);
    expect(results[1]).toEqual({ ...results[0]!, idempotentReplay: true });
    expect(await debitsOf(w.id)).toBe(1);
    expect((await inboxOf(again.messageId))[0].processed_at).not.toBeNull();
  });

  test('falha entre commit e ack => redelivery cai no inbox, sem duplo débito', async () => {
    const qs = await tempQueues({ visibility: 1 });
    const w = await newWallet();
    const m = w.msg('BET', '30.00', 'b1');
    await send(qs.url, m, w.id);

    const crashing = consumer(qs, {
      beforeAck: async () => {
        throw new Error('processo morreu antes do ack');
      },
    });
    expect(await until([crashing], (o) => o.length >= 1)).toEqual(['ack-failed']);
    expect(await balanceOf(w.id)).toBe('70.00'); // commit feito

    // a visibilidade expira e a mensagem volta
    const c = consumer(qs);
    expect(await until([c], (o) => o.length >= 1)).toEqual(['duplicate']);
    expect(await balanceOf(w.id)).toBe('70.00');
    expect(await debitsOf(w.id)).toBe(1);
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
  });
});

describe('consumidor SQS: classificação de erros', () => {
  test('saldo insuficiente => REJECTED persistido e mensagem removida (ack de negócio)', async () => {
    const qs = await tempQueues();
    const w = await newWallet('5.00');
    const m = w.msg('BET', '10.00', 'b1');
    await send(qs.url, m, w.id);

    const c = consumer(qs);
    expect(await until([c], (o) => o.length >= 1)).toEqual(['processed']);
    const [tx] = await q(`SELECT status, failure_code FROM wager_transactions WHERE external_transaction_id = ?`, [m.data.externalTransactionId]);
    expect(tx).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
    expect(await balanceOf(w.id)).toBe('5.00');
    expect((await inboxOf(m.messageId))[0].processed_at).not.toBeNull();
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
  });

  test('conflito de idempotência (mesma key, payload diferente) => ack, sem efeito e sem inbox', async () => {
    const qs = await tempQueues();
    const w = await newWallet();
    const first = w.msg('BET', '10.00', 'b1');
    const conflicting = w.msg('BET', '11.00', 'b1');
    await send(qs.url, first, w.id);
    await send(qs.url, conflicting, w.id);

    const c = consumer(qs);
    expect(await until([c], (o) => o.length >= 2)).toEqual(['processed', 'business-error']);
    expect(c.counters.conflicts).toBe(1);
    expect(await debitsOf(w.id)).toBe(1);
    expect(await inboxOf(conflicting.messageId)).toHaveLength(0); // rollback levou o inbox junto
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
  });

  test('JSON quebrado, schema inválido, OPENING e messageId reaproveitado com outro payload => DLQ explícita', async () => {
    const qs = await tempQueues();
    const w = await newWallet();
    const ok = w.msg('BET', '10.00', 'b1');
    const reused = { ...w.msg('BET', '20.00', 'b2'), messageId: ok.messageId }; // mesmo messageId, conteúdo diferente
    const noKind = w.msg('BET', '1.00', 'b3');
    delete (noKind.data as Partial<typeof noKind.data>).kind;
    const bodies: [string, unknown][] = [
      ['g-json', '{"messageId": "quebrado"'],
      ['g-schema', noKind],
      ['g-opening', w.msg('OPENING', '1.00', 'o1')],
      ['g-money', w.msg('BET', '1.234', 'b4')],
    ];
    await send(qs.url, ok, w.id);
    await send(qs.url, reused, w.id);
    for (const [g, b] of bodies) await send(qs.url, b, g);

    const c = consumer(qs);
    const out = await until([c], (o) => o.length >= 6);
    expect(count(out, 'processed')).toBe(1);
    expect(count(out, 'dead-lettered')).toBe(5);
    expect(c.counters.deadLettered).toBe(5);
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
    expect(await debitsOf(w.id)).toBe(1);

    const dlq = await readDlq(qs.dlqUrl, 5);
    expect(dlq).toHaveLength(5);
    const byName = Object.fromEntries(dlq.map((m) => [m.MessageAttributes!.errorName!.StringValue!, m]));
    expect(Object.keys(byName).sort()).toEqual(
      ['InboxPayloadMismatchError', 'InvalidMoneyError', 'InvalidWagerTransactionError', 'SyntaxError', 'ZodError'].sort(),
    );
    for (const m of dlq) {
      expect(m.MessageAttributes!.receiveCount!.StringValue).toBe('1');
      expect(m.MessageAttributes!.reason!.StringValue!.length).toBeGreaterThan(0);
    }
    expect(byName.SyntaxError!.Body).toBe('{"messageId": "quebrado"');
    expect(JSON.parse(byName.InboxPayloadMismatchError!.Body!)).toEqual(reused);
  });

  test('erro transitório => visibilidade com backoff, sem delete; após maxReceiveCount vai à DLQ por redrive', async () => {
    const qs = await tempQueues({ maxReceiveCount: 2, visibility: 30 });
    const w = await newWallet();
    const m = w.msg('BET', '10.00', 'b1');
    await send(qs.url, m, w.id);

    // stub de falha só do UnitOfWork; fila, caso de uso e consumidor são os reais
    const down: UnitOfWork = {
      run: async () => {
        throw new TransientInfrastructureError('Database unavailable');
      },
    };
    const c = consumer(qs, {}, new SubmitWagerTransactionUseCase(down));

    const t0 = Date.now();
    expect(await c.runOnce()).toEqual(['retry']);
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 1 }); // não deletada
    // 1ª falha => 2s (bem antes dos 30s do VisibilityTimeout da fila)
    expect(await until([c], (o) => o.length >= 1, 10_000)).toEqual(['retry']);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(1_500);
    expect(c.counters.retries).toBe(2);

    // 2ª falha => 4s; na próxima entrega o SQS excede maxReceiveCount=2 e move para a DLQ
    const dlq = await readDlq(qs.dlqUrl, 1, 1_000);
    expect(dlq).toHaveLength(0);
    const deadline = Date.now() + 15_000;
    let moved: Message[] = [];
    while (moved.length === 0 && Date.now() < deadline) {
      await c.runOnce();
      moved = await readDlq(qs.dlqUrl, 1, 1_000);
    }
    expect(moved).toHaveLength(1);
    expect(JSON.parse(moved[0]!.Body!)).toEqual(m);
    expect(c.counters.retries).toBe(2);
    expect(c.counters.deadLettered).toBe(0); // foi a redrive policy, não o consumidor
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
    expect(await debitsOf(w.id)).toBe(0);
  });
});

describe('consumidor SQS: concorrência', () => {
  test('50 mensagens da mesma wallet (+10 duplicatas) em 4 consumidores concorrentes => saldo == ledger', async () => {
    const qs = await tempQueues({ visibility: 30 });
    const w = await newWallet('1000.00');
    const msgs = Array.from({ length: 50 }, (_, i) => w.msg('BET', '1.00', `b${i}`));
    const dups = msgs.slice(0, 10);
    // grupos distintos: as mensagens da mesma wallet chegam de fato em paralelo; o banco serializa
    const entries = [...msgs, ...dups].map((m, i) => ({
      Id: String(i),
      MessageBody: JSON.stringify(m),
      MessageGroupId: `g-${i % 25}`,
      MessageDeduplicationId: uid(),
    }));
    for (let i = 0; i < entries.length; i += 10) {
      await sqs.client.send(new SendMessageBatchCommand({ QueueUrl: qs.url, Entries: entries.slice(i, i + 10) }));
    }

    const cs = Array.from({ length: 4 }, () => consumer(qs));
    const out = await until(cs, (o) => count(o, 'processed') + count(o, 'duplicate') >= 60, 60_000);
    expect(count(out, 'processed')).toBe(50);
    expect(count(out, 'duplicate')).toBe(10);
    expect(cs.filter((c) => c.counters.processed > 0).length).toBeGreaterThan(1);

    const { balance, debits } = await assertWalletConsistent(orm, w.id);
    expect(balance.toFixed(2)).toBe('950.00');
    expect(debits).toBe(50);
    expect(await counts(qs.url)).toEqual({ visible: 0, inFlight: 0 });
  });
});

describe('consumidor SQS: graceful shutdown', () => {
  test('stop() conclui a mensagem iniciada e devolve as não iniciadas (visíveis na hora)', async () => {
    const qs = await tempQueues({ visibility: 30 });
    const w = await newWallet();
    const msgs = Array.from({ length: 5 }, (_, i) => w.msg('BET', '1.00', `b${i}`));
    for (const m of msgs) await send(qs.url, m, w.id); // mesmo grupo: processadas em sequência

    let stopping: Promise<void> | undefined;
    const c = consumer(qs, {
      beforeAck: async () => {
        stopping ??= c.stop(); // SIGTERM no meio do lote
      },
    });
    const out = await c.runOnce();
    await stopping;
    expect(out[0]).toBe('processed');
    expect(out.slice(1).every((o) => o === 'released')).toBe(true);
    expect(await c.runOnce()).toEqual([]); // parado: não faz mais polling

    expect(await counts(qs.url)).toEqual({ visible: 4, inFlight: 0 }); // não esperou os 30s
    expect(await debitsOf(w.id)).toBe(1);

    const next = consumer(qs);
    expect(await until([next], (o) => o.length >= 4, 10_000)).toEqual(Array(4).fill('processed'));
    expect(await debitsOf(w.id)).toBe(5);
  });

  test('handler que excede o timeout: stop() devolve a visibilidade e a redelivery vira duplicata', async () => {
    const qs = await tempQueues({ visibility: 30 });
    const w = await newWallet();
    const m = w.msg('BET', '10.00', 'b1');
    await send(qs.url, m, w.id);

    let entered!: () => void;
    const inAck = new Promise<void>((r) => (entered = r));
    const slow = consumer(qs, {
      shutdownTimeoutMs: 300,
      beforeAck: async () => {
        entered();
        await Bun.sleep(4_000);
      },
    });
    const running = slow.runOnce();
    await inAck; // commit feito, ack ainda não
    const t0 = Date.now();
    await slow.stop();
    expect(Date.now() - t0).toBeLessThan(2_000);

    const other = consumer(qs);
    expect(await until([other], (o) => o.length >= 1, 3_000)).toEqual(['duplicate']);
    await running;
    expect(await debitsOf(w.id)).toBe(1);
  });
});
