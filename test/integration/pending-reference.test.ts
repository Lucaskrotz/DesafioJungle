import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import { SubmitWagerTransactionUseCase, type SubmitWagerCommand } from '../../src/application/submit-wager-transaction';
import { createApp } from '../../src/bootstrap';
import { MAX_REFERENCE_ATTEMPTS } from '../../src/domain/wager-transaction';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { PendingReferenceWorker } from '../../src/infrastructure/workers/pending-reference-worker';
import { assertWalletConsistent } from '../support/invariants';

// Requer Postgres migrado. O relógio do worker é injetado (`now`) para avançar o tempo sem esperar.
// Lote grande: pendências deixadas por outras suítes também vencem e não podem esconder as do teste.
let orm: Orm;
let uow: MikroOrmUnitOfWork;
let open: OpenWalletUseCase;
let submit: SubmitWagerTransactionUseCase;
let app: INestApplication;
let base: string;
const worker = () => new PendingReferenceWorker(uow, { batchSize: 1_000 });

beforeAll(async () => {
  orm = await createOrm();
  uow = new MikroOrmUnitOfWork(orm);
  open = new OpenWalletUseCase(uow);
  submit = new SubmitWagerTransactionUseCase(uow);
  app = await createApp();
  await app.listen(0);
  base = (await app.getUrl()).replace('[::1]', 'localhost');
});
afterAll(async () => {
  await app.close();
  await orm.close();
});

const uid = () => crypto.randomUUID();
const q = (sql: string, p: unknown[] = []) => orm.em.getConnection().execute(sql, p) as Promise<any[]>;
const txByExt = async (ext: string) => (await q(`SELECT * FROM wager_transactions WHERE external_transaction_id = ?`, [ext]))[0];
const hours = (n: number) => new Date(Date.now() + n * 3_600_000);

async function newWallet(initial = '100.00') {
  const playerId = uid();
  const w = await open.execute({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
  const cmd = (kind: string, amount: string, ext: string, roundId: string, ref?: string): SubmitWagerCommand => ({
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

async function post(c: SubmitWagerCommand) {
  const { idempotencyKey, ...body } = c;
  const res = await fetch(base + '/wagering/transactions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
    body: JSON.stringify(body),
  });
  return { status: res.status, json: (await res.json()) as any };
}

const balance = async (walletId: string) => (await q(`SELECT balance::text AS b FROM wallets WHERE id = ?`, [walletId]))[0].b;

describe('worker de PENDING_REFERENCE', () => {
  test('REFUND, ROLLBACK e WIN chegam antes da BET: 202 e depois PROCESSED pelo worker', async () => {
    const w = await newWallet('100.00');
    const [r1, r2, r3] = [uid(), uid(), uid()];
    const early = [
      w.cmd('REFUND', '10.00', 'rf', r1, 'b1'),
      w.cmd('ROLLBACK', '10.00', 'rb', r2, 'b2'),
      w.cmd('WIN', '25.00', 'wn', r3, 'b3'),
    ];
    for (const c of early) {
      const r = await post(c);
      expect(r.status).toBe(202);
      expect(r.json.status).toBe('PENDING_REFERENCE');
    }

    // referência ainda ausente: o worker só reagenda
    await worker().runOnce(hours(1));
    for (const c of early) expect((await txByExt(c.externalTransactionId)).status).toBe('PENDING_REFERENCE');

    for (const [ext, round] of [['b1', r1], ['b2', r2], ['b3', r3]] as const) {
      expect((await post(w.cmd('BET', '10.00', ext, round))).json.status).toBe('PROCESSED');
    }
    expect(await balance(w.id)).toBe('70.00');

    await worker().runOnce(hours(2));
    for (const c of early) {
      const tx = await txByExt(c.externalTransactionId);
      expect(tx.status).toBe('PROCESSED');
      expect(tx.reference_transaction_id).toBe((await txByExt(c.referenceExternalTransactionId!)).id);
      const ledger = await q(`SELECT 1 FROM wallet_ledger_entries WHERE transaction_id = ?`, [tx.id]);
      expect(ledger).toHaveLength(1);
      const events = await q(`SELECT event_type FROM outbox_messages WHERE payload->'data'->>'transactionId' = ? ORDER BY seq`, [tx.id]);
      expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference', 'WagerTransactionProcessed', 'WalletBalanceChanged']);
    }
    // 100 - 30 + 10 (refund) + 10 (rollback da BET) + 25 (win)
    expect(await balance(w.id)).toBe('115.00');
    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.entries).toBe(7);

    // replay pelo HTTP devolve o estado resolvido
    expect((await post(early[0]!)).json).toMatchObject({ status: 'PROCESSED', idempotentReplay: true });
  });

  test('referência nunca chega: após MAX tentativas => REJECTED REFERENCE_NOT_FOUND + evento', async () => {
    const w = await newWallet('100.00');
    const c = w.cmd('REFUND', '10.00', 'rf', uid(), 'nunca');
    expect((await post(c)).status).toBe(202);

    for (let i = 1; i <= MAX_REFERENCE_ATTEMPTS; i++) {
      await worker().runOnce(hours(i)); // 1h por rodada supera o teto do backoff (5 min)
      const tx = await txByExt(c.externalTransactionId);
      if (i < MAX_REFERENCE_ATTEMPTS) {
        expect(tx.status).toBe('PENDING_REFERENCE');
        expect(tx.reference_attempts).toBe(i);
      } else {
        expect(tx.status).toBe('REJECTED');
        expect(tx.failure_code).toBe('REFERENCE_NOT_FOUND');
      }
    }
    const tx = await txByExt(c.externalTransactionId);
    const rejected = await q(`SELECT payload FROM outbox_messages WHERE payload->'data'->>'transactionId' = ? AND event_type = 'WagerTransactionRejected'`, [
      tx.id,
    ]);
    expect(rejected).toHaveLength(1);
    expect(rejected[0].payload.data.failureCode).toBe('REFERENCE_NOT_FOUND');
    expect(await balance(w.id)).toBe('100.00');
    await assertWalletConsistent(orm, w.id);
    expect((await post(c)).json).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_NOT_FOUND', idempotentReplay: true });
  });

  test('workers concorrentes sobre os mesmos pendentes não duplicam efeito', async () => {
    const w = await newWallet('100.00');
    const rounds = Array.from({ length: 20 }, () => uid());
    const refunds = rounds.map((r, i) => w.cmd('REFUND', '2.00', `rf${i}`, r, `b${i}`));
    const orphans = Array.from({ length: 5 }, (_, i) => w.cmd('REFUND', '2.00', `orf${i}`, uid(), `nunca${i}`));
    for (const c of [...refunds, ...orphans]) expect((await submit.execute(c)).status).toBe('PENDING_REFERENCE');
    for (const [i, r] of rounds.entries()) await submit.execute(w.cmd('BET', '2.00', `b${i}`, r));
    expect(await balance(w.id)).toBe('60.00');

    const now = hours(1);
    const results = await Promise.all(Array.from({ length: 4 }, () => worker().runOnce(now)));
    expect(results.every((r) => r.failed === 0)).toBe(true);

    for (const c of refunds) {
      const tx = await txByExt(c.externalTransactionId);
      expect(tx.status).toBe('PROCESSED');
      const processed = await q(
        `SELECT 1 FROM outbox_messages WHERE payload->'data'->>'transactionId' = ? AND event_type = 'WagerTransactionProcessed'`,
        [tx.id],
      );
      expect(processed).toHaveLength(1);
    }
    // reagendamento também uma única vez por pendente
    for (const c of orphans) expect((await txByExt(c.externalTransactionId)).reference_attempts).toBe(1);
    expect(await balance(w.id)).toBe('100.00');
    expect((await assertWalletConsistent(orm, w.id)).entries).toBe(1 + 20 + 20);
  });
});
