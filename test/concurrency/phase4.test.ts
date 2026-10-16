import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { IdempotencyConflictError } from '../../src/application/errors';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import { SubmitWagerTransactionUseCase, type SubmitResult, type SubmitWagerCommand } from '../../src/application/submit-wager-transaction';
import { createApp } from '../../src/bootstrap';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { assertWalletConsistent } from '../support/invariants';

// Requer Postgres migrado. Todas as chamadas de cada cenário disparam juntas (Promise.all).
let orm: Orm;
let open: OpenWalletUseCase;
let submit: SubmitWagerTransactionUseCase;
let app: INestApplication;
let base: string;

beforeAll(async () => {
  orm = await createOrm();
  const uow = new MikroOrmUnitOfWork(orm);
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
const money = (amount: string) => ({ amount, currency: 'BRL' });
type W = { id: string; playerId: string };

async function newWallet(initial: string): Promise<W> {
  const playerId = uid();
  const w = await open.execute({ playerId, initialBalance: money(initial) });
  return { id: w.id, playerId };
}

const cmd = (w: W, over: Partial<SubmitWagerCommand> = {}): SubmitWagerCommand => {
  const ext = uid();
  return {
    idempotencyKey: `k:${ext}`, providerId: 'prov-a', externalTransactionId: ext, playerId: w.playerId, walletId: w.id,
    roundId: uid(), gameId: 'g1', kind: 'BET', money: money('80.00'), ...over,
  };
};

// Executa via use case ou HTTP; devolve resultado normalizado ou conflito.
type Out = { result?: SubmitResult; conflict?: boolean; status?: number };
type Via = 'uc' | 'http';
async function send(via: Via, c: SubmitWagerCommand): Promise<Out> {
  if (via === 'uc') {
    try {
      return { result: await submit.execute(c) };
    } catch (e) {
      if (e instanceof IdempotencyConflictError) return { conflict: true };
      throw e;
    }
  }
  const { idempotencyKey, ...body } = c;
  const res = await fetch(base + '/wagering/transactions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
    body: JSON.stringify(body),
  });
  const json = (await res.json()) as any;
  if (res.status === 409) return { conflict: true, status: 409 };
  expect([200, 422], `HTTP ${res.status}: ${JSON.stringify(json)}`).toContain(res.status);
  return { result: json as SubmitResult, status: res.status };
}
const sendAll = (via: Via, cs: SubmitWagerCommand[]) => Promise.all(cs.map((c) => send(via, c)));

const count = async (sql: string, p: unknown[]) => Number(((await orm.em.getConnection().execute(sql, p)) as any[])[0].n);
const outboxCount = (txId: string, type: string) =>
  count(`SELECT count(*) AS n FROM outbox_messages WHERE payload->'data'->>'transactionId' = ? AND event_type = ?`, [txId, type]);

describe.each(['uc', 'http'] as Via[])('seção 8 (%s)', (via) => {
  test.each(Array.from({ length: via === 'uc' ? 20 : 1 }, (_, i) => i))('duas BETs de 80 em wallet de 100 (rodada %i)', async () => {
    const w = await newWallet('100.00');
    const cs = [cmd(w), cmd(w)];
    const rs = (await sendAll(via, cs)).map((o) => o.result!);
    expect(rs.filter((r) => r.status === 'PROCESSED')).toHaveLength(1);
    const rej = rs.filter((r) => r.status === 'REJECTED');
    expect(rej).toHaveLength(1);
    expect(rej[0]!.failureCode as string).toBe('INSUFFICIENT_FUNDS');
    expect(rs.every((r) => !r.idempotentReplay)).toBe(true);

    // retry das duas: mesmos resultados, replay, sem débito novo
    const again = (await sendAll(via, cs)).map((o) => o.result!);
    again.forEach((r, i) => {
      expect(r.idempotentReplay).toBe(true);
      expect(r.status).toBe(rs[i]!.status);
      expect(r.transactionId).toBe(rs[i]!.transactionId);
      expect(r.failureCode).toBe(rs[i]!.failureCode);
    });

    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.balance.toFixed(2)).toBe('20.00');
    expect(inv.debits).toBe(1);
  });
});

describe.each(['uc', 'http'] as Via[])('mesma Idempotency-Key x50 (%s)', (via) => {
  test('um único débito, 49 replays', async () => {
    const w = await newWallet('100.00');
    const c = cmd(w, { money: money('30.00') });
    const rs = (await sendAll(via, Array.from({ length: 50 }, () => c))).map((o) => o.result!);
    expect(new Set(rs.map((r) => r.transactionId)).size).toBe(1);
    expect(rs.every((r) => r.status === 'PROCESSED')).toBe(true);
    expect(rs.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(rs.filter((r) => r.idempotentReplay)).toHaveLength(49);

    const txId = rs[0]!.transactionId!;
    expect(await count(`SELECT count(*) AS n FROM wager_transactions WHERE wallet_id = ? AND kind = 'BET'`, [w.id])).toBe(1);
    expect(await count(`SELECT count(*) AS n FROM wallet_ledger_entries WHERE transaction_id = ?`, [txId])).toBe(1);
    expect(await outboxCount(txId, 'WalletBalanceChanged')).toBe(1);
    expect(await outboxCount(txId, 'WagerTransactionProcessed')).toBe(1);
    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.balance.toFixed(2)).toBe('70.00');
    expect(inv.debits).toBe(1);
  });
});

describe.each(['uc', 'http'] as Via[])('mesma key, payloads diferentes (%s)', (via) => {
  test('um payload vence, o outro recebe conflito', async () => {
    const w = await newWallet('100.00');
    const a = cmd(w, { money: money('10.00') });
    const b = { ...a, money: money('11.00') };
    const outs = await sendAll(via, [...Array.from({ length: 25 }, () => a), ...Array.from({ length: 25 }, () => b)]);
    const outsA = outs.slice(0, 25);
    const outsB = outs.slice(25);
    const aWins = outsA.some((o) => o.result);
    expect(aWins !== outsB.some((o) => o.result)).toBe(true); // exatamente um lado vence
    const [win, lose] = aWins ? [outsA, outsB] : [outsB, outsA];
    expect(lose.every((o) => o.conflict)).toBe(true);
    if (via === 'http') expect(lose.every((o) => o.status === 409)).toBe(true);
    expect(win.every((o) => o.result?.status === 'PROCESSED')).toBe(true);
    expect(win.filter((o) => !o.result!.idempotentReplay)).toHaveLength(1);

    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.debits).toBe(1);
    expect(inv.balance.toFixed(2)).toBe(aWins ? '90.00' : '89.00');
  });
});

test('30 wallets x 3 BETs simultâneas', async () => {
  const ws = await Promise.all(Array.from({ length: 30 }, () => newWallet('100.00')));
  const outs = await sendAll('uc', ws.flatMap((w) => Array.from({ length: 3 }, () => cmd(w, { money: money('10.00') }))));
  expect(outs.every((o) => o.result?.status === 'PROCESSED')).toBe(true);
  for (const w of ws) {
    const inv = await assertWalletConsistent(orm, w.id);
    expect(inv.balance.toFixed(2)).toBe('70.00');
    expect(inv.debits).toBe(3);
  }
});

test('hot wallet: 100 BETs de 1.00 em 60.00', async () => {
  const w = await newWallet('60.00');
  const rs = (await sendAll('uc', Array.from({ length: 100 }, () => cmd(w, { money: money('1.00') })))).map((o) => o.result!);
  expect(rs.filter((r) => r.status === 'PROCESSED')).toHaveLength(60);
  const rej = rs.filter((r) => r.status === 'REJECTED');
  expect(rej).toHaveLength(40);
  expect(rej.every((r) => (r.failureCode as string) === 'INSUFFICIENT_FUNDS')).toBe(true);
  const inv = await assertWalletConsistent(orm, w.id);
  expect(inv.balance.toFixed(2)).toBe('0.00');
  expect(inv.debits).toBe(60);
});

test.each(Array.from({ length: 10 }, (_, i) => i))('REFUND x ROLLBACK da mesma BET (rodada %i)', async () => {
  const w = await newWallet('100.00');
  const bet = cmd(w, { money: money('40.00') });
  expect((await send('uc', bet)).result!.status).toBe('PROCESSED');
  const rev = (kind: string) => cmd(w, { kind, roundId: bet.roundId, money: money('40.00'), referenceExternalTransactionId: bet.externalTransactionId });
  const rs = (await sendAll('uc', [rev('REFUND'), rev('ROLLBACK')])).map((o) => o.result!);
  const processed = rs.filter((r) => r.status === 'PROCESSED').length;
  expect(processed).toBeLessThanOrEqual(1);
  const rej = rs.filter((r) => r.status === 'REJECTED');
  expect(rej.every((r) => r.failureCode === 'REFERENCE_ALREADY_REVERSED')).toBe(true);
  const inv = await assertWalletConsistent(orm, w.id);
  expect(inv.credits).toBe(1 + processed); // OPENING + reversões
  expect(inv.balance.toFixed(2)).toBe(processed ? '100.00' : '60.00');
});
