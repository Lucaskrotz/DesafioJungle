import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { createApp } from '../../src/bootstrap';

let app: INestApplication;
let base: string;

beforeAll(async () => {
  app = await createApp();
  await app.listen(0);
  base = await app.getUrl();
  base = base.replace('[::1]', 'localhost');
});
afterAll(() => app.close());

const uid = () => crypto.randomUUID();
const money = (amount: string, currency = 'BRL') => ({ amount, currency });

async function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(base + path, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { res, status: res.status, json: (await res.json()) as any };
}

const newWallet = async (initial = '100.00') => {
  const playerId = uid();
  const { json } = await call('POST', '/wallets', { playerId, initialBalance: money(initial) });
  return { playerId, walletId: json.id as string };
};

const wager = (w: { playerId: string; walletId: string }, over: Record<string, unknown> = {}) => ({
  providerId: 'prov-a',
  externalTransactionId: uid(),
  playerId: w.playerId,
  walletId: w.walletId,
  roundId: uid(),
  gameId: 'game-1',
  kind: 'BET',
  money: money('25.00'),
  ...over,
});

const submit = (body: Record<string, unknown>, key: string | null = `${body.providerId}:${body.externalTransactionId}`) =>
  call('POST', '/wagering/transactions', body, key === null ? {} : { 'idempotency-key': key });

describe('wallets', () => {
  test('cria wallet (201) e rejeita duplicada (409)', async () => {
    const playerId = uid();
    const body = { playerId, initialBalance: money('1000.00') };
    const a = await call('POST', '/wallets', body);
    expect(a.status).toBe(201);
    expect(Object.keys(a.json).sort()).toEqual(['balance', 'id', 'playerId', 'version']);
    expect(a.json.playerId).toBe(playerId);
    expect(a.json.balance).toEqual(money('1000.00'));
    const b = await call('POST', '/wallets', body);
    expect(b.status).toBe(409);
    expect(b.json.error).toBe('WALLET_ALREADY_EXISTS');
  });

  test.each(['10.001', '-1', '1e3'])('saldo inicial inválido %s (400)', async (amount) => {
    const r = await call('POST', '/wallets', { playerId: uid(), initialBalance: money(amount) });
    expect(r.status).toBe(400);
    expect(r.json.error).toBe('INVALID_PAYLOAD');
  });

  test('amount numérico e campo extra são rejeitados', async () => {
    const n = await call('POST', '/wallets', { playerId: uid(), initialBalance: { amount: 10, currency: 'BRL' } });
    expect(n.status).toBe(400);
    const x = await call('POST', '/wallets', { playerId: uid(), initialBalance: money('1.00'), extra: 1 });
    expect(x.status).toBe(400);
  });

  test('GET wallet, ledger com paginação, 404 e cursor inválido', async () => {
    const w = await newWallet('100.00');
    for (let i = 0; i < 2; i++) expect((await submit(wager(w))).status).toBe(200);

    const got = await call('GET', `/wallets/${w.walletId}`);
    expect(got.status).toBe(200);
    expect(got.json.balance).toEqual(money('50.00'));

    const p1 = await call('GET', `/wallets/${w.walletId}/ledger?limit=2`);
    expect(p1.status).toBe(200);
    expect(p1.json.items).toHaveLength(2);
    expect(p1.json.nextCursor).toBeString();
    const p2 = await call('GET', `/wallets/${w.walletId}/ledger?limit=2&cursor=${p1.json.nextCursor}`);
    expect(p2.json.items).toHaveLength(1);
    expect(p2.json.nextCursor).toBeNull();
    const ids = [...p1.json.items, ...p2.json.items].map((i: any) => i.id);
    expect(new Set(ids).size).toBe(3);

    expect((await call('GET', `/wallets/${w.walletId}/ledger?cursor=lixo`)).status).toBe(400);
    expect((await call('GET', `/wallets/${w.walletId}/ledger?limit=0`)).status).toBe(400);
    expect((await call('GET', `/wallets/${uid()}`)).status).toBe(404);
    expect((await call('GET', `/wallets/${uid()}/ledger`)).status).toBe(404);
    expect((await call('GET', '/wallets/nao-uuid')).status).toBe(400);
  });
});

describe('wagering', () => {
  test('BET 200, replay idêntico, consultas de transação', async () => {
    const w = await newWallet();
    const body = wager(w);
    const first = await submit(body);
    expect(first.status).toBe(200);
    expect(first.json).toMatchObject({ status: 'PROCESSED', balance: money('75.00'), idempotentReplay: false });

    const replay = await submit(body);
    expect(replay.status).toBe(200);
    expect(replay.json).toEqual({ ...first.json, idempotentReplay: true });

    const byId = await call('GET', `/wagering/transactions/${first.json.transactionId}`);
    expect(byId.status).toBe(200);
    expect(byId.json).toMatchObject({ status: 'PROCESSED', kind: 'BET', walletId: w.walletId });
    const byExt = await call('GET', `/providers/prov-a/wagering/transactions/${body.externalTransactionId}`);
    expect(byExt.status).toBe(200);
    expect(byExt.json.transactionId).toBe(first.json.transactionId);

    expect((await call('GET', `/wagering/transactions/${uid()}`)).status).toBe(404);
    expect((await call('GET', `/providers/prov-a/wagering/transactions/${uid()}`)).status).toBe(404);
  });

  test('mesma key com payload diferente (409)', async () => {
    const w = await newWallet();
    const body = wager(w);
    const key = `k-${uid()}`;
    expect((await submit(body, key)).status).toBe(200);
    const r = await submit({ ...body, money: money('26.00') }, key);
    expect(r.status).toBe(409);
    expect(r.json.error).toBe('IDEMPOTENCY_CONFLICT');
  });

  test('header ausente/vazio (400) e kind OPENING (400)', async () => {
    const w = await newWallet();
    const none = await submit(wager(w), null);
    expect(none.status).toBe(400);
    expect(none.json.error).toBe('INVALID_PAYLOAD');
    expect((await submit(wager(w), '')).status).toBe(400);
    const opening = await submit(wager(w, { kind: 'OPENING' }));
    expect(opening.status).toBe(400);
    expect(opening.json.error).toBe('INVALID_PAYLOAD');
  });

  test('saldo insuficiente (422) e replay também 422', async () => {
    const w = await newWallet('10.00');
    const body = wager(w);
    const first = await submit(body);
    expect(first.status).toBe(422);
    expect(first.json).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', idempotentReplay: false });
    const replay = await submit(body);
    expect(replay.status).toBe(422);
    expect(replay.json).toEqual({ ...first.json, idempotentReplay: true });
  });

  test('REFUND sem referência existente (202 PENDING_REFERENCE)', async () => {
    const w = await newWallet();
    const r = await submit(wager(w, { kind: 'REFUND', referenceExternalTransactionId: `ref-${uid()}` }));
    expect(r.status).toBe(202);
    expect(r.json.status).toBe('PENDING_REFERENCE');
  });

  test('wallet inexistente (422 WALLET_NOT_FOUND)', async () => {
    const r = await submit(wager({ playerId: uid(), walletId: uid() }));
    expect(r.status).toBe(422);
    expect(r.json).toMatchObject({ status: 'REJECTED', failureCode: 'WALLET_NOT_FOUND' });
  });
});

describe('health', () => {
  test('live e ready', async () => {
    const live = await call('GET', '/health/live');
    expect(live).toMatchObject({ status: 200, json: { status: 'ok' } });
    const ready = await call('GET', '/health/ready');
    expect(ready).toMatchObject({ status: 200, json: { status: 'ok', checks: { postgres: 'up' } } });
  });
});
