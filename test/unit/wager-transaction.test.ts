import { describe, expect, test } from 'bun:test';
import { InvalidTransactionStateError, InvalidWagerTransactionError } from '../../src/domain/errors';
import { FailureCode } from '../../src/domain/failure-code';
import { Money } from '../../src/domain/money';
import { computePayloadHash } from '../../src/domain/payload-hash';
import {
  CreateWagerTransactionProps,
  referenceBackoffMs,
  WagerTransaction,
  WagerTransactionKind as K,
  WagerTransactionStatus as S,
} from '../../src/domain/wager-transaction';
import { LedgerDirection } from '../../src/domain/wallet-ledger-entry';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-01-01T00:00:00Z');

let seq = 0;
function tx(over: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  seq += 1;
  return WagerTransaction.create({
    id: `t${seq}`, providerId: 'prov', externalTransactionId: `ext${seq}`, idempotencyKey: `prov:ext${seq}`,
    payloadHash: 'h', walletId: 'w1', playerId: 'p1', roundId: 'r1', gameId: 'g1', kind: K.Bet,
    money: brl('25.00'), createdAt: at, ...over,
  });
}
const processed = (t: WagerTransaction) => (t.markProcessed(undefined, at, brl('0')), t);

describe('WagerTransaction: criação', () => {
  test('nasce PENDING', () => expect(tx().status).toBe(S.Pending));

  test('OPENING não pode ser criado pela via externa', () => {
    expect(() => tx({ kind: K.Opening })).toThrow(InvalidWagerTransactionError);
    expect(WagerTransaction.opening({ id: 'o', walletId: 'w1', playerId: 'p1', payloadHash: 'h', money: brl('10'), createdAt: at }).kind).toBe(K.Opening);
  });

  test.each([K.Refund, K.Rollback])('%s exige referência', (kind) => {
    expect(() => tx({ kind })).toThrow(InvalidWagerTransactionError);
    expect(tx({ kind, referenceExternalTransactionId: 'x' }).requiresReference()).toBe(true);
  });

  test('BET não aceita referência; valor deve ser positivo', () => {
    expect(() => tx({ referenceExternalTransactionId: 'x' })).toThrow(InvalidWagerTransactionError);
    expect(() => tx({ money: Money.zero('BRL') })).toThrow(InvalidWagerTransactionError);
  });

  test('LOSS não afeta saldo e não tem direção de ledger', () => {
    const loss = tx({ kind: K.Loss });
    expect(loss.affectsBalance()).toBe(false);
    expect(() => loss.ledgerDirectionFor()).toThrow(InvalidWagerTransactionError);
  });
});

describe('WagerTransaction: transições', () => {
  test('PENDING -> PROCESSED / REJECTED / FAILED / PENDING_REFERENCE', () => {
    const a = tx(); a.markProcessed(undefined, at, brl('75')); expect(a.status).toBe(S.Processed);
    expect(a.balanceAfter?.toString()).toBe('75.00');
    const b = tx(); b.reject(FailureCode.InsufficientFunds); expect(b.failureCode).toBe(FailureCode.InsufficientFunds);
    const c = tx(); c.fail(FailureCode.InfrastructureFailure); expect(c.status).toBe(S.Failed);
    const d = tx({ kind: K.Refund, referenceExternalTransactionId: 'x' });
    d.markPendingReference(at); expect(d.status).toBe(S.PendingReference); expect(d.referenceAttempts).toBe(0);
    d.markPendingReference(at); expect(d.referenceAttempts).toBe(1);
    d.markProcessed('ref', at, brl('1')); expect(d.status).toBe(S.Processed);
  });

  test.each(['processed', 'rejected', 'failed'] as const)('estado terminal (%s) não transiciona', (kind) => {
    const t = tx();
    if (kind === 'processed') t.markProcessed(undefined, at, brl('1'));
    if (kind === 'rejected') t.reject(FailureCode.InsufficientFunds);
    if (kind === 'failed') t.fail(FailureCode.InfrastructureFailure);
    expect(t.isTerminal()).toBe(true);
    expect(() => t.markProcessed(undefined, at, brl('1'))).toThrow(InvalidTransactionStateError);
    expect(() => t.markPendingReference(at)).toThrow(InvalidTransactionStateError);
    expect(() => t.reject(FailureCode.AmountMismatch)).toThrow(InvalidTransactionStateError);
    expect(() => t.fail(FailureCode.InfrastructureFailure)).toThrow(InvalidTransactionStateError);
  });
});

describe('Regras de referência (seção 7)', () => {
  const bet = () => processed(tx({ id: 'bet', externalTransactionId: 'bet-ext' }));
  const refund = (over: Partial<CreateWagerTransactionProps> = {}) =>
    tx({ kind: K.Refund, referenceExternalTransactionId: 'bet-ext', ...over });

  test('referência ausente -> wait', () => expect(refund().evaluateReference(undefined)).toEqual({ outcome: 'wait' }));
  test('REFUND de BET processada, valor igual -> ok', () => expect(refund().evaluateReference(bet())).toEqual({ outcome: 'ok' }));
  test('referência ainda pendente -> wait', () => {
    expect(refund().evaluateReference(tx())).toEqual({ outcome: 'wait' });
  });

  test('REFUND só referencia BET', () => {
    const win = processed(tx({ kind: K.Win }));
    expect(refund().evaluateReference(win)).toEqual({ outcome: 'reject', code: FailureCode.InvalidReferenceKind });
  });

  test('ROLLBACK referencia BET, WIN ou REFUND; não LOSS nem ROLLBACK', () => {
    const rb = () => tx({ kind: K.Rollback, referenceExternalTransactionId: 'x' });
    const ok = (k: K, extra: Partial<CreateWagerTransactionProps> = {}) => rb().evaluateReference(processed(tx({ kind: k, ...extra })));
    expect(ok(K.Bet)).toEqual({ outcome: 'ok' });
    expect(ok(K.Win)).toEqual({ outcome: 'ok' });
    expect(ok(K.Refund, { referenceExternalTransactionId: 'y' })).toEqual({ outcome: 'ok' });
    expect(ok(K.Loss)).toEqual({ outcome: 'reject', code: FailureCode.InvalidReferenceKind });
    expect(ok(K.Rollback, { referenceExternalTransactionId: 'y' })).toEqual({ outcome: 'reject', code: FailureCode.InvalidReferenceKind });
  });

  test.each([
    ['playerId', { playerId: 'other' }],
    ['walletId', { walletId: 'other' }],
    ['roundId', { roundId: 'other' }],
    ['providerId', { providerId: 'other' }],
  ] as const)('%s divergente -> REFERENCE_MISMATCH', (_n, over) => {
    expect(refund(over).evaluateReference(bet())).toEqual({ outcome: 'reject', code: FailureCode.ReferenceMismatch });
  });

  test('conflito de moeda -> CURRENCY_MISMATCH', () => {
    const usd = Money.from({ amount: '25.00', currency: 'USD' });
    expect(refund({ money: usd }).evaluateReference(bet())).toEqual({ outcome: 'reject', code: FailureCode.CurrencyMismatch });
  });

  test('valor diferente -> AMOUNT_MISMATCH (reversão parcial fora de escopo)', () => {
    expect(refund({ money: brl('24.99') }).evaluateReference(bet())).toEqual({ outcome: 'reject', code: FailureCode.AmountMismatch });
  });

  test('referência REJECTED/FAILED -> REFERENCE_NOT_PROCESSED', () => {
    const rejected = tx(); rejected.reject(FailureCode.InsufficientFunds);
    expect(refund().evaluateReference(rejected)).toEqual({ outcome: 'reject', code: FailureCode.ReferenceNotProcessed });
  });

  test('WIN com referência opcional: só BET, valor livre', () => {
    const win = tx({ kind: K.Win, referenceExternalTransactionId: 'bet-ext', money: brl('50.00') });
    expect(win.evaluateReference(bet())).toEqual({ outcome: 'ok' });
    expect(win.evaluateReference(undefined)).toEqual({ outcome: 'wait' });
    expect(tx({ kind: K.Win }).evaluateReference(undefined)).toEqual({ outcome: 'ok' });
  });

  test('direção do ledger: ROLLBACK inverte a da referência', () => {
    const rb = tx({ kind: K.Rollback, referenceExternalTransactionId: 'x' });
    expect(tx().ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(rb.ledgerDirectionFor(bet())).toBe(LedgerDirection.Credit);
    expect(rb.ledgerDirectionFor(processed(tx({ kind: K.Win })))).toBe(LedgerDirection.Debit);
    expect(() => rb.ledgerDirectionFor()).toThrow(InvalidWagerTransactionError);
  });
});

describe('payloadHash / idempotência', () => {
  const input = {
    providerId: 'p', externalTransactionId: 'e', playerId: 'pl', walletId: 'w', roundId: 'r', gameId: 'g',
    kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
  };

  test('independe da ordem das chaves e é estável', () => {
    const reordered = { money: { currency: 'BRL', amount: '25.00' }, kind: 'BET', gameId: 'g', roundId: 'r', walletId: 'w', playerId: 'pl', externalTransactionId: 'e', providerId: 'p' };
    expect(computePayloadHash(reordered)).toBe(computePayloadHash(input));
    expect(computePayloadHash(input)).toHaveLength(64);
  });

  test('qualquer campo de negócio diferente muda o hash; matchesPayload detecta conflito', () => {
    const h = computePayloadHash(input);
    expect(computePayloadHash({ ...input, money: { amount: '25.01', currency: 'BRL' } })).not.toBe(h);
    expect(computePayloadHash({ ...input, kind: 'WIN' })).not.toBe(h);
    expect(computePayloadHash({ ...input, referenceExternalTransactionId: 'x' })).not.toBe(h);
    const t = tx({ payloadHash: h });
    expect(t.matchesPayload(h)).toBe(true);
    expect(t.matchesPayload(computePayloadHash({ ...input, roundId: 'r2' }))).toBe(false);
  });
});

test('backoff exponencial com teto', () => {
  expect(referenceBackoffMs(0)).toBe(5_000);
  expect(referenceBackoffMs(3)).toBe(40_000);
  expect(referenceBackoffMs(20)).toBe(300_000);
});
