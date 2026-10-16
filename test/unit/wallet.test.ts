import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InsufficientFundsError, InvalidLedgerEntryError } from '../../src/domain/errors';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/wallet-ledger-entry';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-01-01T00:00:00Z');
const open = (initial = '100.00') => Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: brl(initial), at });
const mv = (amount: string, n = 1) => ({ money: brl(amount), transactionId: `t${n}`, entryId: `e${n}`, at });

describe('Wallet', () => {
  test('nasce com version 1 e moeda do saldo inicial', () => {
    const w = open();
    expect(w.version).toBe(1);
    expect(w.currency).toBe('BRL');
    expect(w.balance.toString()).toBe('100.00');
  });

  test('debit gera lançamento coerente e incrementa version', () => {
    const w = open();
    const e = w.debit(mv('30.00'));
    expect(w.balance.toString()).toBe('70.00');
    expect(w.version).toBe(2);
    expect(e.direction).toBe(LedgerDirection.Debit);
    expect(e.balanceBefore.toString()).toBe('100.00');
    expect(e.balanceAfter.toString()).toBe('70.00');
    expect(e.isBalanced()).toBe(true);
  });

  test('credit gera lançamento coerente', () => {
    const w = open();
    const e = w.credit(mv('0.01'));
    expect(w.balance.toString()).toBe('100.01');
    expect(e.direction).toBe(LedgerDirection.Credit);
  });

  test('saldo nunca fica negativo; falha não altera o agregado', () => {
    const w = open('50.00');
    expect(() => w.debit(mv('50.01'))).toThrow(InsufficientFundsError);
    expect(w.balance.toString()).toBe('50.00');
    expect(w.version).toBe(1);
  });

  test('débito do saldo exato zera a wallet', () => {
    const w = open('50.00');
    w.debit(mv('50.00'));
    expect(w.balance.isZero()).toBe(true);
  });

  test('conflito de moeda', () => {
    const w = open();
    const usd = { ...mv('1.00'), money: Money.from({ amount: '1.00', currency: 'USD' }) };
    expect(() => w.debit(usd)).toThrow(CurrencyMismatchError);
    expect(() => w.credit(usd)).toThrow(CurrencyMismatchError);
    expect(w.version).toBe(1);
  });

  test('lançamento de abertura: crédito 0 -> saldo inicial, sem mudar version', () => {
    const w = open('1000.00');
    const e = w.openingEntry('t0', 'e0')!;
    expect(e.balanceBefore.isZero()).toBe(true);
    expect(e.balanceAfter.toString()).toBe('1000.00');
    expect(w.version).toBe(1);
    expect(open('0').openingEntry('t0', 'e0')).toBeUndefined();
  });

  test('saldo == saldo reconstruído pelo ledger após várias operações', () => {
    const w = open('100.00');
    const entries = [w.debit(mv('10.10', 1)), w.credit(mv('3.33', 2)), w.debit(mv('0.01', 3))];
    const rebuilt = entries.reduce(
      (acc, e) => (e.direction === LedgerDirection.Credit ? acc.add(e.money) : acc.subtract(e.money)),
      brl('100.00'),
    );
    expect(rebuilt.equals(w.balance)).toBe(true);
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    id: 'e', walletId: 'w', transactionId: 't', direction: LedgerDirection.Debit,
    money: brl('10.00'), balanceBefore: brl('100.00'), balanceAfter: brl('90.00'), createdAt: at,
  };

  test('create valida a aritmética', () => {
    expect(WalletLedgerEntry.create(base).isBalanced()).toBe(true);
    expect(() => WalletLedgerEntry.create({ ...base, balanceAfter: brl('91.00') })).toThrow(InvalidLedgerEntryError);
    expect(() => WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit })).toThrow(InvalidLedgerEntryError);
  });

  test('rejeita valor zero ou saldo negativo', () => {
    expect(() => WalletLedgerEntry.create({ ...base, money: Money.zero('BRL'), balanceAfter: brl('100.00') })).toThrow(
      InvalidLedgerEntryError,
    );
    expect(() =>
      WalletLedgerEntry.create({ ...base, balanceBefore: brl('5.00'), balanceAfter: brl('5.00').subtract(brl('10.00')) }),
    ).toThrow(InvalidLedgerEntryError);
  });

  test('rehydrate não revalida', () => {
    expect(WalletLedgerEntry.rehydrate({ ...base, balanceAfter: brl('1.00') }).isBalanced()).toBe(false);
  });
});
