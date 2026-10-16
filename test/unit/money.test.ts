import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../src/domain/errors';
import { Money } from '../../src/domain/money';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money', () => {
  test('normaliza para escala 2 e serializa como string', () => {
    expect(brl('25').toString()).toBe('25.00');
    expect(brl('25.5').toJSON()).toEqual({ amount: '25.50', currency: 'BRL' });
  });

  test('aritmética decimal exata (sem erro de ponto flutuante)', () => {
    expect(brl('0.10').add(brl('0.20')).toString()).toBe('0.30');
    expect(brl('1000000000000.01').add(brl('0.01')).toString()).toBe('1000000000000.02');
  });

  test('imutável: operações devolvem nova instância', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.toString()).toBe('10.00');
    expect(b.toString()).toBe('15.00');
  });

  test('subtract pode ficar negativo; negate inverte', () => {
    const r = brl('1.00').subtract(brl('3.00'));
    expect(r.isNegative()).toBe(true);
    expect(r.toString()).toBe('-2.00');
    expect(r.negate().toString()).toBe('2.00');
  });

  test('predicados', () => {
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('1.00').isLessThan(brl('1.01'))).toBe(true);
    expect(brl('1.00').equals(brl('1'))).toBe(true);
    expect(brl('1.00').equals(Money.from({ amount: '1.00', currency: 'USD' }))).toBe(false);
  });

  test.each([
    '', ' ', 'NaN', 'Infinity', '-1.00', '+1.00', '1e3', '1E3', '0x10', '1.234', '1.', '.5', '1,00', '1 00',
    '1'.repeat(19),
  ])('rejeita amount inválido %p', (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test('rejeita amount que não é string (number nunca é dinheiro)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each(['', 'br', 'BRLL', 'brl', '123'])('rejeita moeda inválida %p', (currency) => {
    expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
  });

  test('conflito de moeda lança erro de domínio em todas as operações binárias', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => brl('1').add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1').subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1').isLessThan(usd)).toThrow(CurrencyMismatchError);
  });
});
