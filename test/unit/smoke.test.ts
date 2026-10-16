import { expect, test } from 'bun:test';
import Decimal from 'decimal.js';

test('toolchain ok (bun + decimal.js)', () => {
  expect(new Decimal('0.1').plus('0.2').toFixed(2)).toBe('0.30');
});
