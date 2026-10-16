import { expect } from 'bun:test';
import Decimal from 'decimal.js';
import type { Orm } from '../../src/infrastructure/mikro-orm/orm.config';

const q = (orm: Orm, sql: string, params: unknown[] = []) =>
  orm.em.getConnection().execute(sql, params) as Promise<any[]>;

/** Verifica as invariantes do ledger de uma wallet com SQL cru; NUMERIC chega como string (Decimal). */
export async function assertWalletConsistent(orm: Orm, walletId: string) {
  const [w] = await q(orm, `SELECT balance::text AS balance, version FROM wallets WHERE id = ?`, [walletId]);
  expect(w).toBeDefined();
  const balance = new Decimal(w.balance);
  expect(balance.gte(0)).toBe(true);

  const rows = await q(
    orm,
    `SELECT l.transaction_id, l.direction, l.amount::text AS amount, l.balance_before::text AS bb,
            l.balance_after::text AS ba, t.kind
       FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id
      WHERE l.wallet_id = ? ORDER BY l.seq`,
    [walletId],
  );

  // um lançamento por transação
  expect(new Set(rows.map((r) => r.transaction_id)).size).toBe(rows.length);

  // cadeia contínua a partir de 0 e saldo == créditos - débitos
  let prev = new Decimal(0);
  let sum = new Decimal(0);
  let debits = 0;
  let credits = 0;
  for (const r of rows) {
    expect(new Decimal(r.bb).eq(prev)).toBe(true);
    const amt = new Decimal(r.amount);
    if (r.direction === 'CREDIT') {
      sum = sum.plus(amt);
      credits++;
    } else {
      sum = sum.minus(amt);
      debits++;
    }
    prev = new Decimal(r.ba);
    expect(prev.eq(sum)).toBe(true);
  }
  expect(sum.eq(balance)).toBe(true);
  if (rows.length) expect(prev.eq(balance)).toBe(true);

  // version = 1 + lançamentos após a OPENING
  const opening = rows.filter((r) => r.kind === 'OPENING').length;
  expect(w.version).toBe(1 + rows.length - opening);

  return { balance, entries: rows.length, debits, credits };
}
