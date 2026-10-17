import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';

// Prova que as garantias vivem no banco: SQL cru, Postgres real, sem mocks.
// O banco não é limpo entre casos: cada caso usa UUIDs novos.
let orm: Orm;
beforeAll(async () => {
  orm = await createOrm();
});
afterAll(() => orm.close());

const uid = () => crypto.randomUUID();
const run = (sql: string, params: unknown[] = []) => orm.em.fork().execute(sql, params);

async function insert(table: string, row: Record<string, unknown>) {
  const cols = Object.keys(row);
  await run(`insert into ${table} (${cols.join(',')}) values (${cols.map(() => '?').join(',')})`, Object.values(row));
}

type W = { id: string; player_id: string; currency: string };
const wallet = async (o: Record<string, unknown> = {}) => {
  const w = { id: uid(), player_id: uid(), currency: 'BRL', balance: '100.00', ...o };
  await insert('wallets', w);
  return w as W;
};

// Linha de wager_transactions válida (PENDING) com overrides.
const txRow = (w: W, o: Record<string, unknown> = {}) => {
  const id = uid();
  return {
    id, provider_id: 'prov', external_transaction_id: id, idempotency_key: id, payload_hash: 'a'.repeat(64),
    wallet_id: w.id, player_id: w.player_id, round_id: 'r1', game_id: 'g1', kind: 'BET', amount: '10.00',
    currency: w.currency, status: 'PENDING', ...o,
  } as Record<string, any>;
};
const processed = { status: 'PROCESSED', processed_at: new Date().toISOString(), balance_after: '90.00' };
const tx = async (w: W, o: Record<string, unknown> = {}) => {
  const r = txRow(w, o);
  await insert('wager_transactions', r);
  return r;
};

describe('wallets', () => {
  test('balance negativo viola CHECK', async () => {
    await expect(wallet({ balance: '-0.01' })).rejects.toThrow('wallets_balance_check');
  });
  test('UNIQUE (player_id, currency)', async () => {
    const w = await wallet();
    await expect(wallet({ player_id: w.player_id })).rejects.toThrow('wallets_player_currency_uq');
    await wallet({ player_id: w.player_id, currency: 'USD' }); // outra moeda é permitida
  });
  test('currency inválida', async () => {
    await expect(wallet({ currency: 'br1' })).rejects.toThrow('wallets_currency_check');
    await expect(wallet({ currency: 'brl' })).rejects.toThrow('wallets_currency_check');
  });
});

describe('wager_transactions: constraints', () => {
  test('UNIQUE idempotency_key', async () => {
    const w = await wallet();
    const a = await tx(w);
    await expect(tx(w, { idempotency_key: a.idempotency_key })).rejects.toThrow('wt_idempotency_uq');
  });
  test('UNIQUE (provider_id, external_transaction_id)', async () => {
    const w = await wallet();
    const a = await tx(w);
    await expect(tx(w, { external_transaction_id: a.external_transaction_id })).rejects.toThrow('wt_provider_external_uq');
    await tx(w, { provider_id: 'outro', external_transaction_id: a.external_transaction_id });
  });
  test('kind e status fora do enum', async () => {
    const w = await wallet();
    await expect(tx(w, { kind: 'XPTO' })).rejects.toThrow('wager_transactions_kind_check');
    await expect(tx(w, { status: 'XPTO' })).rejects.toThrow('wager_transactions_status_check');
  });
  test('amount <= 0', async () => {
    const w = await wallet();
    await expect(tx(w, { amount: '0' })).rejects.toThrow('wager_transactions_amount_check');
    await expect(tx(w, { amount: '-1' })).rejects.toThrow('wager_transactions_amount_check');
  });
  test('REFUND/ROLLBACK sem referência externa', async () => {
    const w = await wallet();
    await expect(tx(w, { kind: 'REFUND' })).rejects.toThrow('wt_reversal_needs_ref');
    await expect(tx(w, { kind: 'ROLLBACK' })).rejects.toThrow('wt_reversal_needs_ref');
  });
  test('REJECTED sem failure_code (e failure_code fora de REJECTED/FAILED)', async () => {
    const w = await wallet();
    await expect(tx(w, { status: 'REJECTED' })).rejects.toThrow('wt_failure_code_iff_failed');
    await expect(tx(w, { failure_code: 'X' })).rejects.toThrow('wt_failure_code_iff_failed');
    await tx(w, { status: 'REJECTED', failure_code: 'X' });
  });
  test('PROCESSED sem processed_at / sem balance_after', async () => {
    const w = await wallet();
    await expect(tx(w, { ...processed, processed_at: null })).rejects.toThrow('wt_processed_at_iff_processed');
    await expect(tx(w, { ...processed, balance_after: null })).rejects.toThrow('wt_processed_has_balance');
  });
  test('FK composta de moeda (wallet BRL, tx USD)', async () => {
    const w = await wallet();
    await expect(tx(w, { currency: 'USD' })).rejects.toThrow('wt_wallet_currency_fk');
  });
  test('FK composta de player (player diferente do dono)', async () => {
    const w = await wallet();
    await expect(tx(w, { player_id: uid() })).rejects.toThrow('wt_wallet_player_fk');
  });
});

describe('reversão única (wt_single_reversal_uq)', () => {
  const rev = (w: W, bet: Record<string, any>, kind: string, o: Record<string, unknown> = {}) =>
    tx(w, { kind, reference_transaction_id: bet.id, reference_external_transaction_id: bet.external_transaction_id, ...processed, ...o });

  test('dois REFUND PROCESSED para a mesma BET', async () => {
    const w = await wallet();
    const bet = await tx(w, processed);
    await rev(w, bet, 'REFUND');
    await expect(rev(w, bet, 'REFUND')).rejects.toThrow('wt_single_reversal_uq');
  });
  test('REFUND + ROLLBACK da mesma BET', async () => {
    const w = await wallet();
    const bet = await tx(w, processed);
    await rev(w, bet, 'REFUND');
    await expect(rev(w, bet, 'ROLLBACK')).rejects.toThrow('wt_single_reversal_uq');
  });
  test('REJECTED com a mesma referência é permitida', async () => {
    const w = await wallet();
    const bet = await tx(w, processed);
    await rev(w, bet, 'REFUND');
    await rev(w, bet, 'ROLLBACK', { status: 'REJECTED', failure_code: 'ALREADY_REVERSED', processed_at: null, balance_after: null });
  });
});

describe('wager_transactions: triggers', () => {
  test.each([
    ['PROCESSED', processed],
    ['REJECTED', { status: 'REJECTED', failure_code: 'X' }],
    ['FAILED', { status: 'FAILED', failure_code: 'X' }],
  ])('UPDATE de linha %s falha', async (_n, o) => {
    const w = await wallet();
    const t = await tx(w, o);
    await expect(run('update wager_transactions set round_id = ? where id = ?', ['r2', t.id])).rejects.toThrow('terminal status');
    await expect(run('update wager_transactions set reference_attempts = 1 where id = ?', [t.id])).rejects.toThrow('terminal status');
  });
  test('UPDATE de coluna imutável em PENDING_REFERENCE falha', async () => {
    const w = await wallet();
    const t = await tx(w, { status: 'PENDING_REFERENCE', kind: 'REFUND', reference_external_transaction_id: 'nope' });
    const other = await wallet();
    for (const [col, val] of [['amount', '1.00'], ['kind', 'WIN'], ['wallet_id', other.id]] as const) {
      await expect(run(`update wager_transactions set ${col} = ? where id = ?`, [val, t.id])).rejects.toThrow('immutable column changed');
    }
  });
  test('DELETE falha', async () => {
    const w = await wallet();
    const t = await tx(w);
    await expect(run('delete from wager_transactions where id = ?', [t.id])).rejects.toThrow('cannot be deleted');
  });
  test('TRUNCATE falha (inclusive com CASCADE)', async () => {
    // sem CASCADE a FK do ledger já barra; o trigger próprio cobre quem derruba as referências
    await expect(run('truncate wager_transactions cascade')).rejects.toThrow(/cannot be truncated|append-only/);
  });
  test('PENDING_REFERENCE -> PROCESSED funciona', async () => {
    const w = await wallet();
    const t = await tx(w, { status: 'PENDING_REFERENCE', kind: 'REFUND', reference_external_transaction_id: 'x' });
    await run('update wager_transactions set status = ?, processed_at = now(), balance_after = 100 where id = ?', ['PROCESSED', t.id]);
    const [row] = await run('select status from wager_transactions where id = ?', [t.id]);
    expect(row?.status).toBe('PROCESSED');
  });
});

describe('wallet_ledger_entries', () => {
  async function setup() {
    const w = await wallet();
    const t = await tx(w, processed);
    const entry = (o: Record<string, unknown> = {}) => ({
      id: uid(), wallet_id: w.id, transaction_id: t.id, direction: 'DEBIT', amount: '10.00', currency: 'BRL',
      balance_before: '100.00', balance_after: '90.00', ...o,
    });
    return { entry };
  }
  const add = (e: Record<string, unknown>) => insert('wallet_ledger_entries', e);

  test('append-only: UPDATE, DELETE e TRUNCATE falham', async () => {
    const { entry } = await setup();
    const e = entry();
    await add(e);
    await expect(run('update wallet_ledger_entries set amount = 1 where id = ?', [e.id])).rejects.toThrow('append-only');
    await expect(run('delete from wallet_ledger_entries where id = ?', [e.id])).rejects.toThrow('append-only');
    await expect(run('truncate wallet_ledger_entries')).rejects.toThrow('append-only');
    expect(await run('select 1 from wallet_ledger_entries where id = ?', [e.id])).toHaveLength(1);
  });
  test('CHECK aritmético para DEBIT e CREDIT', async () => {
    const { entry } = await setup();
    await expect(add(entry({ direction: 'DEBIT', balance_after: '95.00' }))).rejects.toThrow('ledger_arithmetic');
    await expect(add(entry({ direction: 'DEBIT', balance_after: '110.00' }))).rejects.toThrow('ledger_arithmetic');
    await expect(add(entry({ direction: 'CREDIT', balance_after: '90.00' }))).rejects.toThrow('ledger_arithmetic');
    await add(entry({ direction: 'CREDIT', balance_after: '110.00' }));
  });
  test('amount <= 0 e direction inválida', async () => {
    const { entry } = await setup();
    await expect(add(entry({ amount: '0', balance_after: '100.00' }))).rejects.toThrow('wallet_ledger_entries_amount_check');
    // o Postgres avalia CHECKs em ordem alfabética: ledger_arithmetic dispara antes do direction_check
    await expect(add(entry({ direction: 'X' }))).rejects.toThrow('violates check constraint');
  });
  test('UNIQUE (wallet_id, transaction_id)', async () => {
    const { entry } = await setup();
    await add(entry());
    await expect(add(entry())).rejects.toThrow('ledger_wallet_tx_uq');
  });
  test('FK composta (transaction_id, wallet_id) com transação de outra wallet', async () => {
    const { entry } = await setup();
    const other = await wallet();
    const otherTx = await tx(other, processed);
    await expect(add(entry({ transaction_id: otherTx.id }))).rejects.toThrow('ledger_tx_wallet_fk');
  });
  test('moeda divergente da wallet', async () => {
    const { entry } = await setup();
    await expect(add(entry({ currency: 'USD' }))).rejects.toThrow('ledger_wallet_currency_fk');
  });
});

describe('inbox_messages', () => {
  test('PK (consumer_name, message_id)', async () => {
    const m = uid();
    const row = (consumer: string) => ({ consumer_name: consumer, message_id: m, payload_hash: 'b'.repeat(64) });
    await insert('inbox_messages', row('c1'));
    await expect(insert('inbox_messages', row('c1'))).rejects.toThrow('inbox_messages_pkey');
    await insert('inbox_messages', row('c2'));
  });
});

describe('outbox_messages', () => {
  const ob = () => ({ id: uid(), aggregate_id: uid(), event_type: 'x', payload: '{}', occurred_at: new Date().toISOString() });
  test('published_at pode ser setado e seq cresce', async () => {
    const a = ob();
    const b = ob();
    await insert('outbox_messages', a);
    await insert('outbox_messages', b);
    await run('update outbox_messages set published_at = now() where id = ?', [a.id]);
    const rows = await run('select id, seq::text as seq, published_at from outbox_messages where id in (?, ?)', [a.id, b.id]);
    const byId = Object.fromEntries(rows.map((r: any) => [r.id, r]));
    expect(byId[a.id].published_at).not.toBeNull();
    expect(byId[b.id].published_at).toBeNull();
    expect(Number(byId[b.id].seq)).toBeGreaterThan(Number(byId[a.id].seq));
  });
});

describe('migrations reversíveis', () => {
  const dbName = 'wager_migration_test';
  const base = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager');
  const urlFor = (db: string) => {
    const u = new URL(base);
    u.pathname = `/${db}`;
    return u.toString();
  };
  let admin: Orm;
  let tmp: Orm;
  const tables = async () =>
    (await tmp.em.fork().execute(`select tablename from pg_tables where schemaname = 'public' order by 1`)).map((r: any) => r.tablename);

  beforeAll(async () => {
    admin = await createOrm(urlFor('postgres'));
    await admin.em.fork().execute(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
    await admin.em.fork().execute(`CREATE DATABASE ${dbName}`);
    tmp = await createOrm(urlFor(dbName));
  });
  afterAll(async () => {
    await tmp?.close();
    await admin.em.fork().execute(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
    await admin.close();
  });

  test('up -> 6 tabelas -> down x3 -> só mikro_orm_migrations e sem funções -> up', async () => {
    // 5 tabelas de domínio + a de controle do migrator
    const all = ['inbox_messages', 'outbox_messages', 'wager_transactions', 'wallet_ledger_entries', 'wallets', 'mikro_orm_migrations'].sort();
    const m = tmp.getMigrator();
    await m.up();
    expect(await tables()).toEqual(all);
    // down desfaz uma migration por vez: são três
    await m.down();
    await m.down();
    await m.down();
    expect(await tables()).toEqual(['mikro_orm_migrations']);
    const fns = await tmp.em.fork().execute(
      `select proname from pg_proc where proname in ('ledger_immutable','wager_transactions_guard','wager_transactions_no_truncate')`,
    );
    expect(fns).toHaveLength(0);
    await m.up();
    expect(await tables()).toEqual(all);
  });
});
