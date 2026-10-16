import { Migration } from '@mikro-orm/migrations';

/** Fecha a brecha do TRUNCATE em wager_transactions (o trigger de linha não o cobre). */
export class Migration20261016000002_wager_transactions_no_truncate extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE FUNCTION wager_transactions_no_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'wager_transactions cannot be truncated' USING ERRCODE = 'integrity_constraint_violation';
      END $$`);
    this.addSql(`
      CREATE TRIGGER wager_transactions_no_truncate BEFORE TRUNCATE ON wager_transactions
        FOR EACH STATEMENT EXECUTE FUNCTION wager_transactions_no_truncate()`);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TRIGGER IF EXISTS wager_transactions_no_truncate ON wager_transactions`);
    this.addSql(`DROP FUNCTION IF EXISTS wager_transactions_no_truncate()`);
  }
}
