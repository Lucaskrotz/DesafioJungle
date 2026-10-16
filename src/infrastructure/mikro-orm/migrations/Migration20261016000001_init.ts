import { Migration } from '@mikro-orm/migrations';

/**
 * Schema inicial. Toda invariante que importa vive aqui, não só no código:
 * não-negatividade, unicidade, imutabilidade (triggers) e coerência wallet/moeda/player (FKs compostas).
 */
export class Migration20261016000001_init extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id          uuid PRIMARY KEY,
        player_id   uuid NOT NULL,
        currency    char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance     numeric(20,2) NOT NULL CHECK (balance >= 0),
        version     integer NOT NULL DEFAULT 1 CHECK (version >= 1),
        created_at  timestamptz NOT NULL DEFAULT now(),
        updated_at  timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT wallets_player_currency_uq UNIQUE (player_id, currency),
        CONSTRAINT wallets_id_currency_uq UNIQUE (id, currency),
        CONSTRAINT wallets_id_player_uq UNIQUE (id, player_id)
      )`);

    this.addSql(`
      CREATE TABLE wager_transactions (
        id                                uuid PRIMARY KEY,
        provider_id                       text NOT NULL,
        external_transaction_id           text NOT NULL,
        idempotency_key                   text NOT NULL,
        payload_hash                      char(64) NOT NULL,
        wallet_id                         uuid NOT NULL,
        player_id                         uuid NOT NULL,
        round_id                          text NOT NULL,
        game_id                           text NOT NULL,
        kind                              text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount                            numeric(20,2) NOT NULL CHECK (amount > 0),
        currency                          char(3) NOT NULL,
        reference_external_transaction_id text,
        reference_transaction_id          uuid REFERENCES wager_transactions(id),
        status                            text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code                      text,
        balance_after                     numeric(20,2) CHECK (balance_after >= 0),
        reference_attempts                integer NOT NULL DEFAULT 0,
        next_reference_attempt_at         timestamptz,
        created_at                        timestamptz NOT NULL DEFAULT now(),
        processed_at                      timestamptz,
        CONSTRAINT wt_idempotency_uq UNIQUE (idempotency_key),
        CONSTRAINT wt_provider_external_uq UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT wt_id_wallet_uq UNIQUE (id, wallet_id),
        CONSTRAINT wt_wallet_currency_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
        CONSTRAINT wt_wallet_player_fk FOREIGN KEY (wallet_id, player_id) REFERENCES wallets (id, player_id),
        CONSTRAINT wt_reversal_needs_ref CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CONSTRAINT wt_failure_code_iff_failed CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CONSTRAINT wt_processed_at_iff_processed CHECK ((status = 'PROCESSED') = (processed_at IS NOT NULL)),
        CONSTRAINT wt_processed_has_balance CHECK (status <> 'PROCESSED' OR balance_after IS NOT NULL)
      )`);

    // uma reversão PROCESSED por referência (REFUND e ROLLBACK da mesma BET creditariam duas vezes)
    this.addSql(`
      CREATE UNIQUE INDEX wt_single_reversal_uq ON wager_transactions (reference_transaction_id)
        WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`);
    this.addSql(`CREATE INDEX wt_pending_reference_idx ON wager_transactions (next_reference_attempt_at) WHERE status = 'PENDING_REFERENCE'`);
    this.addSql(`CREATE INDEX wt_wallet_idx ON wager_transactions (wallet_id, created_at)`);

    this.addSql(`
      CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wager_transactions rows cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'wager_transactions row % is in terminal status %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.id IS DISTINCT FROM OLD.id
          OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
          OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
          OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
          OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
          OR NEW.wallet_id IS DISTINCT FROM OLD.wallet_id
          OR NEW.player_id IS DISTINCT FROM OLD.player_id
          OR NEW.round_id IS DISTINCT FROM OLD.round_id
          OR NEW.game_id IS DISTINCT FROM OLD.game_id
          OR NEW.kind IS DISTINCT FROM OLD.kind
          OR NEW.amount IS DISTINCT FROM OLD.amount
          OR NEW.currency IS DISTINCT FROM OLD.currency
          OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
          OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
          RAISE EXCEPTION 'wager_transactions immutable column changed' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`
      CREATE TRIGGER wager_transactions_guard BEFORE UPDATE OR DELETE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard()`);

    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        seq            bigserial NOT NULL UNIQUE,
        id             uuid PRIMARY KEY,
        wallet_id      uuid NOT NULL,
        transaction_id uuid NOT NULL,
        direction      text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount         numeric(20,2) NOT NULL CHECK (amount > 0),
        currency       char(3) NOT NULL,
        balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
        balance_after  numeric(20,2) NOT NULL CHECK (balance_after >= 0),
        created_at     timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ledger_wallet_tx_uq UNIQUE (wallet_id, transaction_id),
        CONSTRAINT ledger_wallet_currency_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
        CONSTRAINT ledger_tx_wallet_fk FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id),
        CONSTRAINT ledger_arithmetic CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount) OR
          (direction = 'DEBIT'  AND balance_after = balance_before - amount))
      )`);
    this.addSql(`CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq)`);
    this.addSql(`
      CREATE FUNCTION ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is append-only' USING ERRCODE = 'integrity_constraint_violation';
      END $$`);
    this.addSql(`
      CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION ledger_immutable()`);
    this.addSql(`
      CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
        FOR EACH STATEMENT EXECUTE FUNCTION ledger_immutable()`);

    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL,
        message_id    text NOT NULL,
        payload_hash  char(64) NOT NULL,
        received_at   timestamptz NOT NULL DEFAULT now(),
        processed_at  timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      )`);

    this.addSql(`
      CREATE TABLE outbox_messages (
        seq             bigserial NOT NULL UNIQUE,
        id              uuid PRIMARY KEY,
        aggregate_id    uuid NOT NULL,
        event_type      text NOT NULL,
        payload         jsonb NOT NULL,
        occurred_at     timestamptz NOT NULL,
        attempts        integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz NOT NULL DEFAULT now(),
        published_at    timestamptz
      )`);
    this.addSql(`CREATE INDEX outbox_pending_idx ON outbox_messages (next_attempt_at, seq) WHERE published_at IS NULL`);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries`);
    this.addSql(`DROP FUNCTION IF EXISTS ledger_immutable()`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions`);
    this.addSql(`DROP FUNCTION IF EXISTS wager_transactions_guard()`);
    this.addSql(`DROP TABLE IF EXISTS wallets`);
  }
}
