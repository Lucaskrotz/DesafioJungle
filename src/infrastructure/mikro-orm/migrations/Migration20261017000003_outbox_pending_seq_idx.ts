import { Migration } from '@mikro-orm/migrations';

/**
 * O claim do publisher filtra pendentes e ordena por seq. Com o índice (next_attempt_at, seq) o planner
 * preferia varrer outbox_messages_seq_key desde o início, pulando todas as linhas já publicadas
 * (~200 ms por claim com 300 mil linhas, crescendo com a tabela). O índice parcial em seq entrega os
 * pendentes já na ordem; também atende a contagem do backlog.
 */
export class Migration20261017000003_outbox_pending_seq_idx extends Migration {
  override async up(): Promise<void> {
    this.addSql(`CREATE INDEX outbox_pending_seq_idx ON outbox_messages (seq) WHERE published_at IS NULL`);
    this.addSql(`DROP INDEX outbox_pending_idx`);
  }

  override async down(): Promise<void> {
    this.addSql(`CREATE INDEX outbox_pending_idx ON outbox_messages (next_attempt_at, seq) WHERE published_at IS NULL`);
    this.addSql(`DROP INDEX outbox_pending_seq_idx`);
  }
}
