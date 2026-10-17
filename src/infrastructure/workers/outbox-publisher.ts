import { SendMessageCommand } from '@aws-sdk/client-sqs';
import type { EntityManager } from '@mikro-orm/postgresql';
import { noopObservability, type MetricsPort } from '../../application/ports';
import { loadConfig } from '../../config';
import { errFields, logger } from '../observability/logger';
import type { OutboxMessage } from '../../domain/outbox-message';
import { outboxFromRow } from '../mikro-orm/mappers';
import type { Orm } from '../mikro-orm/orm.config';
import type { OutboxMessageRow } from '../mikro-orm/schemas';
import type { Sqs } from '../sqs/sqs.client';
import { PollLoop } from './poll';

export interface OutboxPublisherOptions {
  queueName?: string;
  batchSize?: number;
  intervalMs?: number;
  metrics?: MetricsPort;
}

// Pendentes e vencidas; SKIP LOCKED reparte o trabalho entre publishers sem lock global.
// Servida por outbox_pending_seq_idx (parcial em seq): não varre as linhas já publicadas.
export const CLAIM_SQL = `
  SELECT id, aggregate_id AS "aggregateId", event_type AS "eventType", payload, occurred_at AS "occurredAt",
         attempts, next_attempt_at AS "nextAttemptAt", published_at AS "publishedAt"
    FROM outbox_messages
   WHERE published_at IS NULL AND next_attempt_at <= COALESCE(?::timestamptz, now())
   ORDER BY seq
   LIMIT ?
   FOR UPDATE SKIP LOCKED`;

/** Pendentes e idade (s) do mais antigo; usado pelo publisher e pelo scrape de /metrics (mesmo sem workers). */
export async function outboxBacklog(orm: Orm): Promise<{ pending: number; ageSeconds: number }> {
  const [row] = (await orm.em
    .fork()
    .execute(
      `SELECT count(*)::int AS pending, EXTRACT(EPOCH FROM now() - min(occurred_at)) AS age
         FROM outbox_messages WHERE published_at IS NULL`,
    )) as { pending: number; age: string | number | null }[];
  return { pending: Number(row?.pending ?? 0), ageSeconds: row?.age == null ? 0 : Math.max(0, Number(row.age)) };
}

/**
 * Publica a outbox no SQS (at-least-once).
 * - Commit feito e processo morre antes de publicar: a linha segue pendente e qualquer instância a publica.
 * - Publicou e morreu antes do commit da marcação: o lock cai, a linha volta a pendente e é republicada
 *   com o mesmo eventId (MessageDeduplicationId cobre 5 min no FIFO; o consumidor deduplica por eventId).
 * Troca consciente: o SendMessage roda com a transação (e os locks das linhas) aberta. Aceitável com lote
 * pequeno e lock_timeout; a alternativa sem transação longa é claim por lease (locked_until) + marcação depois.
 */
export class OutboxPublisher {
  private readonly logger = logger.child({ component: 'OutboxPublisher' });
  private readonly loop: PollLoop;
  private readonly queueName: string;
  private readonly batchSize: number;
  private readonly metrics: MetricsPort;

  constructor(
    private readonly orm: Orm,
    private readonly sqs: Sqs,
    opts: OutboxPublisherOptions = {},
  ) {
    this.queueName = opts.queueName ?? loadConfig().eventsQueue;
    this.batchSize = opts.batchSize ?? 25;
    this.metrics = opts.metrics ?? noopObservability.metrics;
    // lote cheio publicado = provável backlog: drena sem esperar o intervalo (falhas entram em backoff e não giram)
    this.loop = new PollLoop(async () => (await this.runOnce()).published === this.batchSize, opts.intervalMs ?? 500, this.logger);
  }

  start(): void {
    this.loop.start();
  }

  stop(): Promise<void> {
    return this.loop.stop();
  }

  /** `now` opcional só para testes; por padrão o vencimento usa o relógio do banco. */
  async runOnce(now?: Date): Promise<{ published: number; failed: number }> {
    const result = await this.orm.em.fork().transactional(async (em: EntityManager) => {
      await em.execute(`SET LOCAL lock_timeout = '3s'`);
      const rows = (await em.execute(CLAIM_SQL, [now ?? null, this.batchSize])) as OutboxMessageRow[];
      let published = 0;
      let failed = 0;
      for (const row of rows) {
        // SQL cru via MikroORM devolve timestamptz como string
        const m = outboxFromRow({ ...row, occurredAt: new Date(row.occurredAt), nextAttemptAt: new Date(row.nextAttemptAt) });
        try {
          await this.send(m);
        } catch (e) {
          // falha isolada: reagenda com backoff e segue o lote
          m.scheduleRetry(now ?? new Date());
          await em.execute(`UPDATE outbox_messages SET attempts = ?, next_attempt_at = ? WHERE id = ?`, [
            m.attempts,
            m.nextAttemptAt,
            m.id,
          ]);
          failed++;
          // o envio falhou de fato (com ou sem commit do reagendamento, a linha será tentada de novo)
          this.metrics.outboxPublishFailed(1);
          this.metrics.retry('outbox_publisher', 'publish_failed');
          this.logger.warn({ eventId: m.id, eventType: m.eventType, attempts: m.attempts, ...errFields(e) }, 'outbox publish failed');
          continue;
        }
        m.markPublished(now ?? new Date());
        await this.markPublished(em, m);
        published++;
      }
      return { published, failed };
    });
    // contado após o commit da marcação (rollback => nada foi "publicado" do ponto de vista da outbox)
    if (result.published) this.metrics.outboxPublished(result.published);
    return result;
  }

  /** Idade (s) do evento pendente mais antigo: métrica de lag da outbox. */
  async oldestPendingAgeSeconds(): Promise<number> {
    return (await outboxBacklog(this.orm)).ageSeconds;
  }

  private async send(m: OutboxMessage): Promise<void> {
    await this.sqs.client.send(
      new SendMessageCommand({
        QueueUrl: await this.sqs.queueUrl(this.queueName),
        MessageBody: JSON.stringify(m.payload),
        MessageGroupId: m.aggregateId, // ordem por wallet
        MessageDeduplicationId: m.id, // = eventId
      }),
    );
  }

  protected async markPublished(em: EntityManager, m: OutboxMessage): Promise<void> {
    await em.execute(`UPDATE outbox_messages SET published_at = ? WHERE id = ?`, [m.publishedAt, m.id]);
  }
}
