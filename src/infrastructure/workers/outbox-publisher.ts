import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { Logger } from '@nestjs/common';
import type { EntityManager } from '@mikro-orm/postgresql';
import { loadConfig } from '../../config';
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
}

// Pendentes e vencidas; SKIP LOCKED reparte o trabalho entre publishers sem lock global.
const CLAIM_SQL = `
  SELECT id, aggregate_id AS "aggregateId", event_type AS "eventType", payload, occurred_at AS "occurredAt",
         attempts, next_attempt_at AS "nextAttemptAt", published_at AS "publishedAt"
    FROM outbox_messages
   WHERE published_at IS NULL AND next_attempt_at <= COALESCE(?::timestamptz, now())
   ORDER BY seq
   LIMIT ?
   FOR UPDATE SKIP LOCKED`;

/**
 * Publica a outbox no SQS (at-least-once).
 * - Commit feito e processo morre antes de publicar: a linha segue pendente e qualquer instância a publica.
 * - Publicou e morreu antes do commit da marcação: o lock cai, a linha volta a pendente e é republicada
 *   com o mesmo eventId (MessageDeduplicationId cobre 5 min no FIFO; o consumidor deduplica por eventId).
 * Troca consciente: o SendMessage roda com a transação (e os locks das linhas) aberta. Aceitável com lote
 * pequeno e lock_timeout; a alternativa sem transação longa é claim por lease (locked_until) + marcação depois.
 */
export class OutboxPublisher {
  private readonly logger = new Logger(OutboxPublisher.name);
  private readonly loop: PollLoop;
  private readonly queueName: string;
  private readonly batchSize: number;

  constructor(
    private readonly orm: Orm,
    private readonly sqs: Sqs,
    opts: OutboxPublisherOptions = {},
  ) {
    this.queueName = opts.queueName ?? loadConfig().eventsQueue;
    this.batchSize = opts.batchSize ?? 25;
    this.loop = new PollLoop(() => this.runOnce(), opts.intervalMs ?? 500, this.logger);
  }

  start(): void {
    this.loop.start();
  }

  stop(): Promise<void> {
    return this.loop.stop();
  }

  /** `now` opcional só para testes; por padrão o vencimento usa o relógio do banco. */
  async runOnce(now?: Date): Promise<{ published: number; failed: number }> {
    return this.orm.em.fork().transactional(async (em: EntityManager) => {
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
          this.logger.warn(`outbox publish failed eventId=${m.id} attempts=${m.attempts}: ${(e as Error).message}`);
          continue;
        }
        m.markPublished(now ?? new Date());
        await this.markPublished(em, m);
        published++;
      }
      return { published, failed };
    });
  }

  /** Idade (s) do evento pendente mais antigo: métrica de lag da outbox. */
  async oldestPendingAgeSeconds(): Promise<number> {
    const [row] = (await this.orm.em
      .fork()
      .execute(`SELECT EXTRACT(EPOCH FROM now() - min(occurred_at)) AS age FROM outbox_messages WHERE published_at IS NULL`)) as {
      age: string | number | null;
    }[];
    return row?.age == null ? 0 : Math.max(0, Number(row.age));
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
