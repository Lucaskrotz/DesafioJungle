import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { Global, Module } from '@nestjs/common';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';
import type { EntrySource, MetricsPort } from '../../application/ports';
import { loadConfig } from '../../config';
import { MetricsController } from '../http/metrics.controller';
import type { Orm } from '../mikro-orm/orm.config';
import type { Sqs } from '../sqs/sqs.client';
import { LOGGER, METRICS, ORM, SQS_CLIENT } from '../tokens';
import { outboxBacklog } from '../workers/outbox-publisher';
import { logger } from './logger';

/** Adaptador prom-client da MetricsPort. Registry próprio por instância (vários apps no mesmo processo de teste). */
export class PromMetrics implements MetricsPort {
  private readonly transactions: Counter<'status' | 'kind'>;
  private readonly rejected: Counter<'failure_code'>;
  private readonly duplicates: Counter<'source'>;
  private readonly conflicts: Counter;
  private readonly retries: Counter<'component' | 'reason'>;
  private readonly lockConflicts: Counter<'code'>;
  private readonly dlq: Counter;
  private readonly published: Counter;
  private readonly publishFailures: Counter;
  private readonly duration: Histogram<'source'>;
  private readonly pendingRefs: Counter<'outcome'>;
  private readonly divergences: Counter;

  constructor(readonly registry: Registry = new Registry()) {
    const registers = [registry];
    const counter = <L extends string = never>(name: string, help: string, labelNames: L[] = []) =>
      new Counter<L>({ name, help, labelNames, registers });
    this.transactions = counter('wager_transactions_total', 'Resultados gravados por status e kind', ['status', 'kind']);
    this.rejected = counter('wager_transactions_rejected_total', 'Rejeições por failure code', ['failure_code']);
    this.duplicates = counter('wager_duplicates_total', 'Replays de idempotência e duplicatas de inbox', ['source']);
    this.conflicts = counter('wager_idempotency_conflicts_total', 'Mesma key/externalId com payload diferente');
    this.retries = counter('wager_retries_total', 'Retries por componente e motivo', ['component', 'reason']);
    this.lockConflicts = counter('wager_lock_conflicts_total', 'Conflitos de lock no Postgres por SQLSTATE', ['code']);
    this.dlq = counter('wager_dlq_messages_total', 'Mensagens enviadas à DLQ pelo consumidor');
    this.published = counter('wager_outbox_published_total', 'Eventos da outbox publicados');
    this.publishFailures = counter('wager_outbox_publish_failures_total', 'Falhas de publicação da outbox');
    this.pendingRefs = counter('wager_pending_reference_total', 'Reprocessamentos de PENDING_REFERENCE por desfecho', ['outcome']);
    this.divergences = counter('wager_reconciliation_divergences_total', 'Reconciliações com divergência');
    this.duration = new Histogram({
      name: 'wager_processing_duration_seconds',
      help: 'Duração do processamento de uma transação, do início ao commit',
      labelNames: ['source'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers,
    });
  }

  transactionRecorded(status: string, kind: string, failureCode?: string) {
    this.transactions.inc({ status, kind });
    if (failureCode) this.rejected.inc({ failure_code: failureCode });
  }
  duplicate(source: EntrySource) {
    this.duplicates.inc({ source });
  }
  idempotencyConflict() {
    this.conflicts.inc();
  }
  retry(component: string, reason: string) {
    this.retries.inc({ component, reason });
  }
  lockConflict(code: string) {
    this.lockConflicts.inc({ code });
  }
  dlqMessage() {
    this.dlq.inc();
  }
  outboxPublished(n: number) {
    this.published.inc(n);
  }
  outboxPublishFailed(n: number) {
    this.publishFailures.inc(n);
  }
  processingDuration(source: EntrySource, seconds: number) {
    this.duration.observe({ source }, seconds);
  }
  pendingReference(outcome: 'resolved' | 'rejected' | 'rescheduled') {
    this.pendingRefs.inc({ outcome });
  }
  reconciliationDivergence() {
    this.divergences.inc();
  }
}

const PROBE_TIMEOUT_MS = 1_500;

/** Sonda do scrape: timeout curto e falha silenciosa (o gauge mantém o último valor; /metrics nunca cai). */
const probe = <T>(fn: (signal: AbortSignal) => Promise<T>): Promise<T | undefined> =>
  Promise.race([fn(AbortSignal.timeout(PROBE_TIMEOUT_MS)), Bun.sleep(PROBE_TIMEOUT_MS).then(() => undefined)]).catch(() => undefined);

/** Gauges lidos no scrape: backlog da outbox (funciona sem workers) e profundidade da DLQ. */
export function registerProbeGauges(registry: Registry, orm: Orm, sqs: Sqs, dlqName = loadConfig().transactionsDlq) {
  // os dois gauges da outbox coletam em paralelo: compartilham a mesma consulta
  let inflight: Promise<{ pending: number; ageSeconds: number } | undefined> | undefined;
  const backlog = () => (inflight ??= probe(() => outboxBacklog(orm)).finally(() => (inflight = undefined)));

  new Gauge({
    name: 'wager_outbox_pending',
    help: 'Eventos da outbox ainda não publicados',
    registers: [registry],
    async collect() {
      const b = await backlog();
      if (b) this.set(b.pending);
    },
  });
  new Gauge({
    name: 'wager_outbox_lag_seconds',
    help: 'Idade do evento pendente mais antigo da outbox',
    registers: [registry],
    async collect() {
      const b = await backlog();
      if (b) this.set(b.ageSeconds);
    },
  });
  new Gauge({
    name: 'wager_dlq_depth',
    help: 'ApproximateNumberOfMessages da DLQ de transações',
    registers: [registry],
    async collect() {
      const n = await probe(async (abortSignal) => {
        const QueueUrl = await sqs.queueUrl(dlqName, abortSignal);
        const r = await sqs.client.send(
          new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['ApproximateNumberOfMessages'] }),
          { abortSignal },
        );
        return Number(r.Attributes?.ApproximateNumberOfMessages ?? 0);
      });
      if (n !== undefined) this.set(n);
    },
  });
}

export function createMetrics(orm: Orm, sqs: Sqs): PromMetrics {
  const metrics = new PromMetrics();
  collectDefaultMetrics({ register: metrics.registry });
  registerProbeGauges(metrics.registry, orm, sqs);
  return metrics;
}

/** Logger (pino) e métricas (prom-client) para toda a aplicação. */
@Global()
@Module({
  controllers: [MetricsController],
  providers: [
    { provide: LOGGER, useValue: logger },
    { provide: METRICS, useFactory: createMetrics, inject: [ORM, SQS_CLIENT] },
  ],
  exports: [LOGGER, METRICS],
})
export class ObservabilityModule {}
