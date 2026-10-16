import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import { createHash } from 'node:crypto';
import { z } from 'zod';
import { IdempotencyConflictError, RetryableConflictError, TransientInfrastructureError } from '../../application/errors';
import { newId } from '../../application/id';
import { noopObservability, type MetricsPort } from '../../application/ports';
import type { SubmitResult, SubmitWagerTransactionUseCase } from '../../application/submit-wager-transaction';
import { canonicalJson } from '../../domain/payload-hash';
import { loadConfig } from '../../config';
import { idempotencyKeySchema, submitSchema } from '../http/schemas';
import { errFields, logger } from '../observability/logger';
import type { Sqs } from './sqs.client';

export const CONSUMER_NAME = 'wager-transactions';

// Envelope da seção 10; `data` = corpo do HTTP + idempotencyKey.
export const wagerMessageSchema = z
  .object({
    messageId: z.string().min(1).max(255),
    type: z.literal('WagerTransactionRequested'),
    occurredAt: z.string().datetime(),
    correlationId: z.string().min(1).max(255).optional(),
    data: submitSchema.extend({ idempotencyKey: idempotencyKeySchema }),
  })
  .strict();
export type WagerMessage = z.infer<typeof wagerMessageSchema>;

export type ErrorClass = 'business' | 'transient' | 'permanent';

const NETWORK_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE']);

/**
 * business: terminal, ack (o resultado já foi decidido; reenviar não muda nada).
 * transient: retry com backoff (não deleta). permanent: DLQ explícita.
 * Default permanente: erro desconhecido não deve girar em retry nem ser engolido.
 */
export function classifyError(e: unknown): ErrorClass {
  if (e instanceof IdempotencyConflictError) return 'business';
  if (e instanceof TransientInfrastructureError || e instanceof RetryableConflictError) return 'transient';
  const code = (e as { code?: unknown } | null)?.code;
  if (typeof code === 'string' && NETWORK_CODES.has(code)) return 'transient';
  return 'permanent';
}

/** Visibilidade (s) após a n-ésima entrega com falha transitória: 2, 4, 8, ... até 60. */
export const retryBackoffSeconds = (receiveCount: number) => Math.min(2 ** (Math.max(1, receiveCount) - 1) * 2, 60);

/** Hash do inbox: JSON canônico da parte de negócio (type + data), sem metadados de transporte. */
export const messageHash = (m: Pick<WagerMessage, 'type' | 'data'>) =>
  createHash('sha256').update(canonicalJson({ type: m.type, data: m.data })).digest('hex');

export interface AckContext {
  sqsMessageId?: string;
  messageId?: string;
  result?: SubmitResult;
}

export interface WagerConsumerOptions {
  queueName?: string;
  dlqName?: string;
  waitTimeSeconds?: number;
  maxMessages?: number;
  /** Grupos (MessageGroupId) processados em paralelo; dentro do grupo, em ordem. */
  concurrency?: number;
  shutdownTimeoutMs?: number;
  /** Gancho de teste entre o commit e o DeleteMessage; se lançar, a mensagem NÃO é deletada. */
  beforeAck?: (ctx: AckContext) => Promise<void>;
  metrics?: MetricsPort;
}

export type Outcome = 'processed' | 'duplicate' | 'business-error' | 'retry' | 'dead-lettered' | 'dlq-failed' | 'released' | 'ack-failed';

type Fields = Record<string, unknown>;

const consumerLogger = logger.child({ component: 'WagerConsumer' });
const log = (level: 'info' | 'warn' | 'error', msg: string, fields: Fields = {}) => consumerLogger[level](fields, msg);

/**
 * Consumidor de wager-transactions.fifo. Correção garantida pelo banco (inbox + idempotência + lock da
 * wallet na mesma transação); ordem por grupo e dedup do broker são só otimização.
 * Ack (DeleteMessage) só depois do commit: morrer entre os dois gera redelivery, que cai no inbox.
 */
export class WagerConsumer {
  /**
   * Contadores locais (testes). Viram métrica: retries -> wager_retries_total{component="sqs_consumer"} e
   * deadLettered -> wager_dlq_messages_total. duplicates/conflicts já são contados pelo caso de uso
   * (wager_duplicates_total{source="sqs"}, wager_idempotency_conflicts_total): repetir aqui contaria em dobro.
   */
  readonly counters = { processed: 0, duplicates: 0, conflicts: 0, retries: 0, deadLettered: 0 };

  private readonly queueName: string;
  private readonly dlqName: string;
  private readonly waitTimeSeconds: number;
  private readonly maxMessages: number;
  private readonly concurrency: number;
  private readonly shutdownTimeoutMs: number;
  private readonly beforeAck?: (ctx: AckContext) => Promise<void>;
  private readonly metrics: MetricsPort;

  private stopping = false;
  private loop?: Promise<void>;
  private readonly cycles = new Set<Promise<unknown>>();
  private readonly inFlight = new Map<string, { queueUrl: string; m: Message }>();

  constructor(
    private readonly submit: SubmitWagerTransactionUseCase,
    private readonly sqs: Sqs,
    opts: WagerConsumerOptions = {},
  ) {
    const cfg = loadConfig();
    this.queueName = opts.queueName ?? cfg.transactionsQueue;
    this.dlqName = opts.dlqName ?? cfg.transactionsDlq;
    this.waitTimeSeconds = Math.min(opts.waitTimeSeconds ?? 5, 5);
    this.maxMessages = opts.maxMessages ?? 10;
    this.concurrency = Math.max(1, opts.concurrency ?? cfg.consumerConcurrency);
    this.shutdownTimeoutMs = opts.shutdownTimeoutMs ?? cfg.consumerShutdownTimeoutMs;
    this.beforeAck = opts.beforeAck;
    this.metrics = opts.metrics ?? noopObservability.metrics;
  }

  start(): void {
    if (this.loop) return;
    this.stopping = false;
    this.loop = (async () => {
      while (!this.stopping) {
        try {
          await this.runOnce();
        } catch (e) {
          log('error', 'receive cycle failed', errFields(e));
          await Bun.sleep(1_000);
        }
      }
    })();
  }

  /** Para o polling, espera os handlers em andamento e devolve a visibilidade do que não terminou. */
  async stop(): Promise<void> {
    this.stopping = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = await Promise.race([
      Promise.allSettled([...this.cycles, this.loop]).then(() => false),
      new Promise<boolean>((r) => (timer = setTimeout(() => r(true), this.shutdownTimeoutMs))),
    ]);
    clearTimeout(timer);
    if (timedOut) {
      // o handler segue rodando; se ainda commitar, a redelivery cai no inbox
      const stuck = [...this.inFlight.values()];
      log('warn', 'shutdown timeout: releasing in-flight messages', { count: stuck.length });
      await Promise.all(stuck.map(({ queueUrl, m }) => this.release(queueUrl, m)));
    }
    this.loop = undefined;
  }

  /** Um ciclo de receive + processamento (determinístico para testes). */
  async runOnce(): Promise<Outcome[]> {
    const p = this.cycle();
    this.cycles.add(p);
    try {
      return await p;
    } finally {
      this.cycles.delete(p);
    }
  }

  private async cycle(): Promise<Outcome[]> {
    if (this.stopping) return [];
    const queueUrl = await this.sqs.queueUrl(this.queueName);
    const { Messages = [] } = await this.sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: this.maxMessages,
        WaitTimeSeconds: this.waitTimeSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }),
    );

    // mesmo grupo em ordem; grupos distintos em paralelo (limitado)
    const groups = new Map<string, Message[]>();
    for (const m of Messages) {
      const g = m.Attributes?.MessageGroupId ?? m.MessageId ?? '';
      groups.set(g, [...(groups.get(g) ?? []), m]);
    }
    const pending = [...groups.values()];
    const outcomes: Outcome[] = [];
    const worker = async () => {
      for (let group = pending.shift(); group; group = pending.shift()) {
        let blocked = false;
        for (const m of group) {
          // após retry no grupo, o resto volta para a fila para não passar à frente
          const outcome = blocked || this.stopping ? await this.release(queueUrl, m) : await this.handle(queueUrl, m);
          if (outcome === 'retry') blocked = true;
          outcomes.push(outcome);
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(this.concurrency, pending.length) }, worker));
    return outcomes;
  }

  private async handle(queueUrl: string, m: Message): Promise<Outcome> {
    const key = m.ReceiptHandle ?? '';
    this.inFlight.set(key, { queueUrl, m });
    try {
      return await this.process(queueUrl, m);
    } finally {
      this.inFlight.delete(key);
    }
  }

  private async process(queueUrl: string, m: Message): Promise<Outcome> {
    const receiveCount = Number(m.Attributes?.ApproximateReceiveCount ?? '1');
    let fields: Fields = { sqsMessageId: m.MessageId, receiveCount };

    let msg: WagerMessage;
    try {
      msg = wagerMessageSchema.parse(JSON.parse(m.Body ?? ''));
    } catch (e) {
      return this.deadLetter(queueUrl, m, e, receiveCount, fields);
    }

    const correlationId = msg.correlationId ?? newId();
    fields = {
      ...fields,
      messageId: msg.messageId,
      causationId: msg.messageId,
      correlationId,
      walletId: msg.data.walletId,
      providerId: msg.data.providerId,
    };

    let result: SubmitResult;
    try {
      result = await this.submit.execute(msg.data, {
        correlationId,
        messageId: msg.messageId,
        inbox: { consumerName: CONSUMER_NAME, messageId: msg.messageId, payloadHash: messageHash(msg) },
      });
    } catch (e) {
      const kind = classifyError(e);
      if (kind === 'transient') return this.retry(queueUrl, m, e, receiveCount, fields);
      if (kind === 'permanent') return this.deadLetter(queueUrl, m, e, receiveCount, fields);
      this.counters.conflicts++;
      log('warn', 'business error, acking', { ...fields, errorName: (e as Error).name });
      return this.ack(queueUrl, m, fields, 'business-error', { sqsMessageId: m.MessageId, messageId: msg.messageId });
    }

    fields = { ...fields, transactionId: result.transactionId, status: result.status, failureCode: result.failureCode };
    const ctx = { sqsMessageId: m.MessageId, messageId: msg.messageId, result };
    if (result.duplicateMessage) {
      this.counters.duplicates++;
      log('info', 'duplicate message, acking', fields);
      return this.ack(queueUrl, m, fields, 'duplicate', ctx);
    }
    this.counters.processed++;
    log('info', 'message processed', fields);
    return this.ack(queueUrl, m, fields, 'processed', ctx);
  }

  private async ack(queueUrl: string, m: Message, fields: Fields, outcome: Outcome, ctx: AckContext): Promise<Outcome> {
    try {
      await this.beforeAck?.(ctx);
    } catch (e) {
      log('warn', 'ack aborted before delete', { ...fields, ...errFields(e) });
      return 'ack-failed';
    }
    if (process.env.FAULT_EXIT_BEFORE_ACK === '1') process.exit(137); // injeção de falha: morre entre commit e ack
    try {
      await this.sqs.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
      return outcome;
    } catch (e) {
      // não é erro de negócio: a redelivery cai no inbox e é acked como duplicata
      log('warn', 'delete failed', { ...fields, ...errFields(e) });
      return 'ack-failed';
    }
  }

  private async retry(queueUrl: string, m: Message, e: unknown, receiveCount: number, fields: Fields): Promise<Outcome> {
    this.counters.retries++;
    this.metrics.retry('sqs_consumer', 'transient');
    const seconds = retryBackoffSeconds(receiveCount);
    log('warn', 'transient failure, retrying', { ...fields, errorName: (e as Error).name, retryInSeconds: seconds });
    try {
      await this.sqs.client.send(
        new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: seconds }),
      );
    } catch (err) {
      log('warn', 'change visibility failed', { ...fields, ...errFields(err) });
    }
    return 'retry'; // após maxReceiveCount a redrive policy leva para a DLQ
  }

  private async deadLetter(queueUrl: string, m: Message, e: unknown, receiveCount: number, fields: Fields): Promise<Outcome> {
    const errorName = e instanceof Error ? e.name : typeof e;
    const reason = (e instanceof Error && e.message ? e.message : errorName).slice(0, 1024);
    try {
      await this.sqs.client.send(
        new SendMessageCommand({
          QueueUrl: await this.sqs.queueUrl(this.dlqName),
          MessageBody: m.Body || '(empty)',
          MessageGroupId: m.Attributes?.MessageGroupId ?? 'unknown',
          MessageDeduplicationId: newId(),
          MessageAttributes: {
            reason: { DataType: 'String', StringValue: reason },
            errorName: { DataType: 'String', StringValue: errorName },
            receiveCount: { DataType: 'Number', StringValue: String(receiveCount) },
            ...(m.MessageId ? { sqsMessageId: { DataType: 'String', StringValue: m.MessageId } } : {}),
          },
        }),
      );
    } catch (err) {
      // sem delete: a redrive policy ainda leva a mensagem para a DLQ
      log('error', 'dead-letter send failed', { ...fields, ...errFields(err), causeErrorName: errorName });
      return 'dlq-failed';
    }
    this.counters.deadLettered++;
    this.metrics.dlqMessage();
    log('error', 'permanent failure, sent to DLQ', { ...fields, errorName });
    try {
      await this.sqs.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
    } catch (err) {
      log('warn', 'delete after DLQ failed', { ...fields, ...errFields(err) });
    }
    return 'dead-lettered';
  }

  private async release(queueUrl: string, m: Message): Promise<Outcome> {
    try {
      await this.sqs.client.send(
        new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0 }),
      );
    } catch (e) {
      log('warn', 'release failed', { sqsMessageId: m.MessageId, ...errFields(e) });
    }
    return 'released';
  }
}
