import type { InboxMessage } from '../domain/inbox-message';
import type { OutboxMessage } from '../domain/outbox-message';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';
import type { WalletLedgerEntry } from '../domain/wallet-ledger-entry';

export interface WalletRepository {
  /** SELECT ... FOR UPDATE: serializa todas as operações da mesma wallet entre instâncias. */
  findByIdForUpdate(id: string): Promise<Wallet | undefined>;
  findById(id: string): Promise<Wallet | undefined>;
  /** Lança WalletAlreadyExistsError se (playerId, currency) já existe. */
  insert(wallet: Wallet): Promise<void>;
  /** UPDATE ... WHERE version = expectedVersion (defesa em profundidade além do lock). */
  updateBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface TransactionRepository {
  findById(id: string): Promise<WagerTransaction | undefined>;
  findByExternal(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  /** Procura por idempotency key OU por (provider, externalId). */
  findForIdempotency(idempotencyKey: string, providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  /** Ordem de lock: wallet primeiro, depois a linha da transação. */
  findByExternalForUpdate(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  findByIdForUpdate(id: string): Promise<WagerTransaction | undefined>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  /** PENDING_REFERENCE vencidas (sem lock): candidatas do worker, que re-trava e revalida cada uma. */
  listDuePendingReferences(limit: number, now: Date): Promise<{ id: string; walletId: string }[]>;
  /** Lança RetryableConflictError em violação de unicidade. */
  insert(tx: WagerTransaction): Promise<void>;
  update(tx: WagerTransaction): Promise<void>;
}

export interface LedgerRepository {
  insert(entry: WalletLedgerEntry): Promise<void>;
  /** Ordem estável por `seq`; `afterSeq` é exclusivo. */
  list(walletId: string, afterSeq: string | undefined, limit: number): Promise<{ entry: WalletLedgerEntry; seq: string }[]>;
  /** Sem lock: leitura de auditoria. undefined se a wallet não existe. */
  reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | undefined>;
}

/** Foto da wallet e do ledger tirada num ÚNICO SELECT (mesmo snapshot). Valores NUMERIC como string. */
export interface ReconciliationSnapshot {
  storedBalance: string;
  currency: string;
  credits: string;
  debits: string;
  entries: number;
  /** Lançamentos cujo balance_before difere do balance_after anterior (o primeiro deve partir de 0). */
  chainBreaks: number;
}

export interface OutboxRepository {
  add(message: OutboxMessage): Promise<void>;
}

export interface InboxRepository {
  /** INSERT ... ON CONFLICT (consumer_name, message_id) DO NOTHING: true se inseriu, false se já existia. */
  tryReceive(msg: InboxMessage): Promise<boolean>;
  find(consumerName: string, messageId: string): Promise<InboxMessage | undefined>;
  markProcessed(consumerName: string, messageId: string, at: Date): Promise<void>;
}

export interface TxRepos {
  wallets: WalletRepository;
  transactions: TransactionRepository;
  ledger: LedgerRepository;
  outbox: OutboxRepository;
  inbox: InboxRepository;
}

export interface UnitOfWork {
  /** Executa `fn` em UMA transação SQL, com retry limitado para deadlock/lock timeout/serialization. */
  run<T>(fn: (repos: TxRepos) => Promise<T>): Promise<T>;
}

// ---- Observabilidade: portas mínimas; os adaptadores (pino, prom-client) ficam na infraestrutura.

export type LogFields = Record<string, unknown>;

/** Logs de negócio levam só identificadores e códigos: nunca valores monetários nem o payload. */
export interface LoggerPort {
  info(fields: LogFields, msg: string): void;
  warn(fields: LogFields, msg: string): void;
  error(fields: LogFields, msg: string): void;
}

export type EntrySource = 'http' | 'sqs';

/** Labels de baixa cardinalidade apenas (nada de walletId/transactionId). */
export interface MetricsPort {
  transactionRecorded(status: string, kind: string, failureCode?: string): void;
  duplicate(source: EntrySource): void;
  idempotencyConflict(): void;
  retry(component: string, reason: string): void;
  lockConflict(code: string): void;
  dlqMessage(): void;
  outboxPublished(n: number): void;
  outboxPublishFailed(n: number): void;
  processingDuration(source: EntrySource, seconds: number): void;
  pendingReference(outcome: 'resolved' | 'rejected' | 'rescheduled'): void;
  reconciliationDivergence(): void;
}

export interface Observability {
  logger: LoggerPort;
  metrics: MetricsPort;
}

const nop = () => {};
export const noopObservability: Observability = {
  logger: { info: nop, warn: nop, error: nop },
  metrics: {
    transactionRecorded: nop,
    duplicate: nop,
    idempotencyConflict: nop,
    retry: nop,
    lockConflict: nop,
    dlqMessage: nop,
    outboxPublished: nop,
    outboxPublishFailed: nop,
    processingDuration: nop,
    pendingReference: nop,
    reconciliationDivergence: nop,
  },
};
