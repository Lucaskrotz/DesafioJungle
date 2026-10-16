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
}

export interface OutboxRepository {
  add(message: OutboxMessage): Promise<void>;
}

export interface TxRepos {
  wallets: WalletRepository;
  transactions: TransactionRepository;
  ledger: LedgerRepository;
  outbox: OutboxRepository;
}

export interface UnitOfWork {
  /** Executa `fn` em UMA transação SQL, com retry limitado para deadlock/lock timeout/serialization. */
  run<T>(fn: (repos: TxRepos) => Promise<T>): Promise<T>;
}
