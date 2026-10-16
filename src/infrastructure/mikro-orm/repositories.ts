import { LockMode } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import { RetryableConflictError, WalletAlreadyExistsError } from '../../application/errors';
import type {
  InboxRepository,
  LedgerRepository,
  OutboxRepository,
  TransactionRepository,
  TxRepos,
  WalletRepository,
} from '../../application/ports';
import type { InboxMessage } from '../../domain/inbox-message';
import type { OutboxMessage } from '../../domain/outbox-message';
import type { WagerTransaction } from '../../domain/wager-transaction';
import type { Wallet } from '../../domain/wallet';
import type { WalletLedgerEntry } from '../../domain/wallet-ledger-entry';
import {
  inboxFromRow,
  ledgerEntryFromRow,
  ledgerEntryToRow,
  outboxToRow,
  transactionFromRow,
  transactionToRow,
  walletFromRow,
  walletToRow,
} from './mappers';
import { InboxMessageSchema, LedgerEntrySchema, OutboxMessageSchema, WagerTransactionSchema, WalletSchema } from './schemas';

// Leituras sempre do banco (o identity map não enxerga as escritas nativas). Mantém a transação corrente.
const FRESH = { disableIdentityMap: true } as const;
const FOR_UPDATE = { ...FRESH, lockMode: LockMode.PESSIMISTIC_WRITE } as const;

/** SQLSTATE do Postgres; o MikroORM copia `code` do erro original do driver. */
export const pgCode = (e: unknown): string | undefined => {
  const code = (e as { code?: unknown } | null)?.code;
  return typeof code === 'string' ? code : undefined;
};
const isUniqueViolation = (e: unknown) => pgCode(e) === '23505';

class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findByIdForUpdate(id: string) {
    const row = await this.em.findOne(WalletSchema, { id }, FOR_UPDATE);
    return row ? walletFromRow(row) : undefined;
  }

  async findById(id: string) {
    const row = await this.em.findOne(WalletSchema, { id }, FRESH);
    return row ? walletFromRow(row) : undefined;
  }

  async insert(wallet: Wallet) {
    try {
      await this.em.insert(WalletSchema, walletToRow(wallet));
    } catch (e) {
      if (isUniqueViolation(e)) throw new WalletAlreadyExistsError('Wallet already exists for this player and currency');
      throw e;
    }
  }

  async updateBalance(wallet: Wallet, expectedVersion: number) {
    const affected = await this.em.nativeUpdate(
      WalletSchema,
      { id: wallet.id, version: expectedVersion },
      { balance: wallet.balance.toString(), version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) {
      throw new Error(`Lost update on wallet ${wallet.id}: expected version ${expectedVersion}`);
    }
  }
}

class MikroOrmTransactionRepository implements TransactionRepository {
  constructor(private readonly em: EntityManager) {}

  private async one(where: object, opts: typeof FRESH | typeof FOR_UPDATE = FRESH) {
    const row = await this.em.findOne(WagerTransactionSchema, where, opts);
    return row ? transactionFromRow(row) : undefined;
  }

  findById(id: string) {
    return this.one({ id });
  }

  findByExternal(providerId: string, externalTransactionId: string) {
    return this.one({ providerId, externalTransactionId });
  }

  async findForIdempotency(idempotencyKey: string, providerId: string, externalTransactionId: string) {
    // até duas linhas (key de uma, externalId de outra): prioriza a que bate a key
    const rows = await this.em.find(
      WagerTransactionSchema,
      { $or: [{ idempotencyKey }, { providerId, externalTransactionId }] },
      FRESH,
    );
    const row = rows.find((r) => r.idempotencyKey === idempotencyKey) ?? rows[0];
    return row ? transactionFromRow(row) : undefined;
  }

  findByExternalForUpdate(providerId: string, externalTransactionId: string) {
    return this.one({ providerId, externalTransactionId }, FOR_UPDATE);
  }

  findByIdForUpdate(id: string) {
    return this.one({ id }, FOR_UPDATE);
  }

  async hasProcessedReversal(referenceTransactionId: string) {
    const n = await this.em.count(WagerTransactionSchema, {
      referenceTransactionId,
      status: 'PROCESSED',
      kind: { $in: ['REFUND', 'ROLLBACK'] },
    });
    return n > 0;
  }

  async listDuePendingReferences(limit: number, now: Date) {
    const rows = await this.em.find(
      WagerTransactionSchema,
      { status: 'PENDING_REFERENCE', nextReferenceAttemptAt: { $lte: now } },
      { ...FRESH, fields: ['id', 'walletId'], orderBy: { nextReferenceAttemptAt: 'asc' }, limit },
    );
    return rows.map((r) => ({ id: r.id, walletId: r.walletId }));
  }

  async insert(tx: WagerTransaction) {
    try {
      await this.em.insert(WagerTransactionSchema, transactionToRow(tx));
    } catch (e) {
      if (isUniqueViolation(e)) throw new RetryableConflictError(`Concurrent insert conflict for transaction ${tx.externalTransactionId}`);
      throw e;
    }
  }

  async update(tx: WagerTransaction) {
    const { id, ...changes } = transactionToRow(tx);
    const affected = await this.em.nativeUpdate(WagerTransactionSchema, { id }, changes);
    if (affected !== 1) throw new Error(`Transaction ${id} not found for update`);
  }
}

class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly em: EntityManager) {}

  async insert(entry: WalletLedgerEntry) {
    await this.em.insert(LedgerEntrySchema, ledgerEntryToRow(entry));
  }

  async list(walletId: string, afterSeq: string | undefined, limit: number) {
    const rows = await this.em.find(
      LedgerEntrySchema,
      afterSeq === undefined ? { walletId } : { walletId, seq: { $gt: afterSeq } },
      { ...FRESH, orderBy: { seq: 'asc' }, limit },
    );
    return rows.map((r) => ({ entry: ledgerEntryFromRow(r), seq: String(r.seq) }));
  }
}

class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  async add(message: OutboxMessage) {
    await this.em.insert(OutboxMessageSchema, outboxToRow(message));
  }
}

class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async tryReceive(m: InboxMessage) {
    // concorrente com a mesma chave espera o commit/rollback do outro e então decide
    const rows = (await this.em.execute(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       VALUES (?, ?, ?, ?, ?) ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING 1`,
      [m.consumerName, m.messageId, m.payloadHash, m.receivedAt, m.processedAt ?? null],
    )) as unknown[];
    return rows.length === 1;
  }

  async find(consumerName: string, messageId: string) {
    const row = await this.em.findOne(InboxMessageSchema, { consumerName, messageId }, FRESH);
    return row ? inboxFromRow(row) : undefined;
  }

  async markProcessed(consumerName: string, messageId: string, at: Date) {
    const affected = await this.em.nativeUpdate(InboxMessageSchema, { consumerName, messageId }, { processedAt: at });
    if (affected !== 1) throw new Error(`Inbox message ${consumerName}/${messageId} not found`);
  }
}

export function buildRepos(em: EntityManager): TxRepos {
  return {
    wallets: new MikroOrmWalletRepository(em),
    transactions: new MikroOrmTransactionRepository(em),
    ledger: new MikroOrmLedgerRepository(em),
    outbox: new MikroOrmOutboxRepository(em),
    inbox: new MikroOrmInboxRepository(em),
  };
}
