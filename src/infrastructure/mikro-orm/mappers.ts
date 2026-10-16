import type { FailureCode } from '../../domain/failure-code';
import { Money } from '../../domain/money';
import { OutboxMessage } from '../../domain/outbox-message';
import {
  WagerTransaction,
  type WagerTransactionKind,
  type WagerTransactionStatus,
} from '../../domain/wager-transaction';
import { Wallet } from '../../domain/wallet';
import { type LedgerDirection, WalletLedgerEntry } from '../../domain/wallet-ledger-entry';
import { InboxMessage } from '../../domain/inbox-message';
import type { InboxMessageRow, LedgerEntryRow, OutboxMessageRow, WagerTransactionRow, WalletRow } from './schemas';

/** Mapeamento explícito domínio <-> linha. Dinheiro trafega sempre como string numeric(20,2). */

// char(3) pode vir com padding; numeric vem como string ("25.00")
const money = (amount: string, currency: string) => Money.from({ amount, currency: currency.trim() });
const optMoney = (amount: string | null | undefined, currency: string) =>
  amount == null ? undefined : money(amount, currency);
const opt = <T>(v: T | null | undefined): T | undefined => (v == null ? undefined : v);

export const walletToRow = (w: Wallet): WalletRow => ({
  id: w.id,
  playerId: w.playerId,
  currency: w.currency,
  balance: w.balance.toString(),
  version: w.version,
  createdAt: w.createdAt,
  updatedAt: w.updatedAt,
});

export const walletFromRow = (r: WalletRow): Wallet =>
  Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency.trim(),
    balance: money(r.balance, r.currency),
    version: r.version,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  });

export const transactionToRow = (t: WagerTransaction): WagerTransactionRow => ({
  id: t.id,
  providerId: t.providerId,
  externalTransactionId: t.externalTransactionId,
  idempotencyKey: t.idempotencyKey,
  payloadHash: t.payloadHash,
  walletId: t.walletId,
  playerId: t.playerId,
  roundId: t.roundId,
  gameId: t.gameId,
  kind: t.kind,
  amount: t.money.toString(),
  currency: t.money.currency,
  // null explícito: em UPDATE limpa a coluna
  referenceExternalTransactionId: t.referenceExternalTransactionId ?? null,
  referenceTransactionId: t.referenceTransactionId ?? null,
  status: t.status,
  failureCode: t.failureCode ?? null,
  balanceAfter: t.balanceAfter?.toString() ?? null,
  referenceAttempts: t.referenceAttempts,
  nextReferenceAttemptAt: t.nextReferenceAttemptAt ?? null,
  createdAt: t.createdAt,
  processedAt: t.processedAt ?? null,
});

export const transactionFromRow = (r: WagerTransactionRow): WagerTransaction =>
  WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.providerId,
    externalTransactionId: r.externalTransactionId,
    idempotencyKey: r.idempotencyKey,
    payloadHash: r.payloadHash,
    walletId: r.walletId,
    playerId: r.playerId,
    roundId: r.roundId,
    gameId: r.gameId,
    kind: r.kind as WagerTransactionKind,
    money: money(r.amount, r.currency),
    referenceExternalTransactionId: opt(r.referenceExternalTransactionId),
    createdAt: r.createdAt,
    status: r.status as WagerTransactionStatus,
    referenceTransactionId: opt(r.referenceTransactionId),
    failureCode: opt(r.failureCode) as FailureCode | undefined,
    processedAt: opt(r.processedAt),
    balanceAfter: optMoney(r.balanceAfter, r.currency),
    referenceAttempts: r.referenceAttempts,
    nextReferenceAttemptAt: opt(r.nextReferenceAttemptAt),
  });

/** Sem `seq`: preenchido pelo banco (bigserial). */
export const ledgerEntryToRow = (e: WalletLedgerEntry): Omit<LedgerEntryRow, 'seq'> => ({
  id: e.id,
  walletId: e.walletId,
  transactionId: e.transactionId,
  direction: e.direction,
  amount: e.money.toString(),
  currency: e.money.currency,
  balanceBefore: e.balanceBefore.toString(),
  balanceAfter: e.balanceAfter.toString(),
  createdAt: e.createdAt,
});

export const ledgerEntryFromRow = (r: LedgerEntryRow): WalletLedgerEntry =>
  WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.walletId,
    transactionId: r.transactionId,
    direction: r.direction as LedgerDirection,
    money: money(r.amount, r.currency),
    balanceBefore: money(r.balanceBefore, r.currency),
    balanceAfter: money(r.balanceAfter, r.currency),
    createdAt: r.createdAt,
  });

export const inboxFromRow = (r: InboxMessageRow): InboxMessage =>
  InboxMessage.rehydrate({
    consumerName: r.consumerName,
    messageId: r.messageId,
    payloadHash: r.payloadHash,
    receivedAt: r.receivedAt,
    processedAt: opt(r.processedAt),
  });

export const outboxToRow =(m: OutboxMessage): Omit<OutboxMessageRow, 'seq'> => ({
  id: m.id,
  aggregateId: m.aggregateId,
  eventType: m.eventType,
  payload: { ...m.payload },
  occurredAt: m.occurredAt,
  attempts: m.attempts,
  nextAttemptAt: m.nextAttemptAt ?? m.occurredAt,
  publishedAt: m.publishedAt ?? null,
});

export const outboxFromRow = (r: OutboxMessageRow): OutboxMessage =>
  OutboxMessage.rehydrate({
    id: r.id,
    aggregateId: r.aggregateId,
    eventType: r.eventType,
    payload: r.payload,
    occurredAt: r.occurredAt,
    attempts: r.attempts,
    nextAttemptAt: r.nextAttemptAt,
    publishedAt: opt(r.publishedAt),
  });
