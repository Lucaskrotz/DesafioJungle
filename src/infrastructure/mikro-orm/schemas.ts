import { EntitySchema } from '@mikro-orm/core';

/**
 * Entidades de PERSISTÊNCIA (linhas), separadas do domínio. EntitySchema em vez de decorators:
 * o Bun não emite `design:type`, então metadata por decorator é frágil.
 * Valores monetários são `string` com columnType numeric(20,2): o driver devolve string, nunca number.
 * O schema real (constraints, índices, triggers) é definido pela migration, não por estas classes.
 */
export interface WalletRow {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WagerTransactionRow {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  amount: string;
  currency: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status: string;
  failureCode?: string | null;
  balanceAfter?: string | null;
  referenceAttempts: number;
  nextReferenceAttemptAt?: Date | null;
  createdAt: Date;
  processedAt?: Date | null;
}

export interface LedgerEntryRow {
  seq?: string;
  id: string;
  walletId: string;
  transactionId: string;
  direction: string;
  amount: string;
  currency: string;
  balanceBefore: string;
  balanceAfter: string;
  createdAt: Date;
}

export interface InboxMessageRow {
  consumerName: string;
  messageId: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date | null;
}

export interface OutboxMessageRow {
  seq?: string;
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date;
  publishedAt?: Date | null;
}

const money = { type: 'string', columnType: 'numeric(20,2)' } as const;
const ts = { type: 'Date', columnType: 'timestamptz' } as const;

export const WalletSchema = new EntitySchema<WalletRow>({
  name: 'WalletRow',
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string', columnType: 'char(3)' },
    balance: money,
    version: { type: 'number', columnType: 'integer' },
    createdAt: ts,
    updatedAt: ts,
  },
});

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRow>({
  name: 'WagerTransactionRow',
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string' },
    externalTransactionId: { type: 'string' },
    idempotencyKey: { type: 'string' },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    walletId: { type: 'uuid' },
    playerId: { type: 'uuid' },
    roundId: { type: 'string' },
    gameId: { type: 'string' },
    kind: { type: 'string' },
    amount: money,
    currency: { type: 'string', columnType: 'char(3)' },
    referenceExternalTransactionId: { type: 'string', nullable: true },
    referenceTransactionId: { type: 'uuid', nullable: true },
    status: { type: 'string' },
    failureCode: { type: 'string', nullable: true },
    balanceAfter: { ...money, nullable: true },
    referenceAttempts: { type: 'number', columnType: 'integer' },
    nextReferenceAttemptAt: { ...ts, nullable: true },
    createdAt: ts,
    processedAt: { ...ts, nullable: true },
  },
});

export const LedgerEntrySchema = new EntitySchema<LedgerEntryRow>({
  name: 'LedgerEntryRow',
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    seq: { type: 'string', columnType: 'bigint', nullable: true }, // bigserial: preenchido pelo banco
    walletId: { type: 'uuid' },
    transactionId: { type: 'uuid' },
    direction: { type: 'string' },
    amount: money,
    currency: { type: 'string', columnType: 'char(3)' },
    balanceBefore: money,
    balanceAfter: money,
    createdAt: ts,
  },
});

export const InboxMessageSchema = new EntitySchema<InboxMessageRow>({
  name: 'InboxMessageRow',
  tableName: 'inbox_messages',
  properties: {
    consumerName: { type: 'string', primary: true },
    messageId: { type: 'string', primary: true },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    receivedAt: ts,
    processedAt: { ...ts, nullable: true },
  },
});

export const OutboxMessageSchema = new EntitySchema<OutboxMessageRow>({
  name: 'OutboxMessageRow',
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    seq: { type: 'string', columnType: 'bigint', nullable: true },
    aggregateId: { type: 'uuid' },
    eventType: { type: 'string' },
    payload: { type: 'json', columnType: 'jsonb' },
    occurredAt: ts,
    attempts: { type: 'number', columnType: 'integer' },
    nextAttemptAt: ts,
    publishedAt: { ...ts, nullable: true },
  },
});

export const ALL_SCHEMAS = [
  WalletSchema,
  WagerTransactionSchema,
  LedgerEntrySchema,
  InboxMessageSchema,
  OutboxMessageSchema,
];
