import type { MoneyProps } from './money';
import type { WagerTransaction } from './wager-transaction';
import type { Wallet } from './wallet';
import type { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface EventContext {
  eventId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
}

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export interface IntegrationEventJson<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = props.data;
  }

  /** Envelope serializado gravado no payload da outbox. `data` só carrega tipos JSON (Money como MoneyProps). */
  toJSON(): IntegrationEventJson<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data as T,
    };
  }
}

/** aggregateId = walletId: dá ordenação por wallet (MessageGroupId) no publisher. */
function envelope<T>(ctx: EventContext, walletId: string, data: T): IntegrationEventProps<T> {
  return { ...ctx, aggregateId: walletId, data };
}

interface WagerTransactionData {
  transactionId: string;
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  kind: string;
  money: MoneyProps;
}

function base(tx: WagerTransaction): WagerTransactionData {
  return {
    transactionId: tx.id,
    walletId: tx.walletId,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    kind: tx.kind,
    money: tx.money.toJSON(),
  };
}

export interface WagerTransactionProcessedData extends WagerTransactionData {
  balanceAfter?: MoneyProps;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(envelope(ctx, tx.walletId, { ...base(tx), balanceAfter: tx.balanceAfter?.toJSON() }));
  }
}

export interface WagerTransactionRejectedData extends WagerTransactionData {
  failureCode?: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected(envelope(ctx, tx.walletId, { ...base(tx), failureCode: tx.failureCode }));
  }
}

export interface WagerTransactionPendingReferenceData extends WagerTransactionData {
  referenceExternalTransactionId?: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(
      envelope(ctx, tx.walletId, { ...base(tx), referenceExternalTransactionId: tx.referenceExternalTransactionId }),
    );
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged(
      envelope(ctx, wallet.id, {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      }),
    );
  }
}
