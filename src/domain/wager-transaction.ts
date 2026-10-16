import { InvalidTransactionStateError, InvalidWagerTransactionError } from './errors';
import { FailureCode } from './failure-code';
import { Money } from './money';
import { LedgerDirection } from './wallet-ledger-entry';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

const K = WagerTransactionKind;
const S = WagerTransactionStatus;

/**
 * Transições válidas:
 *   PENDING           -> PROCESSED | REJECTED | PENDING_REFERENCE | FAILED
 *   PENDING_REFERENCE -> PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (reagendamento do worker)
 *   PROCESSED, REJECTED, FAILED: terminais.
 */
const TERMINAL = new Set([S.Processed, S.Rejected, S.Failed]);

/** Kinds que podem ser referenciados, por kind da transação que referencia. */
const ALLOWED_REFERENCE_KINDS: Partial<Record<WagerTransactionKind, WagerTransactionKind[]>> = {
  [K.Win]: [K.Bet],
  [K.Loss]: [K.Bet],
  [K.Refund]: [K.Bet],
  [K.Rollback]: [K.Bet, K.Win, K.Refund],
};

export type ReferenceVerdict =
  | { outcome: 'ok' }
  | { outcome: 'wait' }
  | { outcome: 'reject'; code: FailureCode };

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  /** Saldo da wallet após esta transação (base do replay fiel). */
  balanceAfter?: Money;
  referenceAttempts: number;
  nextReferenceAttemptAt?: Date;
}

export type CreateWagerTransactionProps = Omit<
  WagerTransactionState,
  'status' | 'referenceTransactionId' | 'failureCode' | 'processedAt' | 'balanceAfter' | 'referenceAttempts' | 'nextReferenceAttemptAt'
>;

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _balanceAfter?: Money,
    private _referenceAttempts = 0,
    private _nextReferenceAttemptAt?: Date,
  ) {}

  /** Nasce em PENDING. OPENING é interno e só nasce por `opening()`. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === K.Opening) {
      throw new InvalidWagerTransactionError('OPENING is internal and cannot be submitted');
    }
    return WagerTransaction.build(props);
  }

  /** Transação interna de abertura da wallet (crédito do saldo inicial). */
  static opening(props: Omit<CreateWagerTransactionProps, 'kind' | 'providerId' | 'externalTransactionId' | 'idempotencyKey' | 'roundId' | 'gameId' | 'referenceExternalTransactionId'>): WagerTransaction {
    const external = `opening:${props.walletId}`;
    return WagerTransaction.build({
      ...props,
      kind: K.Opening,
      providerId: 'internal',
      externalTransactionId: external,
      idempotencyKey: `internal:${external}`,
      roundId: 'opening',
      gameId: 'opening',
      referenceExternalTransactionId: undefined,
    });
  }

  private static build(props: CreateWagerTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvalidWagerTransactionError('Amount must be greater than zero');
    }
    const needsRef = props.kind === K.Refund || props.kind === K.Rollback;
    if (needsRef && !props.referenceExternalTransactionId) {
      throw new InvalidWagerTransactionError(`${props.kind} requires referenceExternalTransactionId`);
    }
    if (props.kind === K.Bet && props.referenceExternalTransactionId) {
      throw new InvalidWagerTransactionError('BET cannot have a reference');
    }
    return WagerTransaction.rehydrate({ ...props, status: S.Pending, referenceAttempts: 0 });
  }

  /** Reconstrução a partir da persistência — não revalida regras de transição. */
  static rehydrate(s: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      s.id, s.providerId, s.externalTransactionId, s.idempotencyKey, s.payloadHash, s.walletId, s.playerId,
      s.roundId, s.gameId, s.kind, s.money, s.referenceExternalTransactionId, s.createdAt, s.status,
      s.referenceTransactionId, s.failureCode, s.processedAt, s.balanceAfter, s.referenceAttempts,
      s.nextReferenceAttemptAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }
  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get balanceAfter(): Money | undefined {
    return this._balanceAfter;
  }
  get referenceAttempts(): number {
    return this._referenceAttempts;
  }
  get nextReferenceAttemptAt(): Date | undefined {
    return this._nextReferenceAttemptAt;
  }

  // ---- transições
  markProcessed(referenceTransactionId: string | undefined, at: Date, balanceAfter: Money): void {
    this.assertNotTerminal('PROCESSED');
    this._status = S.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._balanceAfter = balanceAfter;
    this._nextReferenceAttemptAt = undefined;
  }

  /** Primeira vez (PENDING) ou reagendamento do worker (já PENDING_REFERENCE). */
  markPendingReference(nextAttemptAt: Date): void {
    this.assertNotTerminal('PENDING_REFERENCE');
    if (this._status === S.PendingReference) this._referenceAttempts += 1;
    this._status = S.PendingReference;
    this._nextReferenceAttemptAt = nextAttemptAt;
  }

  /** `balanceAfter` é opcional: registra o saldo observado no momento da rejeição (replay fiel). */
  reject(code: FailureCode, balanceAfter?: Money): void {
    this.assertNotTerminal('REJECTED');
    this._status = S.Rejected;
    this._failureCode = code;
    this._balanceAfter = balanceAfter;
    this._nextReferenceAttemptAt = undefined;
  }

  fail(code: FailureCode): void {
    this.assertNotTerminal('FAILED');
    this._status = S.Failed;
    this._failureCode = code;
    this._nextReferenceAttemptAt = undefined;
  }

  // ---- consultas
  isTerminal(): boolean {
    return TERMINAL.has(this._status);
  }
  affectsBalance(): boolean {
    return this.kind !== K.Loss;
  }
  requiresReference(): boolean {
    return this.kind === K.Refund || this.kind === K.Rollback;
  }
  /** Referência existe (obrigatória ou opcional informada). */
  hasReference(): boolean {
    return this.referenceExternalTransactionId !== undefined;
  }
  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /** Direção do lançamento. ROLLBACK é o inverso da direção da referência. */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case K.Bet:
        return LedgerDirection.Debit;
      case K.Win:
      case K.Refund:
      case K.Opening:
        return LedgerDirection.Credit;
      case K.Rollback: {
        if (!reference) throw new InvalidWagerTransactionError('ROLLBACK needs its reference to define direction');
        return reference.ledgerDirectionFor() === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;
      }
      case K.Loss:
        throw new InvalidWagerTransactionError('LOSS does not move balance');
    }
  }

  /**
   * Regras 2, 3 e 5 da seção 7. `reference` indefinido = ainda não existe -> wait.
   * Dupla reversão (regra 4) depende de consulta ao banco e fica no use case + índice único.
   */
  evaluateReference(reference: WagerTransaction | undefined): ReferenceVerdict {
    if (!this.hasReference()) return { outcome: 'ok' };
    if (!reference) return { outcome: 'wait' };

    const allowed = ALLOWED_REFERENCE_KINDS[this.kind] ?? [];
    if (!allowed.includes(reference.kind)) return reject(FailureCode.InvalidReferenceKind);
    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.roundId !== this.roundId
    ) {
      return reject(FailureCode.ReferenceMismatch);
    }
    if (reference.money.currency !== this.money.currency) return reject(FailureCode.CurrencyMismatch);

    if (reference.status === S.Pending || reference.status === S.PendingReference) return { outcome: 'wait' };
    if (reference.status !== S.Processed) return reject(FailureCode.ReferenceNotProcessed);

    if (this.requiresReference() && !reference.money.equals(this.money)) return reject(FailureCode.AmountMismatch);
    return { outcome: 'ok' };
  }

  private assertNotTerminal(target: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`Cannot move ${this._status} transaction to ${target}`);
    }
  }
}

function reject(code: FailureCode): ReferenceVerdict {
  return { outcome: 'reject', code };
}

/** Backoff exponencial para o worker de PENDING_REFERENCE: 5s * 2^n, teto 5min. */
export const MAX_REFERENCE_ATTEMPTS = 8;
export function referenceBackoffMs(attempt: number): number {
  return Math.min(5_000 * 2 ** attempt, 300_000);
}
