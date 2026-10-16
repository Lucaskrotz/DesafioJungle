import { CurrencyMismatchError, InsufficientFundsError, InvalidMoneyError } from './errors';
import { Money } from './money';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface MovementProps {
  money: Money;
  transactionId: string;
  entryId: string;
  at: Date;
}

/**
 * Aggregate root. Saldo e ledger mudam juntos: debit/credit devolvem o lançamento
 * que o chamador DEVE persistir na mesma transação SQL. A serialização entre
 * instâncias é responsabilidade da infraestrutura (SELECT ... FOR UPDATE na linha).
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: { id: string; playerId: string; initialBalance: Money; at?: Date }): Wallet {
    if (props.initialBalance.isNegative()) throw new InvalidMoneyError('Initial balance cannot be negative');
    const at = props.at ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, at, at);
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(s: WalletState): Wallet {
    return new Wallet(s.id, s.playerId, s.currency, s.balance, s.version, s.createdAt, s.updatedAt);
  }

  get balance(): Money {
    return this._balance;
  }
  get version(): number {
    return this._version;
  }
  get updatedAt(): Date {
    return this._updatedAt;
  }

  /** Lançamento de abertura (saldo 0 → saldo inicial). Não incrementa version: a wallet já nasce em 1. */
  openingEntry(transactionId: string, entryId: string): WalletLedgerEntry | undefined {
    if (!this._balance.isPositive()) return undefined;
    return WalletLedgerEntry.create({
      id: entryId,
      walletId: this.id,
      transactionId,
      direction: LedgerDirection.Credit,
      money: this._balance,
      balanceBefore: Money.zero(this.currency),
      balanceAfter: this._balance,
      createdAt: this.createdAt,
    });
  }

  debit(p: MovementProps): WalletLedgerEntry {
    this.assertSameCurrency(p.money);
    if (this._balance.isLessThan(p.money)) {
      throw new InsufficientFundsError('Insufficient funds');
    }
    return this.apply(LedgerDirection.Debit, p);
  }

  credit(p: MovementProps): WalletLedgerEntry {
    this.assertSameCurrency(p.money);
    return this.apply(LedgerDirection.Credit, p);
  }

  private apply(direction: LedgerDirection, p: MovementProps): WalletLedgerEntry {
    const before = this._balance;
    const after = direction === LedgerDirection.Credit ? before.add(p.money) : before.subtract(p.money);
    // cria (e valida) o lançamento antes de mutar: se falhar, o agregado fica intacto
    const entry = WalletLedgerEntry.create({
      id: p.entryId,
      walletId: this.id,
      transactionId: p.transactionId,
      direction,
      money: p.money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: p.at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = p.at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(`Wallet is ${this.currency}, operation is ${money.currency}`);
    }
  }
}
