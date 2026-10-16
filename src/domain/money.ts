import { Decimal } from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './errors';

// Clone isolado: não depende da configuração global do decimal.js.
const D = Decimal.clone({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN });

export interface MoneyProps {
  amount: string;
  currency: string;
}

// Até 18 dígitos inteiros + 2 decimais = NUMERIC(20,2). Sem sinal, expoente ou espaços.
const AMOUNT_RE = /^\d{1,18}(\.\d{1,2})?$/;
const CURRENCY_RE = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  /** Contrato de entrada: string decimal não negativa, no máximo 2 casas. */
  static from(props: MoneyProps): Money {
    const { amount, currency } = props ?? ({} as MoneyProps);
    if (typeof amount !== 'string' || !AMOUNT_RE.test(amount)) {
      throw new InvalidMoneyError(`Invalid amount: expected non-negative decimal string with up to 2 decimals`);
    }
    if (typeof currency !== 'string' || !CURRENCY_RE.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency: expected ISO-4217 code`);
    }
    return new Money(new D(amount), currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0', currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  /** Pode resultar em valor negativo: quem chama decide se isso é aceitável (ex.: Wallet). */
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.gt(0);
  }

  isNegative(): boolean {
    return this.value.lt(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lt(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.eq(other.value);
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  /** Sempre com escala 2. */
  toString(): string {
    return this.value.toFixed(2);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(`Currency mismatch: ${this.currency} vs ${other.currency}`);
    }
  }
}
