/** Erros de domínio: violação de invariante ou uso incorreto do modelo (não são falhas de infraestrutura). */
export abstract class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidMoneyError extends DomainError {}

export class CurrencyMismatchError extends DomainError {}

/** Débito que deixaria o saldo negativo. O use case decide o failureCode conforme o kind. */
export class InsufficientFundsError extends DomainError {}

/** Transição de estado inválida (ex.: sair de um estado terminal) — erro de programação, não de negócio. */
export class InvalidTransactionStateError extends DomainError {}

export class InvalidWagerTransactionError extends DomainError {}

export class InvalidLedgerEntryError extends DomainError {}
