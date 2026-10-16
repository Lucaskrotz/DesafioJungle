/** Erros de aplicação: o adaptador (HTTP/SQS) decide o que fazer com cada um. */
export abstract class ApplicationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Mesma idempotency key (ou mesmo externalTransactionId) com payload/key diferente. */
export class IdempotencyConflictError extends ApplicationError {}

export class WalletAlreadyExistsError extends ApplicationError {}

export class NotFoundError extends ApplicationError {}

export class InvalidQueryError extends ApplicationError {}

/** Falha de infraestrutura que vale a pena reenviar (deadlock, lock timeout, conexão). HTTP 503 / SQS retry. */
export class TransientInfrastructureError extends ApplicationError {
  constructor(message: string, public override readonly cause?: unknown) {
    super(message);
  }
}

/** Corrida benigna (ex.: unique violada por request concorrente): o UnitOfWork repete a transação. */
export class RetryableConflictError extends ApplicationError {}
