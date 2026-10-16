/** Taxonomia estável de rejeições/falhas. Os valores são contrato com os provedores: não renomear. */
export enum FailureCode {
  /** BET sem saldo. Reenviar só faz sentido após novo crédito. */
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  /** REFUND/ROLLBACK que deixaria o saldo negativo. */
  ReversalWouldOverdraw = 'REVERSAL_WOULD_OVERDRAW',
  /** Referência não apareceu dentro do limite de tentativas. */
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  /** Referência existe mas pertence a outro player/wallet/rodada. */
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  /** Referência terminou REJECTED/FAILED: nada a reverter. */
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  AmountMismatch = 'AMOUNT_MISMATCH',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  /** Tipo de referência não permitido para este kind (ex.: REFUND de WIN). */
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',
  WalletNotFound = 'WALLET_NOT_FOUND',
  /** Erro permanente de infraestrutura (estado FAILED). */
  InfrastructureFailure = 'INFRASTRUCTURE_FAILURE',
}
