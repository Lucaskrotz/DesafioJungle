/** Lido sob demanda (não no import) para que testes e processos filhos possam sobrescrever o ambiente. */
export function loadConfig() {
  const env = process.env;
  return {
    port: Number(env.PORT ?? 3000),
    databaseUrl: env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager',
    dbPoolMax: Number(env.DB_POOL_MAX ?? 20),
    sqsEndpoint: env.SQS_ENDPOINT ?? 'http://localhost:4566',
    awsRegion: env.AWS_REGION ?? 'us-east-1',
    awsAccessKeyId: env.AWS_ACCESS_KEY_ID ?? 'test',
    awsSecretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? 'test',
    transactionsQueue: env.SQS_TRANSACTIONS_QUEUE ?? 'wager-transactions.fifo',
    transactionsDlq: env.SQS_TRANSACTIONS_DLQ ?? 'wager-transactions-dlq.fifo',
    eventsQueue: env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
    consumerConcurrency: Number(env.CONSUMER_CONCURRENCY ?? 10),
    consumerShutdownTimeoutMs: Number(env.CONSUMER_SHUTDOWN_TIMEOUT_MS ?? 20_000),
    runWorkers: (env.RUN_WORKERS ?? '1') === '1',
    outboxPollMs: Number(env.OUTBOX_POLL_MS ?? 500),
    pendingReferencePollMs: Number(env.PENDING_REFERENCE_POLL_MS ?? 1000),
  };
}
