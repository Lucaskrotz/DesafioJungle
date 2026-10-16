/** Lido sob demanda (não no import) para que testes e processos filhos possam sobrescrever o ambiente. */
export function loadConfig() {
  const env = process.env;
  return {
    port: Number(env.PORT ?? 3000),
    databaseUrl: env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager',
    dbPoolMax: Number(env.DB_POOL_MAX ?? 20),
    sqsEndpoint: env.SQS_ENDPOINT ?? 'http://localhost:4566',
    awsRegion: env.AWS_REGION ?? 'us-east-1',
    transactionsQueue: env.SQS_TRANSACTIONS_QUEUE ?? 'wager-transactions.fifo',
    eventsQueue: env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
    runWorkers: (env.RUN_WORKERS ?? '1') === '1',
  };
}
