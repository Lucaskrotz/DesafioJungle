import { createApp } from './bootstrap';
import { loadConfig } from './config';
import { errFields, logger } from './infrastructure/observability/logger';

const config = loadConfig();
const app = await createApp({ workers: config.runWorkers });
await app.listen(config.port);
logger.info({ port: config.port, workers: config.runWorkers }, 'listening');

// Sem enableShutdownHooks: o Nest re-emite o sinal após o close e o processo termina "morto por SIGTERM".
// Aqui o close roda os hooks (consumidor conclui ou devolve mensagens) e o processo sai com 0.
// Um segundo sinal durante o close cai no comportamento padrão (encerra na hora).
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, () => {
    logger.info({ signal }, 'shutting down');
    app.close().then(
      () => process.exit(0),
      (e) => {
        logger.error(errFields(e), 'shutdown failed');
        process.exit(1);
      },
    );
  });
}
