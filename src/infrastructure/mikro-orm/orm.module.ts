import { Global, Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import type { LoggerPort, MetricsPort } from '../../application/ports';
import { LOGGER, METRICS, ORM, UNIT_OF_WORK } from '../tokens';
import { createOrm, type Orm } from './orm.config';
import { MikroOrmUnitOfWork, type RetryInfo } from './unit-of-work';

/** Conflito repetível => métrica de lock (por SQLSTATE) e de retry (se ainda houver tentativa). */
const retryObserver =
  (metrics: MetricsPort, logger: LoggerPort) =>
  ({ reason, code, attempt, exhausted }: RetryInfo) => {
    if (code) metrics.lockConflict(code);
    if (!exhausted) metrics.retry('unit_of_work', reason);
    logger.warn({ reason, code, attempt, exhausted }, exhausted ? 'transaction retries exhausted' : 'transaction retry');
  };

@Global()
@Module({
  providers: [
    { provide: ORM, useFactory: () => createOrm() },
    {
      provide: UNIT_OF_WORK,
      useFactory: (orm: Orm, metrics: MetricsPort, logger: LoggerPort) => new MikroOrmUnitOfWork(orm, retryObserver(metrics, logger)),
      inject: [ORM, METRICS, LOGGER],
    },
  ],
  exports: [ORM, UNIT_OF_WORK],
})
export class OrmModule implements OnApplicationShutdown {
  constructor(@Inject(ORM) private readonly orm: Orm) {}

  async onApplicationShutdown() {
    await this.orm.close();
  }
}
