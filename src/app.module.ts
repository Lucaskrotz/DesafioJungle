import { Module, type DynamicModule } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { OpenWalletUseCase } from './application/open-wallet';
import type { LoggerPort, MetricsPort, Observability, UnitOfWork } from './application/ports';
import { QueryService } from './application/queries';
import { ReconcileWalletUseCase } from './application/reconcile-wallet';
import { SubmitWagerTransactionUseCase } from './application/submit-wager-transaction';
import { NoopAuthGuard } from './infrastructure/http/auth';
import { AllExceptionsFilter } from './infrastructure/http/filters';
import { HealthController } from './infrastructure/http/health.controller';
import { WageringController } from './infrastructure/http/wagering.controller';
import { WalletsController } from './infrastructure/http/wallets.controller';
import { OrmModule } from './infrastructure/mikro-orm/orm.module';
import { ObservabilityModule } from './infrastructure/observability/metrics';
import { SqsModule } from './infrastructure/sqs/sqs.client';
import { LOGGER, METRICS, UNIT_OF_WORK } from './infrastructure/tokens';
import { WorkersModule } from './infrastructure/workers/workers.module';

const useCase = <T>(provide: new (uow: UnitOfWork, obs: Observability) => T) => ({
  provide,
  useFactory: (uow: UnitOfWork, metrics: MetricsPort, logger: LoggerPort) => new provide(uow, { metrics, logger }),
  inject: [UNIT_OF_WORK, METRICS, LOGGER],
});

@Module({
  imports: [OrmModule, SqsModule, ObservabilityModule],
  controllers: [WalletsController, WageringController, HealthController],
  providers: [
    useCase(OpenWalletUseCase),
    useCase(SubmitWagerTransactionUseCase),
    useCase(ReconcileWalletUseCase),
    useCase(QueryService),
    // guard global no-op: /health/* e /metrics ficam abertos; um guard real deve isentar essas rotas
    { provide: APP_GUARD, useClass: NoopAuthGuard },
    { provide: APP_FILTER, useClass: AllExceptionsFilter },
  ],
})
export class AppModule {
  /** Workers (outbox, PENDING_REFERENCE) só entram quando pedidos: testes sobem a API sem eles. */
  static register(opts: { workers: boolean }): DynamicModule {
    return { module: AppModule, imports: opts.workers ? [WorkersModule] : [] };
  }
}
