import { Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { OpenWalletUseCase } from './application/open-wallet';
import type { UnitOfWork } from './application/ports';
import { QueryService } from './application/queries';
import { SubmitWagerTransactionUseCase } from './application/submit-wager-transaction';
import { NoopAuthGuard } from './infrastructure/http/auth';
import { AllExceptionsFilter } from './infrastructure/http/filters';
import { HealthController } from './infrastructure/http/health.controller';
import { WageringController } from './infrastructure/http/wagering.controller';
import { WalletsController } from './infrastructure/http/wallets.controller';
import { OrmModule } from './infrastructure/mikro-orm/orm.module';
import { UNIT_OF_WORK } from './infrastructure/tokens';

const useCase = <T>(provide: new (uow: UnitOfWork) => T) => ({
  provide,
  useFactory: (uow: UnitOfWork) => new provide(uow),
  inject: [UNIT_OF_WORK],
});

@Module({
  imports: [OrmModule],
  controllers: [WalletsController, WageringController, HealthController],
  providers: [
    useCase(OpenWalletUseCase),
    useCase(SubmitWagerTransactionUseCase),
    useCase(QueryService),
    { provide: APP_GUARD, useClass: NoopAuthGuard },
    { provide: APP_FILTER, useClass: AllExceptionsFilter },
  ],
})
export class AppModule {}
