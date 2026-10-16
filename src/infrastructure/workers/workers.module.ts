import { Inject, Module, type BeforeApplicationShutdown, type OnApplicationBootstrap } from '@nestjs/common';
import type { LoggerPort, MetricsPort, UnitOfWork } from '../../application/ports';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager-transaction';
import { loadConfig } from '../../config';
import type { Orm } from '../mikro-orm/orm.config';
import type { Sqs } from '../sqs/sqs.client';
import { WagerConsumer } from '../sqs/wager-consumer';
import { LOGGER, METRICS, ORM, SQS_CLIENT, UNIT_OF_WORK } from '../tokens';
import { OutboxPublisher } from './outbox-publisher';
import { PendingReferenceWorker } from './pending-reference-worker';

@Module({
  providers: [
    {
      provide: OutboxPublisher,
      useFactory: (orm: Orm, sqs: Sqs, metrics: MetricsPort) =>
        new OutboxPublisher(orm, sqs, { intervalMs: loadConfig().outboxPollMs, metrics }),
      inject: [ORM, SQS_CLIENT, METRICS],
    },
    {
      provide: PendingReferenceWorker,
      useFactory: (uow: UnitOfWork, metrics: MetricsPort) =>
        new PendingReferenceWorker(uow, { intervalMs: loadConfig().pendingReferencePollMs, metrics }),
      inject: [UNIT_OF_WORK, METRICS],
    },
    {
      provide: WagerConsumer,
      // mesmo caso de uso da entrada HTTP
      useFactory: (uow: UnitOfWork, sqs: Sqs, metrics: MetricsPort, logger: LoggerPort) =>
        new WagerConsumer(new SubmitWagerTransactionUseCase(uow, { metrics, logger }), sqs, { metrics }),
      inject: [UNIT_OF_WORK, SQS_CLIENT, METRICS, LOGGER],
    },
  ],
  exports: [OutboxPublisher, WagerConsumer],
})
export class WorkersModule implements OnApplicationBootstrap, BeforeApplicationShutdown {
  constructor(
    @Inject(OutboxPublisher) private readonly publisher: OutboxPublisher,
    @Inject(PendingReferenceWorker) private readonly references: PendingReferenceWorker,
    @Inject(WagerConsumer) private readonly consumer: WagerConsumer,
  ) {}

  onApplicationBootstrap() {
    this.publisher.start();
    this.references.start();
    this.consumer.start();
  }

  // antes do onApplicationShutdown: o ORM e o SQSClient fecham lá e o ciclo em andamento ainda precisa deles
  async beforeApplicationShutdown() {
    // consumidor primeiro: o que ele commitar ainda gera outbox para o publisher
    await this.consumer.stop();
    await Promise.all([this.publisher.stop(), this.references.stop()]);
  }
}
