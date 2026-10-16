import { Inject, Module, type BeforeApplicationShutdown, type OnApplicationBootstrap } from '@nestjs/common';
import type { UnitOfWork } from '../../application/ports';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager-transaction';
import { loadConfig } from '../../config';
import type { Orm } from '../mikro-orm/orm.config';
import type { Sqs } from '../sqs/sqs.client';
import { WagerConsumer } from '../sqs/wager-consumer';
import { ORM, SQS_CLIENT, UNIT_OF_WORK } from '../tokens';
import { OutboxPublisher } from './outbox-publisher';
import { PendingReferenceWorker } from './pending-reference-worker';

@Module({
  providers: [
    {
      provide: OutboxPublisher,
      useFactory: (orm: Orm, sqs: Sqs) => new OutboxPublisher(orm, sqs, { intervalMs: loadConfig().outboxPollMs }),
      inject: [ORM, SQS_CLIENT],
    },
    {
      provide: PendingReferenceWorker,
      useFactory: (uow: UnitOfWork) => new PendingReferenceWorker(uow, { intervalMs: loadConfig().pendingReferencePollMs }),
      inject: [UNIT_OF_WORK],
    },
    {
      provide: WagerConsumer,
      // mesmo caso de uso da entrada HTTP
      useFactory: (uow: UnitOfWork, sqs: Sqs) => new WagerConsumer(new SubmitWagerTransactionUseCase(uow), sqs),
      inject: [UNIT_OF_WORK, SQS_CLIENT],
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
