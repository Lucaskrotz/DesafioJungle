import { Inject, Module, type BeforeApplicationShutdown, type OnApplicationBootstrap } from '@nestjs/common';
import type { UnitOfWork } from '../../application/ports';
import { loadConfig } from '../../config';
import type { Orm } from '../mikro-orm/orm.config';
import type { Sqs } from '../sqs/sqs.client';
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
  ],
  exports: [OutboxPublisher],
})
export class WorkersModule implements OnApplicationBootstrap, BeforeApplicationShutdown {
  constructor(
    @Inject(OutboxPublisher) private readonly publisher: OutboxPublisher,
    @Inject(PendingReferenceWorker) private readonly references: PendingReferenceWorker,
  ) {}

  onApplicationBootstrap() {
    this.publisher.start();
    this.references.start();
  }

  // antes do onApplicationShutdown: o ORM e o SQSClient fecham lá e o ciclo em andamento ainda precisa deles
  async beforeApplicationShutdown() {
    await Promise.all([this.publisher.stop(), this.references.stop()]);
  }
}
