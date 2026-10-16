import { Global, Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import { ORM, UNIT_OF_WORK } from '../tokens';
import { createOrm, type Orm } from './orm.config';
import { MikroOrmUnitOfWork } from './unit-of-work';

@Global()
@Module({
  providers: [
    { provide: ORM, useFactory: () => createOrm() },
    { provide: UNIT_OF_WORK, useFactory: (orm: Orm) => new MikroOrmUnitOfWork(orm), inject: [ORM] },
  ],
  exports: [ORM, UNIT_OF_WORK],
})
export class OrmModule implements OnApplicationShutdown {
  constructor(@Inject(ORM) private readonly orm: Orm) {}

  async onApplicationShutdown() {
    await this.orm.close();
  }
}
