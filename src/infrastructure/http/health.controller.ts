import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import type { Orm } from '../mikro-orm/orm.config';
import { ORM } from '../tokens';

@Controller('health')
export class HealthController {
  constructor(@Inject(ORM) private readonly orm: Orm) {}

  @Get('live')
  live() {
    return { status: 'ok' };
  }

  // SQS será adicionado aos checks numa fase futura.
  @Get('ready')
  async ready() {
    try {
      await this.orm.em.fork().execute('select 1');
    } catch {
      throw new ServiceUnavailableException({ status: 'unavailable', checks: { postgres: 'down' } });
    }
    return { status: 'ok', checks: { postgres: 'up' } };
  }
}
