import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { loadConfig } from '../../config';
import type { Orm } from '../mikro-orm/orm.config';
import type { Sqs } from '../sqs/sqs.client';
import { ORM, SQS_CLIENT } from '../tokens';

const up = (p: Promise<unknown>) => p.then(() => 'up' as const, () => 'down' as const);

@Controller('health')
export class HealthController {
  constructor(
    @Inject(ORM) private readonly orm: Orm,
    @Inject(SQS_CLIENT) private readonly sqs: Sqs,
  ) {}

  @Get('live')
  live() {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready() {
    const [postgres, sqs] = await Promise.all([up(this.orm.em.fork().execute('select 1')), up(this.checkSqs())]);
    const checks = { postgres, sqs };
    if (postgres === 'down' || sqs === 'down') throw new ServiceUnavailableException({ status: 'unavailable', checks });
    return { status: 'ok', checks };
  }

  // fila de transações: é dela que a instância depende para consumir
  private async checkSqs() {
    const abortSignal = AbortSignal.timeout(2_000);
    const QueueUrl = await this.sqs.queueUrl(loadConfig().transactionsQueue, abortSignal);
    await this.sqs.client.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }), { abortSignal });
  }
}
