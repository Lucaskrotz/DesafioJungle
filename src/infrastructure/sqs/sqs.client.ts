import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Global, Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import { loadConfig } from '../../config';
import { SQS_CLIENT } from '../tokens';

export function createSqsClient(cfg: Pick<ReturnType<typeof loadConfig>, 'sqsEndpoint' | 'awsRegion' | 'awsAccessKeyId' | 'awsSecretAccessKey'> = loadConfig()) {
  return new SQSClient({
    endpoint: cfg.sqsEndpoint,
    region: cfg.awsRegion,
    credentials: { accessKeyId: cfg.awsAccessKeyId, secretAccessKey: cfg.awsSecretAccessKey },
  });
}

/** SQSClient + resolução de URL por nome (GetQueueUrl), com cache por processo; falha não fica em cache. */
export class Sqs {
  private readonly urls = new Map<string, Promise<string>>();

  constructor(readonly client: SQSClient = createSqsClient()) {}

  queueUrl(name: string, abortSignal?: AbortSignal): Promise<string> {
    let url = this.urls.get(name);
    if (!url) {
      url = this.client.send(new GetQueueUrlCommand({ QueueName: name }), { abortSignal }).then((r) => {
        if (!r.QueueUrl) throw new Error(`Queue ${name} has no URL`);
        return r.QueueUrl;
      });
      this.urls.set(name, url);
      url.catch(() => this.urls.delete(name));
    }
    return url;
  }
}

@Global()
@Module({
  providers: [{ provide: SQS_CLIENT, useFactory: () => new Sqs() }],
  exports: [SQS_CLIENT],
})
export class SqsModule implements OnApplicationShutdown {
  constructor(@Inject(SQS_CLIENT) private readonly sqs: Sqs) {}

  onApplicationShutdown() {
    this.sqs.client.destroy();
  }
}
