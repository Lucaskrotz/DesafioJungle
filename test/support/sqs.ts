import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import type { Sqs } from '../../src/infrastructure/sqs/sqs.client';

export type Queues = { name: string; dlqName: string; url: string; dlqUrl: string; urls: string[] };
const fifo = { FifoQueue: 'true', ContentBasedDeduplication: 'false' };

/** Fila FIFO temporária sem redrive (ex.: eventos). */
export async function tempFifo(sqs: Sqs, prefix: string): Promise<{ name: string; url: string }> {
  const name = `${prefix}-${crypto.randomUUID()}.fifo`;
  const { QueueUrl } = await sqs.client.send(new CreateQueueCommand({ QueueName: name, Attributes: fifo }));
  return { name, url: QueueUrl! };
}

/** Fila FIFO principal + DLQ temporárias, com redrive. */
export async function tempQueues(sqs: Sqs, { maxReceiveCount = 3, visibility = 2 } = {}): Promise<Queues> {
  const suffix = crypto.randomUUID();
  const name = `wt-${suffix}.fifo`;
  const dlqName = `wt-dlq-${suffix}.fifo`;
  const dlq = await sqs.client.send(new CreateQueueCommand({ QueueName: dlqName, Attributes: fifo }));
  const arn = (await sqs.client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] })))
    .Attributes!.QueueArn!;
  const main = await sqs.client.send(
    new CreateQueueCommand({
      QueueName: name,
      Attributes: {
        ...fifo,
        VisibilityTimeout: String(visibility),
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: String(maxReceiveCount) }),
      },
    }),
  );
  return { name, dlqName, url: main.QueueUrl!, dlqUrl: dlq.QueueUrl!, urls: [main.QueueUrl!, dlq.QueueUrl!] };
}

export const deleteQueues = (sqs: Sqs, urls: string[]) =>
  Promise.all(urls.map((QueueUrl) => sqs.client.send(new DeleteQueueCommand({ QueueUrl })).catch(() => undefined)));

/** MessageDeduplicationId sempre novo: o broker não deduplica, quem deduplica é o inbox. */
export const sendMessage = (sqs: Sqs, url: string, body: unknown, groupId: string) =>
  sqs.client.send(
    new SendMessageCommand({
      QueueUrl: url,
      MessageBody: typeof body === 'string' ? body : JSON.stringify(body),
      MessageGroupId: groupId,
      MessageDeduplicationId: crypto.randomUUID(),
    }),
  );

export async function queueCounts(sqs: Sqs, url: string) {
  const a = (
    await sqs.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: url,
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
      }),
    )
  ).Attributes!;
  return { visible: Number(a.ApproximateNumberOfMessages), inFlight: Number(a.ApproximateNumberOfMessagesNotVisible) };
}

/** Lê (sem apagar) até `expected` mensagens ou até o prazo. */
export async function readDlq(sqs: Sqs, url: string, expected: number, timeoutMs = 10_000): Promise<Message[]> {
  const got: Message[] = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < expected && Date.now() < deadline) {
    const r = await sqs.client.send(
      new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30, MessageAttributeNames: ['All'] }),
    );
    got.push(...(r.Messages ?? []));
  }
  return got;
}
