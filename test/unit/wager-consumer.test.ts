import { describe, expect, test } from 'bun:test';
import { ZodError } from 'zod';
import {
  IdempotencyConflictError,
  InboxPayloadMismatchError,
  RetryableConflictError,
  TransientInfrastructureError,
} from '../../src/application/errors';
import { InboxMessage } from '../../src/domain/inbox-message';
import { InvalidMoneyError, InvalidWagerTransactionError } from '../../src/domain/errors';
import { classifyError, messageHash, retryBackoffSeconds } from '../../src/infrastructure/sqs/wager-consumer';

describe('classifyError', () => {
  test('negócio: conflito de idempotência é terminal (ack)', () => {
    expect(classifyError(new IdempotencyConflictError('x'))).toBe('business');
  });

  test('transitório: infraestrutura, conflito repetível, rede', () => {
    expect(classifyError(new TransientInfrastructureError('db down'))).toBe('transient');
    expect(classifyError(new RetryableConflictError('race'))).toBe('transient');
    expect(classifyError(Object.assign(new Error('reset'), { code: 'ECONNRESET' }))).toBe('transient');
  });

  test('permanente: payload inválido, mensagem inconsistente e desconhecido', () => {
    let syntax: unknown;
    try {
      JSON.parse('{');
    } catch (e) {
      syntax = e;
    }
    for (const e of [
      syntax,
      new ZodError([]),
      new InvalidMoneyError('x'),
      new InvalidWagerTransactionError('Invalid kind: OPENING'),
      new InboxPayloadMismatchError('x'),
      new Error('boom'),
      'string lançada',
      undefined,
    ]) {
      expect(classifyError(e)).toBe('permanent');
    }
  });
});

test('backoff de visibilidade: 2, 4, 8, ... com teto de 60s', () => {
  expect([0, 1, 2, 3, 4, 5, 6, 10].map(retryBackoffSeconds)).toEqual([2, 2, 4, 8, 16, 32, 60, 60]);
});

test('hash da mensagem ignora ordem de chaves e metadados', () => {
  const data = { b: '1', a: { y: 2, x: 1 } } as never;
  const h = messageHash({ type: 'WagerTransactionRequested', data });
  expect(h).toMatch(/^[0-9a-f]{64}$/);
  expect(messageHash({ type: 'WagerTransactionRequested', data: { a: { x: 1, y: 2 }, b: '1' } as never })).toBe(h);
  expect(messageHash({ type: 'WagerTransactionRequested', data: { a: { x: 1, y: 3 }, b: '1' } as never })).not.toBe(h);
});

test('InboxMessage: receive -> markProcessed (idempotente) e rehydrate', () => {
  const m = InboxMessage.receive({ messageId: 'm1', consumerName: 'c', payloadHash: 'h', receivedAt: new Date(0) });
  expect(m.isProcessed()).toBe(false);
  m.markProcessed(new Date(1));
  m.markProcessed(new Date(2));
  expect(m.processedAt).toEqual(new Date(1));
  const r = InboxMessage.rehydrate({ messageId: 'm1', consumerName: 'c', payloadHash: 'h', receivedAt: new Date(0), processedAt: new Date(5) });
  expect(r.isProcessed()).toBe(true);
});
