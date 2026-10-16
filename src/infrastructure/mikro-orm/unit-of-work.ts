import { ConnectionException } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import { RetryableConflictError, TransientInfrastructureError } from '../../application/errors';
import type { TxRepos, UnitOfWork } from '../../application/ports';
import type { Orm } from './orm.config';
import { buildRepos, pgCode } from './repositories';

const MAX_ATTEMPTS = 3;
// deadlock, serialization failure, lock timeout
const RETRYABLE_CODES = new Set(['40P01', '40001', '55P03']);
// conexão recusada/derrubada, banco reiniciando, sem slots
const UNAVAILABLE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', '57P01', '57P03', '53300']);

const isUnavailable = (e: unknown, code: string | undefined) =>
  e instanceof ConnectionException ||
  (e instanceof Error && e.name === 'KnexTimeoutError') || // pool esgotado
  (code !== undefined && (UNAVAILABLE_CODES.has(code) || code.startsWith('08')));

const jitter = () => new Promise((r) => setTimeout(r, 20 + Math.random() * 60));

export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(
    private readonly orm: Orm,
    private readonly onRetry?: (reason: string) => void,
  ) {}

  async run<T>(fn: (repos: TxRepos) => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.orm.em.fork().transactional(async (em: EntityManager) => {
          await em.execute(`SET LOCAL lock_timeout = '3s'`);
          return fn(buildRepos(em));
        });
      } catch (e) {
        const code = pgCode(e);
        if (isUnavailable(e, code)) throw new TransientInfrastructureError('Database unavailable', e);
        const reason = e instanceof RetryableConflictError ? 'conflict' : code && RETRYABLE_CODES.has(code) ? code : undefined;
        if (!reason) throw e; // domínio/aplicação/constraint de negócio: propaga inalterado
        if (attempt >= MAX_ATTEMPTS) throw new TransientInfrastructureError(`Transaction retries exhausted (${reason})`, e);
        this.onRetry?.(reason);
        await jitter();
      }
    }
  }
}
