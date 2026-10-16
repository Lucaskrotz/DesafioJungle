import { ConnectionException } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import { RetryableConflictError, TransientInfrastructureError } from '../../application/errors';
import type { TxRepos, UnitOfWork } from '../../application/ports';
import type { Orm } from './orm.config';
import { buildRepos, pgCode } from './repositories';

const MAX_ATTEMPTS = 3;
// SQLSTATE repetível -> motivo legível
const RETRYABLE_CODES: Record<string, string> = { '40P01': 'deadlock', '40001': 'serialization_failure', '55P03': 'lock_timeout' };
// conexão recusada/derrubada, banco reiniciando, sem slots
const UNAVAILABLE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', '57P01', '57P03', '53300']);

const isUnavailable = (e: unknown, code: string | undefined) =>
  e instanceof ConnectionException ||
  (e instanceof Error && e.name === 'KnexTimeoutError') || // pool esgotado
  (code !== undefined && (UNAVAILABLE_CODES.has(code) || code.startsWith('08')));

const jitter = () => new Promise((r) => setTimeout(r, 20 + Math.random() * 60));

/** `code` = SQLSTATE quando a causa é conflito de lock; `exhausted` = última tentativa (não haverá retry). */
export interface RetryInfo {
  reason: string;
  code?: string;
  attempt: number;
  exhausted: boolean;
}

export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(
    private readonly orm: Orm,
    /** Chamado a cada conflito repetível, inclusive o que esgota as tentativas. */
    private readonly onRetry?: (info: RetryInfo) => void,
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
        const lockCode = code !== undefined && Object.hasOwn(RETRYABLE_CODES, code) ? code : undefined;
        const reason = e instanceof RetryableConflictError ? 'unique_conflict' : lockCode && RETRYABLE_CODES[lockCode];
        if (!reason) throw e; // domínio/aplicação/constraint de negócio: propaga inalterado
        const exhausted = attempt >= MAX_ATTEMPTS;
        this.onRetry?.({ reason, code: lockCode, attempt, exhausted });
        if (exhausted) throw new TransientInfrastructureError(`Transaction retries exhausted (${reason})`, e);
        await jitter();
      }
    }
  }
}
