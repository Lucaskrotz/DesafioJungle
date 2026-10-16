import { Catch, HttpException, type ArgumentsHost, type ExceptionFilter } from '@nestjs/common';
// Sem @types/express: tipo mínimo do que usamos da resposta.
type Response = { status(code: number): Response; setHeader(k: string, v: string): unknown; json(body: unknown): unknown };
import { ZodError } from 'zod';
import {
  IdempotencyConflictError,
  InvalidQueryError,
  NotFoundError,
  TransientInfrastructureError,
  WalletAlreadyExistsError,
} from '../../application/errors';
import { InvalidMoneyError, InvalidWagerTransactionError } from '../../domain/errors';
import { errFields, logger } from '../observability/logger';

interface ErrorBody {
  error: string;
  message: string;
  details?: unknown;
}

function map(e: unknown): { status: number; body: ErrorBody | object; headers?: Record<string, string> } {
  if (e instanceof ZodError) {
    return { status: 400, body: { error: 'INVALID_PAYLOAD', message: 'Invalid request', details: e.issues } };
  }
  if (e instanceof InvalidMoneyError || e instanceof InvalidWagerTransactionError || e instanceof InvalidQueryError) {
    return { status: 400, body: { error: 'INVALID_PAYLOAD', message: e.message } };
  }
  if (e instanceof IdempotencyConflictError) return { status: 409, body: { error: 'IDEMPOTENCY_CONFLICT', message: e.message } };
  if (e instanceof WalletAlreadyExistsError) return { status: 409, body: { error: 'WALLET_ALREADY_EXISTS', message: e.message } };
  if (e instanceof NotFoundError) return { status: 404, body: { error: 'NOT_FOUND', message: e.message } };
  if (e instanceof TransientInfrastructureError) {
    return { status: 503, body: { error: 'SERVICE_UNAVAILABLE', message: 'Temporary failure, retry' }, headers: { 'Retry-After': '1' } };
  }
  if (e instanceof HttpException) {
    const status = e.getStatus();
    const r = e.getResponse();
    // Corpo próprio (health/ready) é repassado; o resto vira o formato padrão.
    if (status === 503 && typeof r === 'object' && 'checks' in r) return { status, body: r };
    const error = status === 400 ? 'INVALID_PAYLOAD' : status === 404 ? 'NOT_FOUND' : `HTTP_${status}`;
    return { status, body: { error, message: e.message } };
  }
  return { status: 500, body: { error: 'INTERNAL_ERROR', message: 'Internal server error' } };
}

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost) {
    const { status, body, headers } = map(exception);
    const correlationId = host.switchToHttp().getRequest<{ correlationId?: string }>().correlationId;
    if (status >= 500 && status !== 503) logger.error({ correlationId, ...errFields(exception) }, 'unhandled error');
    else if (exception instanceof TransientInfrastructureError) logger.warn({ correlationId, ...errFields(exception) }, 'transient failure');
    const res = host.switchToHttp().getResponse<Response>();
    for (const [k, v] of Object.entries(headers ?? {})) res.setHeader(k, v);
    res.status(status).json(body);
  }
}
