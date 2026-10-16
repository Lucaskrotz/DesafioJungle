import { createParamDecorator, type ExecutionContext } from '@nestjs/common';
import { newId } from '../../application/id';
import { logger } from '../observability/logger';

// Sem @types/express: só o que usamos.
type Req = {
  method: string;
  path: string;
  baseUrl?: string;
  route?: { path?: unknown };
  headers: Record<string, string | string[] | undefined>;
  correlationId?: string;
};
type Res = { statusCode: number; setHeader(k: string, v: string): unknown; on(ev: 'finish', cb: () => void): unknown };

export const CORRELATION_HEADER = 'X-Correlation-Id';
// aceita só um formato seguro e curto; o resto é trocado por um id novo
const VALID_CORRELATION = /^[A-Za-z0-9._:-]{1,128}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Fallback para rotas sem match (404): ids viram placeholder para não espalhar valores nos logs. */
export function normalizePath(path: string): string {
  const out = path
    .split('/')
    .map((s) => (UUID.test(s) ? ':id' : /^\d+$/.test(s) ? ':n' : s))
    .join('/');
  return out.length > 1 && out.endsWith('/') ? out.slice(0, -1) : out || '/';
}

/** Rota com os parâmetros do Express (ex.: /wallets/:walletId) quando houve match; senão o path normalizado. */
export function routeOf(req: Pick<Req, 'path' | 'baseUrl' | 'route'>): string {
  const pattern = req.route?.path;
  return typeof pattern === 'string' ? `${req.baseUrl ?? ''}${pattern}` : normalizePath(req.path);
}

export function resolveCorrelationId(header: string | string[] | undefined): string {
  return typeof header === 'string' && VALID_CORRELATION.test(header) ? header : newId();
}

const QUIET = /^\/(health|metrics)(\/|$)/; // probes e scrape: debug, para não afogar os logs

/**
 * Lê/gera o correlationId, devolve no header e loga UMA linha por requisição (sem body nem headers).
 * O correlationId segue para os casos de uso via @CorrelationId().
 */
export function httpObservability(req: Req, res: Res, next: () => void): void {
  const correlationId = resolveCorrelationId(req.headers['x-correlation-id']);
  req.correlationId = correlationId;
  res.setHeader(CORRELATION_HEADER, correlationId);
  const started = performance.now();
  res.on('finish', () => {
    const fields = {
      correlationId,
      method: req.method,
      route: routeOf(req),
      status: res.statusCode,
      durationMs: Math.round(performance.now() - started),
    };
    if (QUIET.test(req.path)) logger.debug(fields, 'http request');
    else logger.info(fields, 'http request');
  });
  next();
}

export const CorrelationId = createParamDecorator(
  (_: unknown, ctx: ExecutionContext): string => ctx.switchToHttp().getRequest<Req>().correlationId ?? newId(),
);
