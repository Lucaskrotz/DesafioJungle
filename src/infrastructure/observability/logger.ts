import type { LoggerService } from '@nestjs/common';
import pino, { type DestinationStream, type Logger } from 'pino';

// Chaves com valor financeiro ou credencial: censuradas no topo e um nível abaixo (ex.: data.money).
// Logs de negócio já não passam valores; isto é a rede de segurança.
const SENSITIVE = [
  'money',
  'amount',
  'balance',
  'balanceBefore',
  'balanceAfter',
  'initialBalance',
  'storedBalance',
  'calculatedBalance',
  'payload',
  'body',
  'authorization',
];
export const REDACT_PATHS = [...SENSITIVE, ...SENSITIVE.map((k) => `*.${k}`), 'req.headers.authorization', 'headers.authorization'];

export function createLogger(destination: DestinationStream, level = process.env.LOG_LEVEL ?? 'info'): Logger {
  return pino(
    {
      level,
      base: { service: 'wager-processor', pid: process.pid },
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: { level: (label) => ({ level: label }) },
      redact: { paths: REDACT_PATHS, censor: '[REDACTED]' },
    },
    destination,
  );
}

// stdout síncrono: a linha sai antes de um process.exit (ex.: injeção de falha nos testes de crash)
const stdout = pino.destination({ dest: 1, sync: true });
let target: DestinationStream = stdout;

/** Instância única do processo. O destino é trocável só para testes capturarem as linhas. */
export const logger = createLogger({ write: (line: string) => target.write(line) });

export function setLogDestination(d?: DestinationStream): void {
  target = d ?? stdout;
}

/**
 * Campos de erro seguros: erros do driver (com SQLSTATE) carregam o SQL com parâmetros na mensagem,
 * então deles vão só nome e código.
 */
export function errFields(e: unknown): Record<string, unknown> {
  if (!(e instanceof Error)) return { errorName: typeof e };
  const code = (e as { code?: unknown }).code;
  if (typeof code === 'string') return { errorName: e.name, errorCode: code };
  return { errorName: e.name, error: e.message.slice(0, 500), stack: e.stack };
}

/** Logger interno do Nest em JSON (bootstrap, rotas mapeadas em debug). */
export const nestLogger: LoggerService = {
  log: (m: unknown, ...ctx: unknown[]) => logger.debug({ context: ctx.at(-1) }, String(m)),
  warn: (m: unknown, ...ctx: unknown[]) => logger.warn({ context: ctx.at(-1) }, String(m)),
  error: (m: unknown, ...ctx: unknown[]) => logger.error({ context: ctx.at(-1) }, String(m)),
  debug: (m: unknown, ...ctx: unknown[]) => logger.debug({ context: ctx.at(-1) }, String(m)),
  verbose: (m: unknown, ...ctx: unknown[]) => logger.trace({ context: ctx.at(-1) }, String(m)),
};
