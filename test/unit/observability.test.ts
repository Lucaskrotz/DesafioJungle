import { describe, expect, test } from 'bun:test';
import { NotFoundError } from '../../src/application/errors';
import { noopObservability, type ReconciliationSnapshot, type UnitOfWork } from '../../src/application/ports';
import { ReconcileWalletUseCase } from '../../src/application/reconcile-wallet';
import { normalizePath, resolveCorrelationId, routeOf } from '../../src/infrastructure/http/observability';
import { createLogger, errFields } from '../../src/infrastructure/observability/logger';

const capture = () => {
  const lines: string[] = [];
  return { lines, logger: createLogger({ write: (l: string) => void lines.push(l) }, 'info') };
};

describe('logger: redact', () => {
  test('valores financeiros e credenciais nunca saem, no topo e um nível abaixo', () => {
    const { lines, logger } = capture();
    logger.info(
      {
        walletId: 'w-1',
        money: { amount: '25.00', currency: 'BRL' },
        amount: '11.11',
        balance: '22.22',
        balanceAfter: '33.33',
        initialBalance: { amount: '44.44', currency: 'BRL' },
        storedBalance: '55.55',
        data: { amount: '66.66', money: { amount: '77.77' }, balanceBefore: '88.88' },
        headers: { authorization: 'Bearer secreto' },
      },
      'evento',
    );
    const line = lines[0]!;
    for (const v of ['25.00', '11.11', '22.22', '33.33', '44.44', '55.55', '66.66', '77.77', '88.88', 'secreto']) {
      expect(line).not.toContain(v);
    }
    const parsed = JSON.parse(line);
    expect(parsed).toMatchObject({ level: 'info', service: 'wager-processor', walletId: 'w-1', msg: 'evento' });
    expect(parsed.pid).toBe(process.pid);
  });

  test('erro de driver (com SQLSTATE) não expõe a mensagem, que traz o SQL com parâmetros', () => {
    const driver = Object.assign(new Error(`insert into "wallets" values ('100.00') - duplicate key`), { code: '23505' });
    expect(errFields(driver)).toEqual({ errorName: 'Error', errorCode: '23505' });
    expect(errFields(new TypeError('boom'))).toMatchObject({ errorName: 'TypeError', error: 'boom' });
  });
});

describe('http: rota normalizada e correlationId', () => {
  test('usa o padrão da rota quando houve match', () => {
    expect(routeOf({ path: '/wallets/0192f291-27dd-7d3f-8071-5f8685deef37', route: { path: '/wallets/:walletId' } })).toBe(
      '/wallets/:walletId',
    );
  });

  test('sem match: UUID e números viram placeholder', () => {
    expect(normalizePath('/wallets/0192F291-27DD-7D3F-8071-5F8685DEEF37/ledger')).toBe('/wallets/:id/ledger');
    expect(normalizePath('/x/123/y/')).toBe('/x/:n/y');
    expect(normalizePath('/')).toBe('/');
    expect(routeOf({ path: '/nope/42' })).toBe('/nope/:n');
  });

  test('correlationId válido é mantido; ausente/inválido gera UUIDv7', () => {
    expect(resolveCorrelationId('corr-1:abc.2')).toBe('corr-1:abc.2');
    for (const bad of [undefined, '', 'tem espaço', 'x'.repeat(129), ['a', 'b'], '{"inj":1}']) {
      expect(resolveCorrelationId(bad)).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    }
  });
});

describe('ReconcileWalletUseCase (snapshot simulado)', () => {
  const uowWith = (snap: ReconciliationSnapshot | undefined): UnitOfWork => ({
    run: (fn) => fn({ ledger: { reconciliationSnapshot: async () => snap } } as never),
  });
  const snap = (over: Partial<ReconciliationSnapshot> = {}): ReconciliationSnapshot => ({
    storedBalance: '70.00',
    currency: 'BRL',
    credits: '100.00',
    debits: '30.00',
    entries: 2,
    chainBreaks: 0,
    ...over,
  });
  const spy = () => {
    const calls: { level: string; fields: Record<string, unknown>; msg: string }[] = [];
    let divergences = 0;
    const obs = {
      logger: {
        info: (fields: Record<string, unknown>, msg: string) => void calls.push({ level: 'info', fields, msg }),
        warn: (fields: Record<string, unknown>, msg: string) => void calls.push({ level: 'warn', fields, msg }),
        error: (fields: Record<string, unknown>, msg: string) => void calls.push({ level: 'error', fields, msg }),
      },
      metrics: { ...noopObservability.metrics, reconciliationDivergence: () => void divergences++ },
    };
    return { obs, calls, divergences: () => divergences };
  };

  test('consistente: diferença 0.00, sem log nem métrica, sem chainBroken', async () => {
    const s = spy();
    const r = await new ReconcileWalletUseCase(uowWith(snap()), s.obs).execute('w', 'c');
    expect(r).toEqual({
      walletId: 'w',
      storedBalance: { amount: '70.00', currency: 'BRL' },
      calculatedBalance: { amount: '70.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 2,
    });
    expect(s.calls).toHaveLength(0);
    expect(s.divergences()).toBe(0);
  });

  test('ledger vazio e wallet zerada: consistente', async () => {
    const r = await new ReconcileWalletUseCase(uowWith(snap({ storedBalance: '0.00', credits: '0', debits: '0', entries: 0 }))).execute('w');
    expect(r).toMatchObject({ consistent: true, checkedEntries: 0, difference: { amount: '0.00' }, calculatedBalance: { amount: '0.00' } });
  });

  test('saldo abaixo do ledger: diferença negativa, log de erro e métrica', async () => {
    const s = spy();
    const r = await new ReconcileWalletUseCase(uowWith(snap({ storedBalance: '60.01' })), s.obs).execute('w', 'c');
    expect(r).toMatchObject({ consistent: false, difference: { amount: '-9.99', currency: 'BRL' } });
    expect(r.chainBroken).toBeUndefined();
    expect(s.divergences()).toBe(1);
    expect(s.calls).toEqual([
      { level: 'error', msg: 'wallet reconciliation divergence', fields: { correlationId: 'c', walletId: 'w', difference: '-9.99', checkedEntries: 2 } },
    ]);
  });

  test('cadeia quebrada com somas batendo: inconsistente e chainBroken: true', async () => {
    const s = spy();
    const r = await new ReconcileWalletUseCase(uowWith(snap({ chainBreaks: 1 })), s.obs).execute('w', 'c');
    expect(r).toMatchObject({ consistent: false, chainBroken: true, difference: { amount: '0.00' } });
    expect(s.calls[0]!.fields).toMatchObject({ chainBroken: true, chainBreaks: 1 });
  });

  test('wallet inexistente: NotFoundError', async () => {
    await expect(new ReconcileWalletUseCase(uowWith(undefined)).execute('w')).rejects.toBeInstanceOf(NotFoundError);
  });
});
