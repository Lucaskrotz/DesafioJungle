import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { IdempotencyConflictError, WalletAlreadyExistsError } from '../../src/application/errors';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import { QueryService } from '../../src/application/queries';
import { SubmitWagerTransactionUseCase, type SubmitWagerCommand } from '../../src/application/submit-wager-transaction';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';

// Requer Postgres com migrations aplicadas (bun run infra:up && bun run migration:up).
let orm: Orm;
let uow: MikroOrmUnitOfWork;
let openWallet: OpenWalletUseCase;
let submit: SubmitWagerTransactionUseCase;
let queries: QueryService;

beforeAll(async () => {
  orm = await createOrm();
  uow = new MikroOrmUnitOfWork(orm);
  openWallet = new OpenWalletUseCase(uow);
  submit = new SubmitWagerTransactionUseCase(uow);
  queries = new QueryService(uow);
});
afterAll(() => orm.close());

const uid = () => crypto.randomUUID();

async function setup(initial = '100.00') {
  const playerId = uid();
  const wallet = await openWallet.execute({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
  const roundId = uid();
  const cmd = (kind: string, amount: string, ext: string, ref?: string): SubmitWagerCommand => ({
    idempotencyKey: `${wallet.id}:${ext}`,
    providerId: 'prov-a',
    externalTransactionId: `${wallet.id}:${ext}`,
    playerId,
    walletId: wallet.id,
    roundId,
    gameId: 'g1',
    kind,
    money: { amount, currency: 'BRL' },
    referenceExternalTransactionId: ref === undefined ? undefined : `${wallet.id}:${ref}`,
  });
  return { wallet, playerId, cmd };
}

describe('persistência ponta a ponta', () => {
  test('abertura cria wallet, transação OPENING e lançamento; duplicata -> WalletAlreadyExistsError', async () => {
    const { wallet, playerId } = await setup('100');
    expect(wallet.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    expect((await queries.getWallet(wallet.id)).balance).toEqual({ amount: '100.00', currency: 'BRL' });
    const ledger = await queries.listLedger(wallet.id, undefined);
    expect(ledger.items).toHaveLength(1);
    expect(ledger.items[0]!.direction).toBe('CREDIT');
    await expect(openWallet.execute({ playerId, initialBalance: { amount: '0', currency: 'BRL' } })).rejects.toBeInstanceOf(
      WalletAlreadyExistsError,
    );
  });

  test('BET, replay, conflito, WIN, LOSS, REFUND, ROLLBACK e saldo insuficiente', async () => {
    const { wallet, cmd } = await setup('100.00');

    const bet = await submit.execute(cmd('BET', '25.50', 'b1'));
    expect(bet).toMatchObject({ status: 'PROCESSED', balance: { amount: '74.50', currency: 'BRL' }, idempotentReplay: false });

    const replay = await submit.execute(cmd('BET', '25.50', 'b1'));
    expect(replay).toEqual({ ...bet, idempotentReplay: true });
    await expect(submit.execute(cmd('BET', '30.00', 'b1'))).rejects.toBeInstanceOf(IdempotencyConflictError);

    expect(await submit.execute(cmd('WIN', '50.00', 'w1', 'b1'))).toMatchObject({ status: 'PROCESSED', balance: { amount: '124.50' } });

    const bet2 = await submit.execute(cmd('BET', '10.00', 'b2'));
    expect(bet2.balance?.amount).toBe('114.50');
    expect(await submit.execute(cmd('LOSS', '10.00', 'l2', 'b2'))).toMatchObject({ status: 'PROCESSED', balance: { amount: '114.50' } });

    const bet3 = await submit.execute(cmd('BET', '4.50', 'b3'));
    expect(bet3.balance?.amount).toBe('110.00');
    expect(await submit.execute(cmd('REFUND', '4.50', 'r3', 'b3'))).toMatchObject({ status: 'PROCESSED', balance: { amount: '114.50' } });
    // segunda reversão da mesma BET
    expect(await submit.execute(cmd('ROLLBACK', '4.50', 'rb3', 'b3'))).toMatchObject({
      status: 'REJECTED',
      failureCode: 'REFERENCE_ALREADY_REVERSED',
    });

    const bet4 = await submit.execute(cmd('BET', '14.50', 'b4'));
    expect(bet4.balance?.amount).toBe('100.00');
    expect(await submit.execute(cmd('ROLLBACK', '14.50', 'rb4', 'b4'))).toMatchObject({ status: 'PROCESSED', balance: { amount: '114.50' } });

    const broke = await submit.execute(cmd('BET', '1000.00', 'b5'));
    expect(broke).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '114.50' } });
    expect(await submit.execute(cmd('BET', '1000.00', 'b5'))).toEqual({ ...broke, idempotentReplay: true });

    // referência ainda inexistente
    expect((await submit.execute(cmd('REFUND', '1.00', 'r9', 'missing'))).status).toBe('PENDING_REFERENCE');

    const view = await queries.getWallet(wallet.id);
    expect(view.balance.amount).toBe('114.50');

    // ledger: abertura + b1, w1, b2, b3, r3, b4, rb4 = 8; paginação por cursor estável
    const p1 = await queries.listLedger(wallet.id, undefined, 5);
    const p2 = await queries.listLedger(wallet.id, p1.nextCursor!, 5);
    expect(p1.items).toHaveLength(5);
    expect(p2.items).toHaveLength(3);
    expect(p2.nextCursor).toBeNull();
    expect(p2.items.at(-1)!.balanceAfter.amount).toBe('114.50');

    const tx = await queries.getTransaction(bet.transactionId!);
    expect(tx).toMatchObject({ kind: 'BET', money: { amount: '25.50', currency: 'BRL' }, status: 'PROCESSED' });
  });

  test('concorrência na mesma wallet não perde atualização', async () => {
    const { wallet, cmd } = await setup('100.00');
    const results = await Promise.all(Array.from({ length: 20 }, (_, i) => submit.execute(cmd('BET', '7.00', `c${i}`))));
    expect(results.filter((r) => r.status === 'PROCESSED')).toHaveLength(14);
    expect(results.filter((r) => r.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(6);
    expect((await queries.getWallet(wallet.id)).balance.amount).toBe('2.00');
  });
});
