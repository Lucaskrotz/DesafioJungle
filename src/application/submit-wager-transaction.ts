import { InvalidWagerTransactionError } from '../domain/errors';
import { FailureCode } from '../domain/failure-code';
import { Money, MoneyProps } from '../domain/money';
import { computePayloadHash } from '../domain/payload-hash';
import { WagerTransaction, WagerTransactionKind } from '../domain/wager-transaction';
import { InboxMessage } from '../domain/inbox-message';
import { IdempotencyConflictError, InboxPayloadMismatchError } from './errors';
import { newId } from './id';
import type { TxRepos, UnitOfWork } from './ports';
import { applyWagerTransaction } from './wager-processor';

export interface SubmitWagerCommand {
  /** Header Idempotency-Key (HTTP) ou data.idempotencyKey (SQS): fonte da verdade. */
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface SubmitContext {
  correlationId?: string;
  messageId?: string;
  /** Entrada via fila: deduplicação persistente na MESMA transação do efeito financeiro. */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

export interface SubmitResult {
  /** Ausente quando a rejeição não pôde ser persistida (wallet inexistente, player/moeda divergentes). */
  transactionId?: string;
  status: 'PROCESSED' | 'REJECTED' | 'PENDING_REFERENCE' | 'FAILED';
  failureCode?: FailureCode;
  balance?: MoneyProps;
  idempotentReplay: boolean;
  /** Só na entrada via fila: a mensagem já tinha sido processada; nada foi feito. */
  duplicateMessage?: true;
}

export class SubmitWagerTransactionUseCase {
  constructor(private readonly uow: UnitOfWork) {}

  async execute(cmd: SubmitWagerCommand, ctx: SubmitContext = {}): Promise<SubmitResult> {
    const correlationId = ctx.correlationId ?? newId();
    const id = newId();
    const payloadHash = hashOf(cmd);
    // Validação pura antes de tocar o banco (lança InvalidMoneyError / InvalidWagerTransactionError => 400)
    const build = () => buildTransaction(id, cmd, payloadHash);
    build();

    const handle = async (r: TxRepos): Promise<SubmitResult> => {
      const wallet = await r.wallets.findByIdForUpdate(cmd.walletId);
      if (!wallet) return unpersisted(FailureCode.WalletNotFound);

      const existing = await r.transactions.findForIdempotency(cmd.idempotencyKey, cmd.providerId, cmd.externalTransactionId);
      if (existing) {
        if (existing.idempotencyKey !== cmd.idempotencyKey || !existing.matchesPayload(payloadHash)) {
          throw new IdempotencyConflictError('Idempotency key or external transaction id already used with a different payload');
        }
        return toResult(existing, true);
      }

      if (wallet.playerId !== cmd.playerId) return unpersisted(FailureCode.WalletPlayerMismatch);
      if (wallet.currency !== cmd.money.currency) return unpersisted(FailureCode.CurrencyMismatch);

      const tx = build(); // instância nova a cada tentativa (o UoW pode repetir a função)
      await applyWagerTransaction(r, wallet, tx, 'insert', { correlationId, causationId: ctx.messageId });
      return toResult(tx, false);
    };

    const inbox = ctx.inbox;
    if (!inbox) return this.uow.run(handle);

    return this.uow.run(async (r) => {
      // inbox antes de qualquer lock: duplicata não toca na wallet; exceção => rollback e o inbox some junto
      const received = await r.inbox.tryReceive(InboxMessage.receive({ ...inbox, receivedAt: new Date() }));
      if (!received) {
        const stored = await r.inbox.find(inbox.consumerName, inbox.messageId);
        if (stored?.payloadHash !== inbox.payloadHash) {
          throw new InboxPayloadMismatchError(`Message ${inbox.messageId} already received with a different payload`);
        }
        const existing = await r.transactions.findForIdempotency(cmd.idempotencyKey, cmd.providerId, cmd.externalTransactionId);
        // sem transação persistida (ex.: wallet inexistente) não há resultado a repetir
        return { ...(existing ? toResult(existing, true) : { status: 'REJECTED' as const, idempotentReplay: true }), duplicateMessage: true };
      }
      const result = await handle(r);
      await r.inbox.markProcessed(inbox.consumerName, inbox.messageId, new Date());
      return result;
    });
  }
}

function hashOf(cmd: SubmitWagerCommand): string {
  return computePayloadHash({
    providerId: cmd.providerId,
    externalTransactionId: cmd.externalTransactionId,
    playerId: cmd.playerId,
    walletId: cmd.walletId,
    roundId: cmd.roundId,
    gameId: cmd.gameId,
    kind: cmd.kind,
    // normaliza (ex.: "25" == "25.00") para o hash não depender da grafia
    money: Money.from(cmd.money).toJSON(),
    referenceExternalTransactionId: cmd.referenceExternalTransactionId,
  });
}

function buildTransaction(id: string, cmd: SubmitWagerCommand, payloadHash: string): WagerTransaction {
  const kinds = Object.values(WagerTransactionKind).filter((k) => k !== WagerTransactionKind.Opening) as string[];
  if (!kinds.includes(cmd.kind)) throw new InvalidWagerTransactionError(`Invalid kind: ${cmd.kind}`);
  return WagerTransaction.create({
    id,
    providerId: cmd.providerId,
    externalTransactionId: cmd.externalTransactionId,
    idempotencyKey: cmd.idempotencyKey,
    payloadHash,
    walletId: cmd.walletId,
    playerId: cmd.playerId,
    roundId: cmd.roundId,
    gameId: cmd.gameId,
    kind: cmd.kind as WagerTransactionKind,
    money: Money.from(cmd.money),
    referenceExternalTransactionId: cmd.referenceExternalTransactionId,
    createdAt: new Date(),
  });
}

function unpersisted(failureCode: FailureCode): SubmitResult {
  return { status: 'REJECTED', failureCode, idempotentReplay: false };
}

/** Resultado derivado do estado persistido: replay e primeira execução devolvem a mesma coisa. */
export function toResult(tx: WagerTransaction, idempotentReplay: boolean): SubmitResult {
  return {
    transactionId: tx.id,
    status: tx.status as SubmitResult['status'], // PENDING nunca é persistido
    ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    ...(tx.balanceAfter ? { balance: tx.balanceAfter.toJSON() } : {}),
    idempotentReplay,
  };
}

export type { TxRepos };
