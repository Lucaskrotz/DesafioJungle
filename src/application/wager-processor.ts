import { InsufficientFundsError } from '../domain/errors';
import {
  EventContext,
  IntegrationEvent,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../domain/events';
import { FailureCode } from '../domain/failure-code';
import { OutboxMessage } from '../domain/outbox-message';
import {
  MAX_REFERENCE_ATTEMPTS,
  referenceBackoffMs,
  WagerTransaction,
  WagerTransactionKind,
} from '../domain/wager-transaction';
import { Wallet } from '../domain/wallet';
import { LedgerDirection } from '../domain/wallet-ledger-entry';
import { newId } from './id';
import type { TxRepos } from './ports';

export interface ProcessContext {
  correlationId: string;
  causationId?: string;
}

/**
 * Núcleo financeiro, compartilhado por HTTP, SQS e pelo worker de PENDING_REFERENCE.
 *
 * Pré-condição: roda dentro de uma transação SQL e `wallet` foi lida com FOR UPDATE.
 * Ordem de lock fixa: wallet -> linha(s) de transação. Nunca o inverso (evita deadlock).
 *
 * `mode`: 'insert' para transação nova, 'update' para uma PENDING_REFERENCE já persistida.
 * Tudo que muda (transação, ledger, saldo, outbox) é gravado aqui, na mesma transação do chamador.
 */
export async function applyWagerTransaction(
  r: TxRepos,
  wallet: Wallet,
  tx: WagerTransaction,
  mode: 'insert' | 'update',
  cx: ProcessContext,
  now: Date = new Date(), // injetável: o worker decide vencimento/backoff com o mesmo relógio
): Promise<void> {
  const persist = () => (mode === 'insert' ? r.transactions.insert(tx) : r.transactions.update(tx));
  const emit = (make: (ctx: EventContext) => IntegrationEvent<unknown>) =>
    r.outbox.add(
      OutboxMessage.enqueue(make({ eventId: newId(), correlationId: cx.correlationId, causationId: cx.causationId, occurredAt: now })),
    );
  const reject = async (code: FailureCode) => {
    tx.reject(code, wallet.balance); // saldo observado: o replay devolve o mesmo resultado
    await persist();
    await emit((c) => WagerTransactionRejected.from(tx, c));
  };

  // 1. Resolver a referência (quando houver)
  const refExt = tx.referenceExternalTransactionId;
  const reference = refExt === undefined ? undefined : await r.transactions.findByExternalForUpdate(tx.providerId, refExt);
  const verdict = tx.evaluateReference(reference);

  if (verdict.outcome === 'reject') return reject(verdict.code);

  if (verdict.outcome === 'wait') {
    if (mode === 'update' && tx.referenceAttempts + 1 >= MAX_REFERENCE_ATTEMPTS) {
      return reject(reference ? FailureCode.ReferenceNotProcessed : FailureCode.ReferenceNotFound);
    }
    tx.markPendingReference(new Date(now.getTime() + referenceBackoffMs(tx.referenceAttempts + (mode === 'update' ? 1 : 0))));
    await persist();
    if (mode === 'insert') await emit((c) => WagerTransactionPendingReference.from(tx, c));
    return;
  }

  // 2. Regra 4: uma referência só pode ser revertida uma vez (o índice único é a garantia final)
  if (tx.requiresReference() && reference && (await r.transactions.hasProcessedReversal(reference.id))) {
    return reject(FailureCode.ReferenceAlreadyReversed);
  }

  // 3. LOSS: registra o resultado sem mover saldo
  if (!tx.affectsBalance()) {
    tx.markProcessed(reference?.id, now, wallet.balance);
    await persist();
    await emit((c) => WagerTransactionProcessed.from(tx, c));
    return;
  }

  // 4. Movimentação: saldo e ledger mudam juntos
  const direction = tx.ledgerDirectionFor(reference);
  const expectedVersion = wallet.version;
  const movement = { money: tx.money, transactionId: tx.id, entryId: newId(), at: now };
  let entry;
  try {
    entry = direction === LedgerDirection.Debit ? wallet.debit(movement) : wallet.credit(movement);
  } catch (e) {
    if (e instanceof InsufficientFundsError) {
      // aposta sem saldo e reversão que estouraria o saldo são situações operacionais distintas
      return reject(tx.kind === WagerTransactionKind.Bet ? FailureCode.InsufficientFunds : FailureCode.ReversalWouldOverdraw);
    }
    throw e;
  }
  tx.markProcessed(reference?.id, now, wallet.balance);
  await persist(); // antes do ledger: o ledger referencia a transação
  await r.ledger.insert(entry);
  await r.wallets.updateBalance(wallet, expectedVersion);
  await emit((c) => WagerTransactionProcessed.from(tx, c));
  await emit((c) => WalletBalanceChanged.from(wallet, entry, c));
}
