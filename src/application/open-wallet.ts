import { WalletBalanceChanged, WagerTransactionProcessed } from '../domain/events';
import { Money, MoneyProps } from '../domain/money';
import { OutboxMessage } from '../domain/outbox-message';
import { computePayloadHash } from '../domain/payload-hash';
import { WagerTransaction } from '../domain/wager-transaction';
import { Wallet } from '../domain/wallet';
import { newId } from './id';
import { noopObservability, type Observability, type UnitOfWork } from './ports';

export interface OpenWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export function toWalletView(w: Wallet): WalletView {
  return { id: w.id, playerId: w.playerId, balance: w.balance.toJSON(), version: w.version };
}

export class OpenWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly obs: Observability = noopObservability,
  ) {}

  async execute(cmd: OpenWalletCommand, correlationId: string = newId()): Promise<WalletView> {
    const initialBalance = Money.from(cmd.initialBalance);
    const walletId = newId();
    const now = new Date();
    const wallet = Wallet.open({ id: walletId, playerId: cmd.playerId, initialBalance, at: now });

    await this.uow.run(async (r) => {
      await r.wallets.insert(wallet); // 409 se (playerId, currency) já existe

      const openingTxId = newId();
      const entry = wallet.openingEntry(openingTxId, newId());
      if (!entry) return;

      // saldo inicial > 0: transação interna OPENING + lançamento CREDIT, na mesma transação SQL
      const tx = WagerTransaction.opening({
        id: openingTxId,
        walletId,
        playerId: cmd.playerId,
        money: initialBalance,
        payloadHash: computePayloadHash({
          providerId: 'internal', externalTransactionId: `opening:${walletId}`, playerId: cmd.playerId, walletId,
          roundId: 'opening', gameId: 'opening', kind: 'OPENING', money: initialBalance.toJSON(),
        }),
        createdAt: now,
      });
      tx.markProcessed(undefined, now, wallet.balance);
      await r.transactions.insert(tx);
      await r.ledger.insert(entry);
      const ctx = () => ({ eventId: newId(), correlationId, occurredAt: now });
      await r.outbox.add(OutboxMessage.enqueue(WagerTransactionProcessed.from(tx, ctx())));
      await r.outbox.add(OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, entry, ctx())));
    });

    this.obs.logger.info({ correlationId, walletId }, 'wallet opened');
    return toWalletView(wallet);
  }
}
