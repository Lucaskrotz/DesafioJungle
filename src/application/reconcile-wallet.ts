import { Money, type MoneyProps } from '../domain/money';
import { NotFoundError } from './errors';
import { newId } from './id';
import { noopObservability, type Observability, type UnitOfWork } from './ports';

export interface ReconciliationView {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  /** stored − calculated */
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
  /** Só presente quando a cadeia balance_before/balance_after do ledger tem buraco. */
  chainBroken?: true;
}

/**
 * Auditoria: compara wallets.balance com o saldo reconstruído do ledger (CREDIT − DEBIT).
 * Lê um snapshot único sem travar a wallet. Divergência NÃO é corrigida: é logada, contada e devolvida.
 */
export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly obs: Observability = noopObservability,
  ) {}

  async execute(walletId: string, correlationId: string = newId()): Promise<ReconciliationView> {
    const snap = await this.uow.run((r) => r.ledger.reconciliationSnapshot(walletId));
    if (!snap) throw new NotFoundError('Wallet not found');

    const money = (amount: string) => Money.from({ amount, currency: snap.currency });
    const stored = money(snap.storedBalance);
    // créditos e débitos chegam separados (não negativos): o cálculo fica todo em Decimal
    const calculated = Money.zero(snap.currency).add(money(snap.credits)).subtract(money(snap.debits));
    const difference = stored.subtract(calculated);
    const chainBroken = snap.chainBreaks > 0;
    const consistent = difference.isZero() && !chainBroken;

    if (!consistent) {
      this.obs.metrics.reconciliationDivergence();
      this.obs.logger.error(
        {
          correlationId,
          walletId,
          difference: difference.toString(),
          checkedEntries: snap.entries,
          ...(chainBroken ? { chainBroken: true, chainBreaks: snap.chainBreaks } : {}),
        },
        'wallet reconciliation divergence',
      );
    }

    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: snap.entries,
      ...(chainBroken ? { chainBroken: true as const } : {}),
    };
  }
}
