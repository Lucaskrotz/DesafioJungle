import { newId } from '../../application/id';
import { noopObservability, type MetricsPort, type UnitOfWork } from '../../application/ports';
import { applyWagerTransaction } from '../../application/wager-processor';
import { WagerTransactionStatus, type WagerTransaction } from '../../domain/wager-transaction';
import { errFields, logger } from '../observability/logger';
import { PollLoop } from './poll';

export interface PendingReferenceWorkerOptions {
  batchSize?: number;
  intervalMs?: number;
  metrics?: MetricsPort;
}

const OUTCOME = {
  [WagerTransactionStatus.Processed]: 'resolved',
  [WagerTransactionStatus.Rejected]: 'rejected',
  [WagerTransactionStatus.PendingReference]: 'rescheduled',
} as const;

/**
 * Reprocessa PENDING_REFERENCE vencidas. Candidatas lidas sem lock; cada uma roda em transação própria
 * com a MESMA ordem de lock do caso de uso (wallet -> transação), então não há deadlock com o tráfego normal.
 * Concorrência entre instâncias: o lock da wallet serializa e a revalidação descarta o que outra já tratou.
 */
export class PendingReferenceWorker {
  private readonly logger = logger.child({ component: 'PendingReferenceWorker' });
  private readonly loop: PollLoop;
  private readonly batchSize: number;
  private readonly metrics: MetricsPort;

  constructor(
    private readonly uow: UnitOfWork,
    opts: PendingReferenceWorkerOptions = {},
  ) {
    this.batchSize = opts.batchSize ?? 50;
    this.metrics = opts.metrics ?? noopObservability.metrics;
    this.loop = new PollLoop(() => this.runOnce(), opts.intervalMs ?? 1_000, this.logger);
  }

  start(): void {
    this.loop.start();
  }

  stop(): Promise<void> {
    return this.loop.stop();
  }

  /** `handled` = candidatas efetivamente reaplicadas (as já resolvidas por outra instância são puladas). */
  async runOnce(now: Date = new Date()): Promise<{ handled: number; failed: number }> {
    const due = await this.uow.run((r) => r.transactions.listDuePendingReferences(this.batchSize, now));
    let handled = 0;
    let failed = 0;
    for (const c of due) {
      const correlationId = newId();
      try {
        const applied = await this.uow.run(async (r): Promise<WagerTransaction | undefined> => {
          const wallet = await r.wallets.findByIdForUpdate(c.walletId); // 1º lock: wallet
          const tx = wallet && (await r.transactions.findByIdForUpdate(c.id)); // 2º lock: transação
          const stillDue =
            tx?.status === WagerTransactionStatus.PendingReference &&
            tx.nextReferenceAttemptAt !== undefined &&
            tx.nextReferenceAttemptAt <= now;
          if (!wallet || !tx || !stillDue) return undefined;
          await applyWagerTransaction(r, wallet, tx, 'update', { correlationId, causationId: tx.id }, now);
          return tx;
        });
        if (!applied) continue;
        handled++;
        // métricas após o commit
        const outcome = OUTCOME[applied.status as keyof typeof OUTCOME];
        if (outcome) this.metrics.pendingReference(outcome);
        if (outcome !== 'rescheduled') this.metrics.transactionRecorded(applied.status, applied.kind, applied.failureCode);
        this.logger.info(
          {
            correlationId,
            transactionId: applied.id,
            walletId: applied.walletId,
            providerId: applied.providerId,
            status: applied.status,
            failureCode: applied.failureCode,
            outcome,
          },
          'pending reference handled',
        );
      } catch (e) {
        // uma candidata com problema não trava as demais
        failed++;
        this.logger.error({ correlationId, transactionId: c.id, walletId: c.walletId, ...errFields(e) }, 'pending reference failed');
      }
    }
    return { handled, failed };
  }
}
