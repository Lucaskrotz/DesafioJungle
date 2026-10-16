import { Logger } from '@nestjs/common';
import { newId } from '../../application/id';
import type { UnitOfWork } from '../../application/ports';
import { applyWagerTransaction } from '../../application/wager-processor';
import { WagerTransactionStatus } from '../../domain/wager-transaction';
import { PollLoop } from './poll';

export interface PendingReferenceWorkerOptions {
  batchSize?: number;
  intervalMs?: number;
}

/**
 * Reprocessa PENDING_REFERENCE vencidas. Candidatas lidas sem lock; cada uma roda em transação própria
 * com a MESMA ordem de lock do caso de uso (wallet -> transação), então não há deadlock com o tráfego normal.
 * Concorrência entre instâncias: o lock da wallet serializa e a revalidação descarta o que outra já tratou.
 */
export class PendingReferenceWorker {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private readonly loop: PollLoop;
  private readonly batchSize: number;

  constructor(
    private readonly uow: UnitOfWork,
    opts: PendingReferenceWorkerOptions = {},
  ) {
    this.batchSize = opts.batchSize ?? 50;
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
      try {
        const applied = await this.uow.run(async (r) => {
          const wallet = await r.wallets.findByIdForUpdate(c.walletId); // 1º lock: wallet
          const tx = wallet && (await r.transactions.findByIdForUpdate(c.id)); // 2º lock: transação
          const stillDue =
            tx?.status === WagerTransactionStatus.PendingReference &&
            tx.nextReferenceAttemptAt !== undefined &&
            tx.nextReferenceAttemptAt <= now;
          if (!wallet || !tx || !stillDue) return false;
          await applyWagerTransaction(r, wallet, tx, 'update', { correlationId: newId(), causationId: tx.id }, now);
          return true;
        });
        if (applied) handled++;
      } catch (e) {
        // uma candidata com problema não trava as demais
        failed++;
        this.logger.error(`pending reference failed transactionId=${c.id} walletId=${c.walletId}: ${(e as Error).message}`);
      }
    }
    return { handled, failed };
  }
}
