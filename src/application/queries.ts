import { InvalidQueryError, NotFoundError } from './errors';
import { toWalletView, WalletView } from './open-wallet';
import type { UnitOfWork } from './ports';
import { SubmitResult, toResult } from './submit-wager-transaction';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { MoneyProps } from '../domain/money';

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerPage {
  items: LedgerEntryView[];
  nextCursor: string | null;
}

export interface TransactionView extends SubmitResult {
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  kind: string;
  money: MoneyProps;
  createdAt: string;
}

const MAX_LIMIT = 200;

/** Cursor opaco = base64url({ s: seq do último item }). `seq` é monotônico, então a paginação é estável. */
const encodeCursor = (seq: string) => Buffer.from(JSON.stringify({ s: seq })).toString('base64url');
function decodeCursor(cursor: string): string {
  try {
    const { s } = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (typeof s === 'string' && /^\d+$/.test(s)) return s;
  } catch {
    /* cai no erro abaixo */
  }
  throw new InvalidQueryError('Invalid cursor');
}

export class QueryService {
  constructor(private readonly uow: UnitOfWork) {}

  async getWallet(id: string): Promise<WalletView> {
    const wallet = await this.uow.run((r) => r.wallets.findById(id));
    if (!wallet) throw new NotFoundError('Wallet not found');
    return toWalletView(wallet);
  }

  async listLedger(walletId: string, cursor: string | undefined, limit = 50): Promise<LedgerPage> {
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
      throw new InvalidQueryError(`limit must be between 1 and ${MAX_LIMIT}`);
    }
    const after = cursor === undefined ? undefined : decodeCursor(cursor);
    return this.uow.run(async (r) => {
      if (!(await r.wallets.findById(walletId))) throw new NotFoundError('Wallet not found');
      const rows = await r.ledger.list(walletId, after, limit + 1);
      const page = rows.slice(0, limit);
      const last = page[page.length - 1];
      return {
        items: page.map(({ entry: e }) => ({
          id: e.id,
          transactionId: e.transactionId,
          direction: e.direction,
          money: e.money.toJSON(),
          balanceBefore: e.balanceBefore.toJSON(),
          balanceAfter: e.balanceAfter.toJSON(),
          createdAt: e.createdAt.toISOString(),
        })),
        nextCursor: rows.length > limit && last ? encodeCursor(last.seq) : null,
      };
    });
  }

  async getTransaction(id: string): Promise<TransactionView> {
    const tx = await this.uow.run((r) => r.transactions.findById(id));
    if (!tx) throw new NotFoundError('Transaction not found');
    return view(tx);
  }

  async getTransactionByExternal(providerId: string, externalId: string): Promise<TransactionView> {
    const tx = await this.uow.run((r) => r.transactions.findByExternal(providerId, externalId));
    if (!tx) throw new NotFoundError('Transaction not found');
    return view(tx);
  }
}

function view(tx: WagerTransaction): TransactionView {
  return {
    ...toResult(tx, false),
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    kind: tx.kind,
    money: tx.money.toJSON(),
    createdAt: tx.createdAt.toISOString(),
  };
}
