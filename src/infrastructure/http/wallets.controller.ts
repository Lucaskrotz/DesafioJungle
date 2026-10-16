import { Body, Controller, Get, HttpCode, Inject, Param, Post, Query } from '@nestjs/common';
import { OpenWalletUseCase } from '../../application/open-wallet';
import { QueryService } from '../../application/queries';
import { ReconcileWalletUseCase } from '../../application/reconcile-wallet';
import { CorrelationId } from './observability';
import { ledgerQuerySchema, openWalletSchema, parse, uuidSchema } from './schemas';

@Controller('wallets')
export class WalletsController {
  constructor(
    @Inject(OpenWalletUseCase) private readonly openWallet: OpenWalletUseCase,
    @Inject(QueryService) private readonly queries: QueryService,
    @Inject(ReconcileWalletUseCase) private readonly reconcile: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(201)
  open(@Body() body: unknown, @CorrelationId() correlationId: string) {
    return this.openWallet.execute(parse(openWalletSchema, body), correlationId);
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: string) {
    return this.queries.getWallet(parse(uuidSchema, walletId));
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query() query: unknown) {
    const { cursor, limit } = parse(ledgerQuerySchema, query);
    return this.queries.listLedger(parse(uuidSchema, walletId), cursor, limit);
  }

  /**
   * Auditoria: 200 mesmo com divergência (`consistent: false`). A divergência é um resultado da
   * verificação, não falha da requisição; nada é corrigido. 404 se a wallet não existe.
   */
  @Post(':walletId/reconciliation')
  @HttpCode(200)
  reconciliation(@Param('walletId') walletId: string, @CorrelationId() correlationId: string) {
    return this.reconcile.execute(parse(uuidSchema, walletId), correlationId);
  }
}
