import { Body, Controller, Get, HttpCode, Inject, Param, Post, Query } from '@nestjs/common';
import { OpenWalletUseCase } from '../../application/open-wallet';
import { QueryService } from '../../application/queries';
import { ledgerQuerySchema, openWalletSchema, parse, uuidSchema } from './schemas';

@Controller('wallets')
export class WalletsController {
  constructor(
    @Inject(OpenWalletUseCase) private readonly openWallet: OpenWalletUseCase,
    @Inject(QueryService) private readonly queries: QueryService,
  ) {}

  @Post()
  @HttpCode(201)
  open(@Body() body: unknown) {
    return this.openWallet.execute(parse(openWalletSchema, body));
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
}
