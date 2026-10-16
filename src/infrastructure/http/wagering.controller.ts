import { Body, Controller, Get, Headers, Inject, Param, Post, Res } from '@nestjs/common';
// Sem @types/express: tipo mínimo do que usamos da resposta.
type Response = { status(code: number): Response; setHeader(k: string, v: string): unknown; json(body: unknown): unknown };
import { QueryService } from '../../application/queries';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager-transaction';
import { idempotencyKeySchema, parse, submitSchema, uuidSchema } from './schemas';

const STATUS_HTTP = { PROCESSED: 200, PENDING_REFERENCE: 202, REJECTED: 422, FAILED: 500 } as const;

@Controller()
export class WageringController {
  constructor(
    @Inject(SubmitWagerTransactionUseCase) private readonly submit: SubmitWagerTransactionUseCase,
    @Inject(QueryService) private readonly queries: QueryService,
  ) {}

  @Post('wagering/transactions')
  async create(@Body() body: unknown, @Headers('idempotency-key') key: unknown, @Res({ passthrough: true }) res: Response) {
    const idempotencyKey = parse(idempotencyKeySchema, key);
    const result = await this.submit.execute({ ...parse(submitSchema, body), idempotencyKey });
    res.status(STATUS_HTTP[result.status]);
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  byId(@Param('transactionId') id: string) {
    return this.queries.getTransaction(parse(uuidSchema, id));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  byExternal(@Param('providerId') providerId: string, @Param('externalTransactionId') externalId: string) {
    return this.queries.getTransactionByExternal(providerId, externalId);
  }
}
