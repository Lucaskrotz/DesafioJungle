import { Controller, Get, Inject, Res } from '@nestjs/common';
import type { PromMetrics } from '../observability/metrics';
import { METRICS } from '../tokens';

type Response = { setHeader(k: string, v: string): unknown };

/** Formato texto do Prometheus. Sem autenticação, como /health (o guard global é no-op; um guard real deve liberar esta rota). */
@Controller('metrics')
export class MetricsController {
  constructor(@Inject(METRICS) private readonly metrics: PromMetrics) {}

  @Get()
  async scrape(@Res({ passthrough: true }) res: Response) {
    res.setHeader('Content-Type', this.metrics.registry.contentType);
    return this.metrics.registry.metrics();
  }
}
