import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { httpObservability } from './infrastructure/http/observability';
import { nestLogger } from './infrastructure/observability/logger';

export async function createApp({ workers = false }: { workers?: boolean } = {}) {
  const app = await NestFactory.create(AppModule.register({ workers }), { logger: nestLogger });
  app.use(httpObservability); // antes do roteamento: correlationId e log de acesso valem para toda rota
  return app;
}
