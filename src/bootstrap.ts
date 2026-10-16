import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

export const createApp = ({ workers = false }: { workers?: boolean } = {}) =>
  NestFactory.create(AppModule.register({ workers }), { logger: ['error', 'warn'] });
