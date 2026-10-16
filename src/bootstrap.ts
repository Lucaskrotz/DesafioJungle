import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

export const createApp = () => NestFactory.create(AppModule, { logger: ['error', 'warn'] });
