import 'reflect-metadata';
import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, MikroORM } from '@mikro-orm/postgresql';
import { join } from 'node:path';
import { loadConfig } from '../../config';
import { ALL_SCHEMAS } from './schemas';

export function buildOrmConfig(databaseUrl = loadConfig().databaseUrl) {
  return defineConfig({
    clientUrl: databaseUrl,
    entities: ALL_SCHEMAS,
    extensions: [Migrator],
    allowGlobalContext: false,
    pool: { min: 1, max: loadConfig().dbPoolMax },
    migrations: {
      path: join(import.meta.dir, 'migrations'),
      pathTs: join(import.meta.dir, 'migrations'),
      glob: '!(*.d).{js,ts}',
      transactional: true,
      allOrNothing: true,
      snapshot: false,
      emit: 'ts',
    },
  });
}

export const createOrm = (databaseUrl?: string) => MikroORM.init(buildOrmConfig(databaseUrl));
export type Orm = Awaited<ReturnType<typeof createOrm>>;
