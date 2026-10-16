import { createOrm } from './orm.config';

const direction = process.argv[2];
if (direction !== 'up' && direction !== 'down') {
  console.error('usage: bun migrate.ts <up|down>');
  process.exit(1);
}

const orm = await createOrm();
try {
  const migrator = orm.getMigrator();
  // `down` desfaz apenas a última migration (comportamento padrão do MikroORM)
  const done = direction === 'up' ? await migrator.up() : await migrator.down();
  console.log(`${direction}: ${done.length ? done.map((m) => m.name).join(', ') : 'nothing to do'}`);
} finally {
  await orm.close();
}
