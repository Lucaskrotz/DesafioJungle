import { expect, test } from 'bun:test';
import { PollLoop } from '../../src/infrastructure/workers/poll';

const silent = { info() {}, warn() {}, error() {}, debug() {} } as never;

test('PollLoop: ciclo que devolve true (lote cheio) emenda o próximo sem esperar o intervalo', async () => {
  let calls = 0;
  const loop = new PollLoop(async () => ++calls <= 3, 60_000, silent);
  loop.start();
  // 3 ciclos "cheios" + 1 vazio, todos bem antes do intervalo de 60 s
  const deadline = Date.now() + 2_000;
  while (calls < 4 && Date.now() < deadline) await Bun.sleep(5);
  await Bun.sleep(50);
  await loop.stop();
  expect(calls).toBe(4);
});

test('PollLoop: retorno não-true (ou erro) respeita o intervalo', async () => {
  let calls = 0;
  const loop = new PollLoop(
    async () => {
      calls++;
      if (calls === 1) throw new Error('falha');
      return { published: 25 };
    },
    60_000,
    silent,
  );
  loop.start();
  await Bun.sleep(100);
  await loop.stop();
  expect(calls).toBe(1);
});
