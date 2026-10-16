import type { Subprocess } from 'bun';
import { resolve } from 'node:path';

const ROOT = resolve(import.meta.dir, '../..');

/** Polling com prazo: repete `fn` até não lançar (ou devolver truthy). Sem sleeps cegos. */
export async function eventually<T>(fn: () => Promise<T> | T, { timeout = 15_000, interval = 100 } = {}): Promise<T> {
  const deadline = Date.now() + timeout;
  for (;;) {
    try {
      const v = await fn();
      if (v !== false) return v;
      if (Date.now() >= deadline) throw new Error('eventually: condição não satisfeita no prazo');
    } catch (e) {
      if (Date.now() >= deadline) throw e;
    }
    await Bun.sleep(interval);
  }
}

/** Promessa com prazo (evita esperar para sempre por um processo que não sai). */
export const within = <T>(p: Promise<T>, ms: number, what = 'operação'): Promise<T> =>
  Promise.race([p, Bun.sleep(ms).then(() => Promise.reject(new Error(`${what}: prazo de ${ms}ms excedido`)))]);

// Rede de segurança: se o runner sair sem passar pelo finally, nenhum filho fica órfão.
const live = new Set<Subprocess>();
process.on('exit', () => {
  for (const p of live) p.kill('SIGKILL');
});

function freePort(): number {
  const s = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = s.port!;
  s.stop(true);
  return port;
}

async function pump(stream: ReadableStream<Uint8Array>, sink: string[]) {
  const dec = new TextDecoder();
  let buf = '';
  for await (const chunk of stream) {
    buf += dec.decode(chunk, { stream: true });
    for (let i = buf.indexOf('\n'); i >= 0; i = buf.indexOf('\n')) {
      sink.push(buf.slice(0, i));
      buf = buf.slice(i + 1);
    }
  }
  if (buf) sink.push(buf);
}

/** Processo real `bun src/main.ts` com stdout/stderr capturados. */
export class Instance {
  readonly logs: string[] = [];
  readonly exited: Promise<number | null>;

  constructor(
    readonly name: string,
    readonly port: number,
    readonly proc: Subprocess<'ignore', 'pipe', 'pipe'>,
  ) {
    live.add(proc);
    const streams = Promise.all([pump(proc.stdout, this.logs), pump(proc.stderr, this.logs)]);
    // código de saída depois de drenar a saída (diagnóstico completo)
    this.exited = proc.exited.then(async (code) => {
      live.delete(proc);
      await streams.catch(() => undefined);
      return proc.signalCode ? null : code;
    });
  }

  get base() {
    return `http://127.0.0.1:${this.port}`;
  }

  get alive() {
    return live.has(this.proc);
  }

  kill(signal: 'SIGKILL' | 'SIGTERM' = 'SIGKILL') {
    this.proc.kill(signal);
  }

  /** Linhas de log JSON cuja `msg` começa com o texto dado (ex.: 'duplicate message'). */
  logged(msg: string) {
    return this.logs.filter((l) => l.includes(`"msg":"${msg}`)).length;
  }
}

/** Grupo de instâncias de um teste; `shutdown` mata todas (inclusive em falha). */
export class Cluster {
  readonly all: Instance[] = [];
  private seq = 0;

  constructor(private readonly env: Record<string, string> = {}) {}

  /** Instâncias vivas, na ordem de criação. */
  get alive() {
    return this.all.filter((i) => i.alive);
  }

  async spawn(env: Record<string, string> = {}, readyTimeoutMs = 30_000): Promise<Instance> {
    const name = `i${++this.seq}`;
    for (let attempt = 1; ; attempt++) {
      const port = freePort();
      const proc = Bun.spawn(['bun', 'src/main.ts'], {
        cwd: ROOT,
        env: { ...process.env, ...this.env, ...env, PORT: String(port) },
        stdin: 'ignore',
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const inst = new Instance(name, port, proc);
      this.all.push(inst);
      try {
        await this.waitReady(inst, readyTimeoutMs);
        return inst;
      } catch (e) {
        inst.kill();
        await inst.exited;
        // porta tomada entre o freePort e o listen: tenta outra
        if (attempt < 3 && inst.logs.some((l) => l.includes('EADDRINUSE'))) continue;
        throw new Error(`${name} não ficou pronta: ${(e as Error).message}\n${inst.logs.slice(-40).join('\n')}`);
      }
    }
  }

  spawnMany(n: number, env: Record<string, string> = {}) {
    return Promise.all(Array.from({ length: n }, () => this.spawn(env)));
  }

  private async waitReady(inst: Instance, timeoutMs: number) {
    let exited = false;
    void inst.exited.then(() => (exited = true));
    await eventually(
      async () => {
        if (exited) throw new Error('processo saiu antes de ficar pronto');
        const res = await fetch(`${inst.base}/health/ready`, { signal: AbortSignal.timeout(2_000) });
        if (res.status !== 200) throw new Error(`ready ${res.status}`);
        return true;
      },
      { timeout: timeoutMs, interval: 100 },
    ).catch((e) => {
      throw exited ? new Error('processo saiu antes de ficar pronto') : e;
    });
  }

  dump(lines = 60) {
    for (const i of this.all) {
      console.error(`----- ${i.name} (porta ${i.port}, ${i.alive ? 'viva' : 'morta'}) -----\n${i.logs.slice(-lines).join('\n')}`);
    }
  }

  async shutdown() {
    for (const i of this.all) if (i.alive) i.kill('SIGKILL');
    await Promise.all(this.all.map((i) => i.exited));
  }
}

/** Roda o cenário com um cluster novo; em falha imprime os logs; sempre mata tudo. */
export async function withCluster<T>(env: Record<string, string>, fn: (c: Cluster) => Promise<T>): Promise<T> {
  const c = new Cluster(env);
  try {
    return await fn(c);
  } catch (e) {
    c.dump();
    throw e;
  } finally {
    await c.shutdown();
  }
}
