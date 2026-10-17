// Teste de carga sob demanda (`bun run test:load`): instâncias reais `bun src/main.ts`, Postgres e LocalStack.
// Laço fechado: CONCURRENCY workers virtuais, cada um com no máximo uma requisição em voo.
// Configuração por env ou flags (`--duration-s=30` equivale a DURATION_S=30). Ver LOAD_TEST.md.
import Decimal from 'decimal.js';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { cpus, totalmem } from 'node:os';
import { resolve } from 'node:path';
import { OpenWalletUseCase } from '../../src/application/open-wallet';
import { createOrm, type Orm } from '../../src/infrastructure/mikro-orm/orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/mikro-orm/unit-of-work';
import { Sqs } from '../../src/infrastructure/sqs/sqs.client';
import { CONSUMER_NAME } from '../../src/infrastructure/sqs/wager-consumer';
import { outboxBacklog } from '../../src/infrastructure/workers/outbox-publisher';
import { eventually, withCluster, type Instance } from '../support/cluster';
import { assertWalletConsistent } from '../support/invariants';
import { queueCounts, queueEnv, sendBatch, toMessage, withQueues, type Msg } from '../support/sqs';

for (const a of process.argv.slice(2)) {
  const m = /^--([\w-]+)=(.*)$/.exec(a);
  if (m) process.env[m[1]!.toUpperCase().replace(/-/g, '_')] = m[2]!;
}
const E = process.env;
const num = (k: string, d: number) => (E[k] ? Number(E[k]) : d);
const CFG = {
  scenarios: (E.SCENARIOS ?? 'A,B,B_LOW,C').split(','),
  instances: (E.INSTANCES ?? '1,3').split(',').map(Number),
  repeat: num('REPEAT', 1),
  durationS: num('DURATION_S', 15),
  warmupS: num('WARMUP_S', 3),
  concurrency: num('CONCURRENCY', 32),
  wallets: num('WALLETS', 500),
  replayPct: num('REPLAY_PCT', 5),
  lowBalance: num('LOW_BALANCE', 100),
  sqsMessages: num('SQS_MESSAGES', 2000),
  sqsWallets: num('SQS_WALLETS', 100),
  drainTimeoutS: num('DRAIN_TIMEOUT_S', 60),
  requestTimeoutMs: num('REQUEST_TIMEOUT_MS', 10_000),
  // só os defaults que o teste fixa; o resto do ambiente (LOG_LEVEL, OUTBOX_POLL_MS...) é herdado
  // RUN_WORKERS=0 mede só a API (sem publisher: a outbox não drena; não use com o cenário C)
  appEnv: { RUN_WORKERS: E.RUN_WORKERS ?? '1', DB_POOL_MAX: E.DB_POOL_MAX ?? '10' },
};
const HIGH = '1000000.00';

const uid = () => crypto.randomUUID();
const now = () => performance.now();
const round = (x: number, d = 1) => (Number.isFinite(x) ? Number(x.toFixed(d)) : null);

let orm: Orm;
let sqs: Sqs;
const q = (sql: string, p: unknown[] = []) => orm.em.fork().getConnection().execute(sql, p) as Promise<any[]>;

// ---------- métricas do cliente ----------
type Cat = '200' | '202' | '422' | '409' | '4xx' | '5xx' | 'net';
const CATS: Cat[] = ['200', '202', '422', '409', '4xx', '5xx', 'net'];
const catOf = (s: number): Cat =>
  s === 0 ? 'net' : s >= 500 ? '5xx' : s === 200 ? '200' : s === 202 ? '202' : s === 422 ? '422' : s === 409 ? '409' : '4xx';

/** Percentil por posto mais próximo sobre as amostras ordenadas (exato, sem histograma). */
const pct = (sorted: number[], p: number) => (sorted.length ? sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)]! : NaN);
const latSummary = (xs: number[]) => {
  const s = Float64Array.from(xs).sort();
  const a = Array.from(s);
  return { n: a.length, p50: round(pct(a, 0.5), 2), p95: round(pct(a, 0.95), 2), p99: round(pct(a, 0.99), 2), max: round(a.at(-1) ?? NaN, 2) };
};

class Stats {
  counts = Object.fromEntries(CATS.map((c) => [c, 0])) as Record<Cat, number>;
  all: number[] = [];
  byLabel = new Map<string, number[]>();
  last = 0;
  add(label: string, cat: Cat, ms: number, end: number) {
    this.counts[cat]++;
    this.all.push(ms);
    (this.byLabel.get(label) ?? this.byLabel.set(label, []).get(label)!).push(ms);
    this.last = Math.max(this.last, end);
  }
}

type Done = { key: string; body: string; status: number; json: any };
type Req = { key: string; body: string; label: string; replayOf?: Done; onDone?: (status: number, json: any) => void };

/** Contabilidade de tudo que o cliente enviou (inclusive aquecimento), para conferir com o banco. */
class ClientLedger {
  ids = new Map<string, number>(); // transactionId -> HTTP da primeira resposta
  indeterminate = 0; // 5xx/rede: pode ou não ter commitado
  replays = 0;
  problems: string[] = [];
  record(r: Req, status: number, json: any) {
    if (status === 0 || status >= 500) {
      this.indeterminate++;
      return;
    }
    if (r.replayOf) {
      this.replays++;
      const o = r.replayOf;
      if (status !== o.status || json?.transactionId !== o.json.transactionId || json?.idempotentReplay !== true)
        this.problems.push(`replay divergente: ${o.status}/${o.json.transactionId} -> ${status} ${JSON.stringify(json).slice(0, 200)}`);
      return;
    }
    if ((status === 200 || status === 202 || status === 422) && json?.idempotentReplay === false) {
      if (json.transactionId) this.ids.set(json.transactionId, status);
      return;
    }
    if (this.problems.length < 50) this.problems.push(`resposta inesperada ${status}: ${JSON.stringify(json).slice(0, 200)}`);
  }
}

async function send(inst: Instance, r: Req): Promise<{ status: number; json: any }> {
  try {
    const res = await fetch(`${inst.base}/wagering/transactions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': r.key },
      body: r.body,
      signal: AbortSignal.timeout(CFG.requestTimeoutMs),
    });
    return { status: res.status, json: await res.json().catch(() => null) };
  } catch (e) {
    const err = e as Error & { code?: string };
    return { status: 0, json: { error: err.name, code: err.code, message: err.message?.slice(0, 120) } };
  }
}

/** Laço fechado com aquecimento descartado; round-robin entre instâncias. */
async function closedLoop(insts: Instance[], next: () => Req, ledger: ClientLedger) {
  const stats = new Stats();
  const firstErrors: string[] = [];
  let rr = 0;
  const from = now() + CFG.warmupS * 1000;
  const until = from + CFG.durationS * 1000;
  await Promise.all(
    Array.from({ length: CFG.concurrency }, async () => {
      while (now() < until) {
        const r = next();
        const s = now();
        const { status, json } = await send(insts[rr++ % insts.length]!, r);
        const e = now();
        ledger.record(r, status, json);
        r.onDone?.(status, json);
        if ((status === 0 || status >= 500) && firstErrors.length < 5) firstErrors.push(`${status} ${JSON.stringify(json).slice(0, 200)}`);
        if (s >= from) stats.add(r.label, catOf(status), e - s, e);
      }
    }),
  );
  const seconds = (Math.max(stats.last, until) - from) / 1000;
  const total = stats.all.length;
  const errors = stats.counts['5xx'] + stats.counts.net;
  return {
    seconds: round(seconds, 2),
    requests: total,
    rps: round(total / seconds),
    rpsByResult: Object.fromEntries(CATS.map((c) => [c, round(stats.counts[c] / seconds)])),
    counts: stats.counts,
    errorRatePct: round((100 * errors) / Math.max(1, total), 3),
    businessRejectPct: round((100 * stats.counts['422']) / Math.max(1, total), 2),
    latencyMs: latSummary(stats.all),
    latencyByLabelMs: Object.fromEntries([...stats.byLabel].map(([k, v]) => [k, latSummary(v)])),
    firstErrors,
  };
}

// ---------- /metrics ----------
type Sample = { name: string; labels: string; value: number };
const parseProm = (txt: string): Sample[] =>
  txt.split('\n').flatMap((l) => {
    const m = /^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+(\S+)$/.exec(l);
    return m ? [{ name: m[1]!, labels: m[2] ?? '', value: Number(m[3]) }] : [];
  });
const msum = (s: Sample[], name: string, has = '') => s.filter((x) => x.name === name && x.labels.includes(has)).reduce((a, x) => a + x.value, 0);
const scrape = async (i: Instance) =>
  parseProm(await (await fetch(`${i.base}/metrics`, { signal: AbortSignal.timeout(5_000) })).text());
const scrapeAll = (insts: Instance[]) => Promise.all(insts.map(scrape));

/** Quantil aproximado (limite superior do bucket) do histograma do servidor, a partir dos deltas. */
function histQuantile(before: Sample[][], after: Sample[][], q: number) {
  const buckets = new Map<number, number>();
  after.forEach((a, i) =>
    a
      .filter((x) => x.name === 'wager_processing_duration_seconds_bucket' && x.labels.includes('source="http"'))
      .forEach((x) => {
        const le = Number(/le="([^"]+)"/.exec(x.labels)![1]);
        const b = before[i]!.find((y) => y.name === x.name && y.labels === x.labels)?.value ?? 0;
        buckets.set(le, (buckets.get(le) ?? 0) + x.value - b);
      }),
  );
  const sorted = [...buckets].sort((a, b) => a[0] - b[0]);
  const total = sorted.at(-1)?.[1] ?? 0;
  const hit = sorted.find(([, c]) => c >= q * total);
  return hit ? (hit[0] === Infinity ? '>5000' : `<=${hit[0] * 1000}`) : null;
}

function serverDeltas(before: Sample[][], after: Sample[][], seconds: number) {
  const d = (name: string, has = '') => after.reduce((a, s, i) => a + msum(s, name, has) - msum(before[i]!, name, has), 0);
  const durCount = d('wager_processing_duration_seconds_count', 'source="http"');
  return {
    lockConflicts: d('wager_lock_conflicts_total'),
    lockConflictsByCode: Object.fromEntries(
      [...new Set(after.flat().filter((x) => x.name === 'wager_lock_conflicts_total').map((x) => x.labels))].map((l) => [l, d('wager_lock_conflicts_total', l)]),
    ),
    uowRetries: d('wager_retries_total', 'component="unit_of_work"'),
    httpReplaysServer: d('wager_duplicates_total', 'source="http"'),
    outboxPublished: d('wager_outbox_published_total'),
    serviceTimeMeanMs: round((1000 * d('wager_processing_duration_seconds_sum', 'source="http"')) / Math.max(1, durCount), 2),
    serviceTimeP50Ms: histQuantile(before, after, 0.5),
    serviceTimeP99Ms: histQuantile(before, after, 0.99),
    cpuCoresPerInstance: after.map((s, i) => round((msum(s, 'process_cpu_seconds_total') - msum(before[i]!, 'process_cpu_seconds_total')) / seconds, 2)),
    rssMbPerInstance: after.map((s) => round(msum(s, 'process_resident_memory_bytes') / 2 ** 20, 0)),
  };
}

/** Amostra lag/pendentes da outbox a cada 1s via /metrics (o gauge é global: basta uma instância). */
function outboxSampler(inst: Instance) {
  const samples: { t: number; phase: string; lag: number; pending: number }[] = [];
  let phase = 'load';
  let stop = false;
  const t0 = now();
  const loop = (async () => {
    while (!stop) {
      const s = await scrape(inst).catch(() => null);
      if (s) samples.push({ t: round((now() - t0) / 1000)!, phase, lag: msum(s, 'wager_outbox_lag_seconds'), pending: msum(s, 'wager_outbox_pending') });
      await Bun.sleep(1000);
    }
  })();
  return {
    samples,
    drainPhase: () => (phase = 'drain'),
    stop: async () => {
      stop = true;
      await loop;
    },
  };
}

/** Espera a outbox zerar depois da carga; devolve segundos (null se estourou o prazo) e o que sobrou. */
async function waitOutboxDrain() {
  const t0 = now();
  for (;;) {
    const b = await outboxBacklog(orm);
    if (b.pending === 0) return { drainS: round((now() - t0) / 1000, 2), pendingLeft: 0 };
    if (now() - t0 > CFG.drainTimeoutS * 1000) return { drainS: null, pendingLeft: b.pending };
    await Bun.sleep(250);
  }
}

// ---------- CPU do host e dos containers ----------
const hostCpu = () => {
  const v = readFileSync('/proc/stat', 'utf8').split('\n')[0]!.trim().split(/\s+/).slice(1).map(Number);
  return { idle: v[3]! + v[4]!, total: v.reduce((a, x) => a + x, 0) };
};
const containers = (() => {
  try {
    const names = Bun.spawnSync(['docker', 'ps', '--format', '{{.Names}}']).stdout.toString().split('\n');
    return { postgres: names.find((n) => n.includes('postgres')), localstack: names.find((n) => n.includes('localstack')) };
  } catch {
    return {};
  }
})();
const containerCpuUs = (name?: string) => {
  if (!name) return NaN;
  const out = Bun.spawnSync(['docker', 'exec', name, 'cat', '/sys/fs/cgroup/cpu.stat']).stdout.toString();
  return Number(/usage_usec (\d+)/.exec(out)?.[1] ?? NaN);
};
function cpuProbe() {
  const h = hostCpu();
  const pg = containerCpuUs(containers.postgres);
  const ls = containerCpuUs(containers.localstack);
  const me = process.cpuUsage();
  const t = now();
  return () => {
    const h2 = hostCpu();
    const s = (now() - t) / 1000;
    const mine = process.cpuUsage(me);
    return {
      hostBusyPct: round(100 * (1 - (h2.idle - h.idle) / (h2.total - h.total))),
      postgresCores: round((containerCpuUs(containers.postgres) - pg) / 1e6 / s, 2),
      localstackCores: round((containerCpuUs(containers.localstack) - ls) / 1e6 / s, 2),
      loadGeneratorCores: round((mine.user + mine.system) / 1e6 / s, 2),
    };
  };
}

// ---------- setup e verificação ----------
type Bet = { ext: string; round: string; amount: string };
type W = { id: string; playerId: string; bets: Bet[]; refundable: Bet[] };

async function openWallets(n: number, balance: string): Promise<W[]> {
  const open = new OpenWalletUseCase(new MikroOrmUnitOfWork(orm));
  const out: W[] = [];
  for (let i = 0; i < n; i += 50) {
    const chunk = await Promise.all(
      Array.from({ length: Math.min(50, n - i) }, async () => {
        const playerId = uid();
        const { id } = await open.execute({ playerId, initialBalance: { amount: balance, currency: 'BRL' } });
        return { id, playerId, bets: [], refundable: [] } as W;
      }),
    );
    out.push(...chunk);
  }
  return out;
}

/** Marca como publicado o backlog anterior (aberturas, outras suítes) para o lag refletir só a carga. */
const settleOutbox = async () =>
  Number(
    (await q(`WITH u AS (UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL RETURNING 1) SELECT count(*)::int AS n FROM u`))[0]
      .n,
  );

function wagerBody(w: W, kind: string, amount: string, roundId: string, ref?: string) {
  const ext = `load-${uid()}`;
  const body = {
    providerId: 'load',
    externalTransactionId: ext,
    playerId: w.playerId,
    walletId: w.id,
    roundId,
    gameId: 'g-load',
    kind,
    money: { amount, currency: 'BRL' },
    ...(ref ? { referenceExternalTransactionId: ref } : {}),
  };
  return { ext, key: `load:${ext}`, body: JSON.stringify(body) };
}

async function verify(walletIds: string[], ledger: ClientLedger, extra?: (problems: string[]) => Promise<void>) {
  const problems = [...ledger.problems];
  for (const id of walletIds) {
    try {
      await assertWalletConsistent(orm, id);
    } catch (e) {
      problems.push(`assertWalletConsistent(${id}): ${(e as Error).message.split('\n').slice(0, 3).join(' ')}`);
    }
  }
  const rows = await q(`SELECT id, status FROM wager_transactions WHERE wallet_id IN (?) AND kind <> 'OPENING'`, [walletIds]);
  const db = new Map(rows.map((r) => [r.id as string, r.status as string]));
  const okStatus: Record<number, string[]> = { 200: ['PROCESSED'], 422: ['REJECTED'], 202: ['PENDING_REFERENCE', 'PROCESSED', 'REJECTED'] };
  let missing = 0;
  for (const [id, http] of ledger.ids) {
    const st = db.get(id);
    if (!st || !okStatus[http]!.includes(st)) missing++;
  }
  if (missing) problems.push(`${missing} respostas do cliente sem a transação correspondente (ou com status divergente) no banco`);
  const extras = db.size - ledger.ids.size;
  if (extras < 0 || extras > ledger.indeterminate)
    problems.push(`banco tem ${db.size} transações, cliente confirmou ${ledger.ids.size} (+${ledger.indeterminate} indeterminadas)`);
  const dupKeys = await q(
    `SELECT idempotency_key FROM wager_transactions WHERE wallet_id IN (?) GROUP BY idempotency_key HAVING count(*) > 1`,
    [walletIds],
  );
  if (dupKeys.length) problems.push(`${dupKeys.length} idempotency keys duplicadas`);
  const dupLedger = await q(
    `SELECT transaction_id FROM wallet_ledger_entries WHERE wallet_id IN (?) GROUP BY transaction_id HAVING count(*) > 1`,
    [walletIds],
  );
  if (dupLedger.length) problems.push(`${dupLedger.length} transações com mais de um lançamento`);
  const [neg] = await q(
    `SELECT (SELECT count(*)::int FROM wallet_ledger_entries WHERE wallet_id IN (?) AND (balance_after < 0 OR balance_before < 0)) AS l,
            (SELECT count(*)::int FROM wallets WHERE id IN (?) AND balance < 0) AS w`,
    [walletIds, walletIds],
  );
  if (neg.l || neg.w) problems.push(`saldo negativo: ${neg.l} lançamentos, ${neg.w} wallets`);
  await extra?.(problems);
  const byStatus: Record<string, number> = {};
  for (const s of db.values()) byStatus[s] = (byStatus[s] ?? 0) + 1;
  return {
    ok: problems.length === 0,
    walletsChecked: walletIds.length,
    dbTransactions: db.size,
    dbByStatus: byStatus,
    clientConfirmed: ledger.ids.size,
    clientIndeterminate: ledger.indeterminate,
    replaysChecked: ledger.replays,
    problems: problems.slice(0, 30),
  };
}

// ---------- cenários ----------
type Gen = { wallets: W[]; next: () => Req; check?: (problems: string[]) => Promise<void> };

/** A: muitas wallets, mix 60/25/10/5 (BET/WIN/LOSS/REFUND) + REPLAY_PCT% de reenvios deliberados. */
async function spread(): Promise<Gen> {
  const wallets = await openWallets(CFG.wallets, HIGH);
  const recent: Done[] = [];
  const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)]!;
  const remember = (r: { key: string; body: string }) => (status: number, json: any) => {
    if (status !== 200 && status !== 422) return;
    recent.push({ ...r, status, json });
    if (recent.length > 2000) recent.splice(0, 1000);
  };
  const next = (): Req => {
    if (recent.length && Math.random() * 100 < CFG.replayPct) {
      const o = pick(recent);
      return { key: o.key, body: o.body, label: 'REPLAY', replayOf: o };
    }
    const w = pick(wallets);
    const r = Math.random() * 100;
    if (r >= 60 && w.bets.length) {
      if (r < 85 || (r >= 95 && !w.refundable.length)) {
        const b = pick(w.bets);
        const x = wagerBody(w, 'WIN', (0.01 + Math.random() * 100).toFixed(2), b.round, b.ext);
        return { ...x, label: 'WIN', onDone: remember(x) };
      }
      if (r < 95) {
        const b = pick(w.bets);
        const x = wagerBody(w, 'LOSS', b.amount, b.round, b.ext);
        return { ...x, label: 'LOSS', onDone: remember(x) };
      }
      // REFUND: tira a BET da lista antes de enviar (uma reversão por BET)
      const b = w.refundable.splice(Math.floor(Math.random() * w.refundable.length), 1)[0]!;
      const x = wagerBody(w, 'REFUND', b.amount, b.round, b.ext);
      return { ...x, label: 'REFUND', onDone: remember(x) };
    }
    const bet: Bet = { ext: '', round: uid(), amount: (1 + Math.random() * 49).toFixed(2) };
    const x = wagerBody(w, 'BET', bet.amount, bet.round);
    bet.ext = x.ext;
    return {
      ...x,
      label: 'BET',
      onDone: (status, json) => {
        remember(x)(status, json);
        if (status !== 200) return;
        w.bets.push(bet);
        w.refundable.push(bet);
        if (w.bets.length > 20) w.bets.shift();
        if (w.refundable.length > 20) w.refundable.shift();
      },
    };
  };
  return { wallets, next };
}

/** B: toda a carga em uma wallet, BETs de 1.00. Saldo alto (sem rejeição) ou baixo (INSUFFICIENT_FUNDS sob contenção). */
async function hot(balance: string): Promise<Gen> {
  const wallets = await openWallets(1, balance);
  const w = wallets[0]!;
  const next = (): Req => ({ ...wagerBody(w, 'BET', '1.00', 'hot'), label: 'BET' });
  const check = async (problems: string[]) => {
    const [r] = await q(
      `SELECT w.balance::text AS balance, (SELECT count(*)::int FROM wager_transactions t WHERE t.wallet_id = w.id AND t.kind = 'BET' AND t.status = 'PROCESSED') AS n
         FROM wallets w WHERE w.id = ?`,
      [w.id],
    );
    const expected = new Decimal(balance).minus(r.n);
    if (!new Decimal(r.balance).eq(expected)) problems.push(`hot wallet: saldo ${r.balance}, esperado ${expected.toFixed(2)} (${r.n} BETs)`);
    if (new Decimal(balance).lt(1000) && r.n !== Number(balance)) problems.push(`hot wallet (saldo baixo): ${r.n} BETs processadas, esperado ${balance}`);
  };
  return { wallets, next, check };
}

async function httpScenario(name: string, instances: number, build: () => Promise<Gen>) {
  return withQueues(sqs, {}, (queues) =>
    withCluster(
      { ...CFG.appEnv, ...queueEnv(queues) },
      async (c) => {
        const insts = await c.spawnMany(instances);
        const gen = await build();
        const settled = await settleOutbox();
        const ledger = new ClientLedger();
        const sampler = outboxSampler(insts[0]!);
        const before = await scrapeAll(insts);
        const cpu = cpuProbe();
        const load = await closedLoop(insts, gen.next, ledger);
        const resources = cpu();
        const after = await scrapeAll(insts);
        const atEnd = sampler.samples.at(-1);
        sampler.drainPhase();
        const drain = await waitOutboxDrain();
        await sampler.stop();
        const during = sampler.samples.filter((s) => s.phase === 'load');
        const verification = await verify(
          gen.wallets.map((w) => w.id),
          ledger,
          gen.check,
        );
        const deaths = insts.filter((i) => !i.alive).map((i) => i.name);
        if (deaths.length) verification.problems.push(`instâncias morreram: ${deaths.join(',')}`);
        if (deaths.length) verification.ok = false;
        return {
          scenario: name,
          instances,
          concurrency: CFG.concurrency,
          load,
          server: serverDeltas(before, after, load.seconds ?? CFG.durationS),
          resources,
          outbox: {
            settledBefore: settled,
            lagMaxS: round(Math.max(0, ...during.map((s) => s.lag)), 2),
            lagAtEndS: round(atEnd?.lag ?? NaN, 2),
            pendingMax: Math.max(0, ...during.map((s) => s.pending)),
            pendingAtEnd: atEnd?.pending ?? null,
            ...drain,
          },
          verification,
        };
      },
      300,
    ),
  );
}

/** C: enfileira N mensagens (BET 1.00, grupo = wallet) e mede a drenagem pelas instâncias. */
async function sqsScenario(instances: number) {
  return withQueues(sqs, { visibility: 30 }, async (queues) => {
    const wallets = await openWallets(CFG.sqsWallets, HIGH);
    const msgs: Msg[] = Array.from({ length: CFG.sqsMessages }, (_, i) => {
      const w = wallets[i % wallets.length]!;
      const x = wagerBody(w, 'BET', '1.00', 'sqs');
      return toMessage({ idempotencyKey: x.key, ...JSON.parse(x.body) });
    });
    await settleOutbox();
    const tEnq = now();
    const chunks = Array.from({ length: Math.ceil(msgs.length / 100) }, (_, i) => msgs.slice(i * 100, i * 100 + 100));
    for (let i = 0; i < chunks.length; i += 10)
      await Promise.all(chunks.slice(i, i + 10).map((ch) => sendBatch(sqs, queues.qs.url, ch.map((m) => ({ body: m, group: m.data.walletId })))));
    const enqueueS = (now() - tEnq) / 1000;
    const ids = wallets.map((w) => w.id);
    const processed = async () =>
      Number((await q(`SELECT count(*)::int AS n FROM wager_transactions WHERE wallet_id IN (?) AND kind = 'BET'`, [ids]))[0].n);

    return withCluster(
      { ...CFG.appEnv, ...queueEnv(queues) },
      async (c) => {
        const cpu = cpuProbe();
        const tSpawn = now();
        const insts = await c.spawnMany(instances);
        const sampler = outboxSampler(insts[0]!);
        await eventually(async () => (await processed()) >= msgs.length, { timeout: 300_000, interval: 250 });
        const wallS = (now() - tSpawn) / 1000;
        const resources = cpu();
        const after = await scrapeAll(insts);
        await eventually(async () => {
          const k = await queueCounts(sqs, queues.qs.url);
          return k.visible + k.inFlight === 0;
        }, { timeout: 30_000, interval: 250 });
        const [span] = await q(
          `SELECT count(*)::int AS n, EXTRACT(EPOCH FROM max(processed_at) - min(processed_at)) AS s
             FROM inbox_messages WHERE consumer_name = ? AND message_id IN (?) AND processed_at IS NOT NULL`,
          [CONSUMER_NAME, msgs.map((m) => m.messageId)],
        );
        const during = [...sampler.samples];
        sampler.drainPhase();
        const drain = await waitOutboxDrain();
        await sampler.stop();

        const ledger = new ClientLedger(); // sem cliente HTTP: confere contagens diretamente
        const verification = await verify(ids, ledger, async (problems) => {
          if (span.n !== msgs.length) problems.push(`inbox com ${span.n} mensagens processadas, esperado ${msgs.length}`);
          const per = await q(`SELECT wallet_id, count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id IN (?) AND direction = 'DEBIT' GROUP BY 1`, [ids]);
          const expected = new Map<string, number>();
          for (const m of msgs) expected.set(m.data.walletId, (expected.get(m.data.walletId) ?? 0) + 1);
          for (const r of per) if (expected.get(r.wallet_id) !== r.n) problems.push(`wallet ${r.wallet_id}: ${r.n} débitos, esperado ${expected.get(r.wallet_id)}`);
          const dlq = await queueCounts(sqs, queues.qs.dlqUrl);
          if (dlq.visible + dlq.inFlight) problems.push(`DLQ com ${dlq.visible + dlq.inFlight} mensagens`);
        });
        // aqui o banco tem todas as transações e o cliente HTTP nenhuma: a regra "banco ⊆ cliente + indeterminadas" não se aplica
        verification.problems = verification.problems.filter((p) => !p.startsWith('banco tem'));
        verification.ok = verification.problems.length === 0;
        const spanS = Number(span.s);
        const zero = after.map(() => [] as Sample[]); // processos novos: contadores partem de 0
        return {
          scenario: 'C',
          instances,
          messages: msgs.length,
          wallets: wallets.length,
          enqueue: { seconds: round(enqueueS, 2), msgsPerS: round(msgs.length / enqueueS) },
          consume: {
            wallFromSpawnS: round(wallS, 2),
            dbSpanS: round(spanS, 2),
            msgsPerSDbSpan: round(msgs.length / spanS),
            processedPerInstance: c.all.map((i) => i.logged('message processed')),
          },
          server: { ...serverDeltas(zero, after, wallS), sqsDuplicates: after.reduce((a, s) => a + msum(s, 'wager_duplicates_total', 'source="sqs"'), 0) },
          resources,
          outbox: {
            lagMaxS: round(Math.max(0, ...during.map((s) => s.lag)), 2),
            pendingMax: Math.max(0, ...during.map((s) => s.pending)),
            ...drain,
          },
          verification,
        };
      },
      5000, // conta 'message processed' nos logs: guarda bastante cauda
    );
  });
}

// ---------- execução ----------
async function environment() {
  const sh = (cmd: string[]) => {
    try {
      return Bun.spawnSync(cmd).stdout.toString().trim();
    } catch {
      return null;
    }
  };
  const [pg] = await q(`SELECT version() AS v, current_setting('max_connections') AS mc, current_setting('synchronous_commit') AS sc`);
  return {
    date: new Date().toISOString(),
    bun: Bun.version,
    cpu: cpus()[0]?.model,
    logicalCpus: cpus().length,
    memMb: Math.round(totalmem() / 2 ** 20),
    kernel: sh(['uname', '-r']),
    docker: sh(['docker', 'version', '--format', '{{.Server.Version}}']),
    postgres: pg.v,
    postgresMaxConnections: Number(pg.mc),
    postgresSynchronousCommit: pg.sc,
  };
}

function printRun(r: any) {
  if (r.scenario === 'C') {
    console.log(
      `  C inst=${r.instances} msgs=${r.messages} enfileirar=${r.enqueue.msgsPerS}/s consumo=${r.consume.msgsPerSDbSpan}/s (span ${r.consume.dbSpanS}s, parede ${r.consume.wallFromSpawnS}s) por instância=${r.consume.processedPerInstance.join('/')} ` +
        `conflitos=${r.server.lockConflicts} retries=${r.server.uowRetries} dup-sqs=${r.server.sqsDuplicates} outbox lagMax=${r.outbox.lagMaxS}s drenagem=${r.outbox.drainS ?? `>${CFG.drainTimeoutS}s (${r.outbox.pendingLeft} pendentes)`}s`,
    );
  } else {
    const l = r.load;
    console.log(
      `  ${r.scenario} inst=${r.instances} c=${r.concurrency} ${l.seconds}s req=${l.requests} rps=${l.rps} [${CATS.map((k) => `${k}:${l.counts[k]}`).join(' ')}] ` +
        `p50/p95/p99/max=${l.latencyMs.p50}/${l.latencyMs.p95}/${l.latencyMs.p99}/${l.latencyMs.max}ms erro=${l.errorRatePct}% 422=${l.businessRejectPct}% ` +
        `serviço(média)=${r.server.serviceTimeMeanMs}ms conflitos=${r.server.lockConflicts} retries=${r.server.uowRetries} ` +
        `outbox lagMax=${r.outbox.lagMaxS}s lagFim=${r.outbox.lagAtEndS}s drenagem=${r.outbox.drainS ?? `>${CFG.drainTimeoutS}s (${r.outbox.pendingLeft} pendentes)`}s ` +
        `cpu host=${r.resources.hostBusyPct}% pg=${r.resources.postgresCores} gerador=${r.resources.loadGeneratorCores} inst=${r.server.cpuCoresPerInstance.join('/')}`,
    );
  }
  console.log(`    verificação: ${r.verification.ok ? 'OK' : 'FALHOU'} (${r.verification.walletsChecked} wallets, ${r.verification.dbTransactions} transações no banco, ${r.verification.clientConfirmed} confirmadas pelo cliente, ${r.verification.clientIndeterminate} indeterminadas, ${r.verification.replaysChecked} replays conferidos)`);
  for (const p of r.verification.problems) console.log(`    !! ${p}`);
  if (r.load?.firstErrors?.length) console.log(`    primeiros erros: ${r.load.firstErrors.join(' | ')}`);
}

async function main() {
  orm = await createOrm();
  sqs = new Sqs();
  const results: any[] = [];
  try {
    const env = await environment();
    console.log(`ambiente: ${JSON.stringify(env)}\nconfig: ${JSON.stringify(CFG)}`);
    const plan: Record<string, (n: number) => Promise<any>> = {
      A: (n) => httpScenario('A', n, spread),
      B: (n) => httpScenario('B', n, () => hot('10000000.00')),
      B_LOW: (n) => httpScenario('B_LOW', n, () => hot(CFG.lowBalance.toFixed(2))),
      C: (n) => sqsScenario(n),
    };
    for (const s of CFG.scenarios) {
      for (const n of CFG.instances) {
        for (let rep = 1; rep <= CFG.repeat; rep++) {
          console.log(`\n>> cenário ${s}, ${n} instância(s), execução ${rep}/${CFG.repeat}`);
          const r = { ...(await plan[s]!(n)), rep };
          results.push(r);
          printRun(r);
        }
      }
    }
    const dir = resolve(import.meta.dir, '../../load-results');
    mkdirSync(dir, { recursive: true });
    const file = resolve(dir, `load-${env.date.replace(/[:.]/g, '-')}.json`);
    writeFileSync(file, JSON.stringify({ environment: env, config: CFG, results }, null, 2));
    console.log(`\nresumo:`);
    for (const r of results) printRun(r);
    console.log(`\nresultados completos: ${file}`);
  } finally {
    sqs.client.destroy();
    await orm.close();
  }
  const failed = results.filter((r) => !r.verification.ok);
  if (failed.length) {
    console.error(`\nFALHA DE CORREÇÃO em ${failed.length} execução(ões): ${failed.map((r) => `${r.scenario}/inst=${r.instances}/rep=${r.rep}`).join(', ')}`);
    process.exit(1);
  }
}

process.on('SIGINT', () => process.exit(130)); // o handler de exit do cluster mata os filhos
await main();
