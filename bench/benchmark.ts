import { Database } from "bun:sqlite";
import { AsyncDatabase } from "../src/async-database";
import { AsyncDatabasePool } from "../src/async-database-pool";

const DB_FILE = "/tmp/bun-async-sqlite-bench.db";
const ROWS = 50_000;
const OPS = 100_000;
const PARALLEL_OPS = 40_000;
const CONCURRENCY = 200;

function nowMs() { return performance.now(); }

function rate(ops: number, ms: number) {
  return `${((ops / ms) * 1000).toFixed(0)} ops/sec`;
}

async function runMulticoreProbe(cores: number) {
  console.log("\nMulticore probe");
  console.log(`Detected logical cores: ${cores}`);

  const requestedThreads = [...new Set([1, 2, cores - 1, cores, cores * 2].filter((value) => value >= 1))];
  const results: Array<{ threads: number; kbPerSec: number }> = [];

  for (const threads of requestedThreads) {
    const command = `openssl speed -seconds 2 -multi ${threads} sha256`;
    try {
      const output = await Bun.$`bash -lc ${command}`.text();
      const line = output
        .split("\n")
        .map((entry) => entry.trim())
        .find((entry) => entry.startsWith("sha256"));

      if (!line) {
        console.log(`  threads=${threads}: unable to parse sha256 line`);
        continue;
      }

      const values = line
        .split(/\s+/)
        .slice(1)
        .map((token) => Number.parseFloat(token.replace(/k$/i, "")))
        .filter((value) => Number.isFinite(value));

      if (values.length === 0) {
        console.log(`  threads=${threads}: unable to parse throughput values`);
        continue;
      }

      results.push({ threads, kbPerSec: values[values.length - 1] });
    } catch (error) {
      console.log(`  threads=${threads}: probe unavailable (${error})`);
    }
  }

  if (results.length === 0) {
    return;
  }

  const baseline = results[0].kbPerSec;
  console.log("OpenSSL sha256 throughput (largest block size):");
  for (const result of results) {
    const multiplier = baseline === 0 ? 0 : result.kbPerSec / baseline;
    console.log(`  threads=${result.threads}: ${result.kbPerSec.toFixed(2)}kB/s (${multiplier.toFixed(2)}x)`);
  }
}

function setupDb() {
  const db = new Database(DB_FILE);
  db.exec("DROP TABLE IF EXISTS bench");
  db.exec("CREATE TABLE bench (id INTEGER PRIMARY KEY, value TEXT)");
  const insert = db.prepare("INSERT INTO bench (id, value) VALUES (?, ?)");
  db.exec("BEGIN");
  for (let i = 1; i <= ROWS; i++) insert.run(i, `value-${i}`);
  db.exec("COMMIT");
  insert.finalize();
  db.close();
}

async function benchSyncSequential() {
  const db = new Database(DB_FILE, { readonly: true });
  const stmt = db.query<{ value: string }, [number]>("SELECT value FROM bench WHERE id = ?");
  const start = nowMs();
  for (let i = 0; i < OPS; i++) stmt.get((i % ROWS) + 1);
  const elapsed = nowMs() - start;
  stmt.finalize();
  db.close();
  return elapsed;
}

async function benchAsyncSequential() {
  const db = new AsyncDatabase(DB_FILE, { readonly: true });
  const stmt = await db.query<{ value: string }, [number]>("SELECT value FROM bench WHERE id = ?");
  const start = nowMs();
  for (let i = 0; i < OPS; i++) await stmt.get((i % ROWS) + 1);
  const elapsed = nowMs() - start;
  await stmt.finalize();
  await db.close();
  return elapsed;
}

async function benchPoolSequential(workers: number) {
  const pool = new AsyncDatabasePool(workers, DB_FILE, { readonly: true });
  const stmts = await pool.mapWorkers((db) => db.query<{ value: string }, [number]>("SELECT value FROM bench WHERE id = ?"));
  const start = nowMs();
  for (let i = 0; i < OPS; i++) {
    const stmt = stmts[i % workers];
    await stmt.get((i % ROWS) + 1);
  }
  const elapsed = nowMs() - start;
  await Promise.all(stmts.map((stmt) => stmt.finalize()));
  await pool.close();
  return elapsed;
}

async function runParallelWith<T>(ops: number, concurrency: number, task: (index: number) => Promise<T>) {
  let next = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const idx = next++;
      if (idx >= ops) return;
      await task(idx);
    }
  });
  await Promise.all(workers);
}

async function benchAsyncParallelSingle() {
  const db = new AsyncDatabase(DB_FILE, { readonly: true });
  const stmt = await db.query<{ value: string }, [number]>("SELECT value FROM bench WHERE id = ?");
  const start = nowMs();
  await runParallelWith(PARALLEL_OPS, CONCURRENCY, (i) => stmt.get((i % ROWS) + 1));
  const elapsed = nowMs() - start;
  await stmt.finalize();
  await db.close();
  return elapsed;
}

async function benchAsyncParallelPool(workers: number) {
  const pool = new AsyncDatabasePool(workers, DB_FILE, { readonly: true });
  const stmts = await pool.mapWorkers((db) => db.query<{ value: string }, [number]>("SELECT value FROM bench WHERE id = ?"));
  const start = nowMs();
  await runParallelWith(PARALLEL_OPS, CONCURRENCY, (i) => {
    const stmt = stmts[i % workers];
    return stmt.get((i % ROWS) + 1);
  });
  const elapsed = nowMs() - start;
  await Promise.all(stmts.map((stmt) => stmt.finalize()));
  await pool.close();
  return elapsed;
}

// ─── Expensive-query benchmarks ────────────────────────────────────────────
//
// Cross-join forces O(N²) row-pair evaluations per query (SQLite nested-loop,
// returns a single scalar so result-transfer overhead is negligible).
// With N=700 each query takes ~20–30 ms, making IPC overhead irrelevant and
// exposing how well async workers parallelise truly compute-bound SQLite work.
//
// Tune HEAVY_N up/down if the single-query time is outside the 10–50 ms range
// on your hardware.

const HEAVY_N = 700;
const HEAVY_OPS = 40;
const HEAVY_SQL =
  "SELECT SUM(a.id * b.id % 1000007) FROM bench a CROSS JOIN bench b WHERE a.id <= ? AND b.id <= ?";

async function benchHeavySyncSeq() {
  const db = new Database(DB_FILE, { readonly: true });
  const stmt = db.query<{ n: number }, [number, number]>(HEAVY_SQL);
  const start = nowMs();
  for (let i = 0; i < HEAVY_OPS; i++) stmt.get(HEAVY_N, HEAVY_N);
  const elapsed = nowMs() - start;
  stmt.finalize();
  db.close();
  return elapsed;
}

async function benchHeavyAsyncSeq() {
  const db = new AsyncDatabase(DB_FILE, { readonly: true });
  const stmt = await db.query<{ n: number }, [number, number]>(HEAVY_SQL);
  const start = nowMs();
  for (let i = 0; i < HEAVY_OPS; i++) await stmt.get(HEAVY_N, HEAVY_N);
  const elapsed = nowMs() - start;
  await stmt.finalize();
  await db.close();
  return elapsed;
}

async function benchHeavyAsyncParallel(workers: number) {
  const pool = new AsyncDatabasePool(workers, DB_FILE, { readonly: true });
  const stmts = await pool.mapWorkers((db) =>
    db.query<{ n: number }, [number, number]>(HEAVY_SQL)
  );
  const start = nowMs();
  // Fire all HEAVY_OPS queries simultaneously; the pool distributes them across workers.
  await runParallelWith(HEAVY_OPS, HEAVY_OPS, (i) => stmts[i % workers].get(HEAVY_N, HEAVY_N));
  const elapsed = nowMs() - start;
  await Promise.all(stmts.map((s) => s.finalize()));
  await pool.close();
  return elapsed;
}

// ─── Main-thread overlap benchmark ─────────────────────────────────────────
//
// Demonstrates a unique advantage of the async API: the main thread stays
// free during query execution and can do useful CPU work in parallel.
//
//   sync  path: run query (blocks) → CPU burn        wall ≈ query_ms + burn_ms
//   async path: fire query (non-blocking) → CPU burn → await result
//               the worker runs in a separate OS thread, so both proceed
//               simultaneously.                       wall ≈ max(query_ms, burn_ms)
//
// We burn for ~75 % of the average query time.  The theoretical speedup is
// 1 + 0.75 = 1.75×; in practice it is close to that.

const OVERLAP_TRIALS = 15;

function cpuBurn(ms: number): number {
  const deadline = performance.now() + ms;
  let x = 0;
  while (performance.now() < deadline) x = (x + 1) & 0xffff;
  return x; // prevent dead-code elimination
}

async function benchOverlapSync(burnMs: number) {
  const db = new Database(DB_FILE, { readonly: true });
  const stmt = db.query<any, [number, number]>(HEAVY_SQL);
  stmt.get(HEAVY_N, HEAVY_N); // warm-up
  const start = nowMs();
  for (let i = 0; i < OVERLAP_TRIALS; i++) {
    stmt.get(HEAVY_N, HEAVY_N); // blocks the main thread
    cpuBurn(burnMs);             // CPU work must wait until query finishes
  }
  const elapsed = nowMs() - start;
  stmt.finalize();
  db.close();
  return elapsed;
}

async function benchOverlapAsync(burnMs: number) {
  const db = new AsyncDatabase(DB_FILE, { readonly: true });
  const stmt = await db.query<any, [number, number]>(HEAVY_SQL);
  await stmt.get(HEAVY_N, HEAVY_N); // warm-up
  const start = nowMs();
  for (let i = 0; i < OVERLAP_TRIALS; i++) {
    const pending = stmt.get(HEAVY_N, HEAVY_N); // dispatched to worker, returns immediately
    cpuBurn(burnMs);                             // runs while the worker executes the query
    await pending;
  }
  const elapsed = nowMs() - start;
  await stmt.finalize();
  await db.close();
  return elapsed;
}

async function main() {
  setupDb();
  const cores = navigator.hardwareConcurrency || 1;
  console.log(`Benchmark DB rows=${ROWS}, sequential ops=${OPS}, parallel ops=${PARALLEL_OPS}, concurrency=${CONCURRENCY}, cores=${cores}`);
  await runMulticoreProbe(cores);

  const syncSeq = await benchSyncSequential();
  const asyncSeq1 = await benchAsyncSequential();
  const asyncSeq2 = await benchPoolSequential(2);
  const asyncSeq4 = await benchPoolSequential(4);

  console.log("\nSequential (sync-like request pattern)");
  console.table([
    { mode: "sync Database", ms: syncSeq.toFixed(2), throughput: rate(OPS, syncSeq) },
    { mode: "async worker x1", ms: asyncSeq1.toFixed(2), throughput: rate(OPS, asyncSeq1) },
    { mode: "async worker x2", ms: asyncSeq2.toFixed(2), throughput: rate(OPS, asyncSeq2) },
    { mode: "async worker x4", ms: asyncSeq4.toFixed(2), throughput: rate(OPS, asyncSeq4) },
  ]);

  const asyncPar1 = await benchAsyncParallelSingle();
  const asyncPar2 = await benchAsyncParallelPool(2);
  const asyncPar4 = await benchAsyncParallelPool(4);

  console.log("\nParallelisable queries");
  console.table([
    { mode: "async worker x1", ms: asyncPar1.toFixed(2), throughput: rate(PARALLEL_OPS, asyncPar1) },
    { mode: "async worker x2", ms: asyncPar2.toFixed(2), throughput: rate(PARALLEL_OPS, asyncPar2) },
    { mode: "async worker x4", ms: asyncPar4.toFixed(2), throughput: rate(PARALLEL_OPS, asyncPar4) },
  ]);

  // ── Expensive queries ──────────────────────────────────────────────────────
  console.log(`\nExpensive queries — cross-join N=${HEAVY_N}, ops=${HEAVY_OPS} (all fired simultaneously in parallel modes)`);

  const heavySyncSeq    = await benchHeavySyncSeq();
  const heavyAsyncSeq   = await benchHeavyAsyncSeq();
  const heavyAsyncPar1  = await benchHeavyAsyncParallel(1);
  const heavyAsyncPar2  = await benchHeavyAsyncParallel(2);
  const heavyAsyncPar4  = await benchHeavyAsyncParallel(4);

  const avgQueryMs = heavySyncSeq / HEAVY_OPS;
  console.log(`Average single-query time (sync baseline): ${avgQueryMs.toFixed(1)} ms`);
  console.table([
    { mode: "sync sequential",              ms: heavySyncSeq.toFixed(1),   throughput: rate(HEAVY_OPS, heavySyncSeq) },
    { mode: "async x1 sequential",          ms: heavyAsyncSeq.toFixed(1),  throughput: rate(HEAVY_OPS, heavyAsyncSeq) },
    { mode: "async x1 parallel (all at once)", ms: heavyAsyncPar1.toFixed(1), throughput: rate(HEAVY_OPS, heavyAsyncPar1) },
    { mode: "async x2 parallel (all at once)", ms: heavyAsyncPar2.toFixed(1), throughput: rate(HEAVY_OPS, heavyAsyncPar2) },
    { mode: "async x4 parallel (all at once)", ms: heavyAsyncPar4.toFixed(1), throughput: rate(HEAVY_OPS, heavyAsyncPar4) },
  ]);

  // ── Main-thread overlap ────────────────────────────────────────────────────
  const burnMs = Math.max(5, Math.round(avgQueryMs * 0.75));
  console.log(`\nMain-thread overlap — ${OVERLAP_TRIALS} trials`);
  console.log(`Async fires the query (non-blocking) then immediately runs ${burnMs} ms of CPU work.`);
  console.log(`The worker executes the query in a separate OS thread, so both proceed in parallel.`);
  console.log(`Sync must wait for the query before any CPU work can start.`);

  const overlapSync  = await benchOverlapSync(burnMs);
  const overlapAsync = await benchOverlapAsync(burnMs);
  const speedup = overlapSync / overlapAsync;

  console.table([
    { mode: "sync  (query → CPU work, serial)",   ms: overlapSync.toFixed(1),  throughput: rate(OVERLAP_TRIALS, overlapSync) },
    { mode: "async (query ∥ CPU work, parallel)", ms: overlapAsync.toFixed(1), throughput: rate(OVERLAP_TRIALS, overlapAsync) },
  ]);
  console.log(`Overlap speedup: ${speedup.toFixed(2)}×  (theoretical ceiling for ${burnMs} ms burn / ${avgQueryMs.toFixed(1)} ms query: ${(1 + burnMs / avgQueryMs).toFixed(2)}×)`);
}

await main();
