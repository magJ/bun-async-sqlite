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

async function main() {
  setupDb();
  console.log(`Benchmark DB rows=${ROWS}, sequential ops=${OPS}, parallel ops=${PARALLEL_OPS}, concurrency=${CONCURRENCY}`);

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
}

await main();
