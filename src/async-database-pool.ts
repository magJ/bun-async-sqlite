import type { DatabaseOptions, SQLQueryBindings } from "bun:sqlite";
import { AsyncDatabase, AsyncStatement } from "./async-database";

type WorkerSlot = {
  db: AsyncDatabase;
  inFlight: number;
};

export class AsyncDatabasePool {
  private readonly workers: WorkerSlot[];

  constructor(size: number, filename?: string, options?: number | DatabaseOptions) {
    if (size < 1) throw new Error("Pool size must be >= 1");
    this.workers = Array.from({ length: size }, () => ({ db: new AsyncDatabase(filename, options), inFlight: 0 }));
  }

  private nextAvailable() {
    // Prefer idle workers; otherwise pick the least busy worker.
    let selected = this.workers[0];
    for (const worker of this.workers) {
      if (worker.inFlight === 0) return worker;
      if (worker.inFlight < selected.inFlight) selected = worker;
    }
    return selected;
  }

  private async withWorker<T>(task: (db: AsyncDatabase) => Promise<T>) {
    const worker = this.nextAvailable();
    worker.inFlight += 1;
    try {
      return await task(worker.db);
    } finally {
      worker.inFlight -= 1;
    }
  }

  get size() {
    return this.workers.length;
  }

  run(sql: string, ...bindings: SQLQueryBindings[]) {
    return this.withWorker((db) => db.run(sql, ...bindings));
  }

  exec(sql: string, ...bindings: SQLQueryBindings[]) {
    return this.withWorker((db) => db.exec(sql, ...bindings));
  }

  query<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string): Promise<AsyncStatement<ReturnType, ParamsType>> {
    return this.withWorker((db) => db.query<ReturnType, ParamsType>(sql));
  }

  prepare<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string, params?: SQLQueryBindings): Promise<AsyncStatement<ReturnType, ParamsType>> {
    return this.withWorker((db) => db.prepare<ReturnType, ParamsType>(sql, params));
  }

  async close() {
    await Promise.all(this.workers.map((worker) => worker.db.close()));
  }

  async mapWorkers<T>(fn: (db: AsyncDatabase, index: number) => Promise<T>) {
    return Promise.all(this.workers.map((worker, index) => fn(worker.db, index)));
  }
}
