import type { DatabaseOptions, SQLQueryBindings } from "bun:sqlite";
import { AsyncDatabase, AsyncStatement } from "./async-database";

export class AsyncDatabasePool {
  private readonly workers: AsyncDatabase[];
  private cursor = 0;

  constructor(size: number, filename?: string, options?: number | DatabaseOptions) {
    if (size < 1) throw new Error("Pool size must be >= 1");
    this.workers = Array.from({ length: size }, () => new AsyncDatabase(filename, options));
  }

  private next() {
    const db = this.workers[this.cursor % this.workers.length];
    this.cursor += 1;
    return db;
  }

  get size() { return this.workers.length; }

  run(sql: string, ...bindings: SQLQueryBindings[]) {
    return this.next().run(sql, ...bindings);
  }

  exec(sql: string, ...bindings: SQLQueryBindings[]) {
    return this.next().exec(sql, ...bindings);
  }

  query<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string): Promise<AsyncStatement<ReturnType, ParamsType>> {
    return this.next().query<ReturnType, ParamsType>(sql);
  }

  prepare<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string, params?: SQLQueryBindings): Promise<AsyncStatement<ReturnType, ParamsType>> {
    return this.next().prepare<ReturnType, ParamsType>(sql, params);
  }

  async close() {
    await Promise.all(this.workers.map((worker) => worker.close()));
  }

  async mapWorkers<T>(fn: (db: AsyncDatabase, index: number) => Promise<T>) {
    return Promise.all(this.workers.map(fn));
  }
}
