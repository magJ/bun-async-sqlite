import type { Changes, DatabaseOptions, SQLQueryBindings } from "bun:sqlite";
import { A } from "./actions";

export type QueryArg = SQLQueryBindings;

type RequestPayload = { [key: string]: any };
type WorkerResponse = { id: number; ok: true; result: unknown } | { id: number; ok: false; error: { name: string; message: string; stack?: string } };

type StatementMeta = {
  columnNames: string[];
  paramsCount: number;
  columnTypes: Array<"INTEGER" | "FLOAT" | "TEXT" | "BLOB" | "NULL" | null>;
  declaredTypes: Array<string | null>;
};

// Build the minimal message for a statement call, avoiding array boxing for ≤1 param.
function stmtMsg(a: number, s: number, params: SQLQueryBindings[]): RequestPayload {
  if (params.length === 0) return { a, s };
  if (params.length === 1) return { a, s, p: params[0] };
  return { a, s, ps: params };
}

export class AsyncStatement<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]> {
  #meta?: Promise<StatementMeta>;
  constructor(private readonly database: AsyncDatabase, private readonly s: number) {}
  private getMeta() { return (this.#meta ??= this.database.request<StatementMeta>({ a: A.StmtMeta, s: this.s })); }

  all(...params: ParamsType): Promise<ReturnType[]> { return this.database.request(stmtMsg(A.StmtAll, this.s, params)); }
  get(...params: ParamsType): Promise<ReturnType | null> { return this.database.request(stmtMsg(A.StmtGet, this.s, params)); }
  iterate(...params: ParamsType): Promise<ReturnType[]> { return this.database.request(stmtMsg(A.StmtIterate, this.s, params)); }
  run(...params: ParamsType): Promise<Changes> { return this.database.request(stmtMsg(A.StmtRun, this.s, params)); }
  values(...params: ParamsType): Promise<Array<Array<string | bigint | number | boolean | Uint8Array>>> { return this.database.request(stmtMsg(A.StmtValues, this.s, params)); }
  raw(...params: ParamsType): Promise<Array<Array<Uint8Array | null>>> { return this.database.request(stmtMsg(A.StmtRaw, this.s, params)); }
  finalize(): Promise<void> { return this.database.request({ a: A.StmtFinalize, s: this.s }); }
  toString(): Promise<string> { return this.database.request({ a: A.StmtToString, s: this.s }); }

  get columnNames() { return this.getMeta().then(m => m.columnNames); }
  get paramsCount() { return this.getMeta().then(m => m.paramsCount); }
  get columnTypes() { return this.getMeta().then(m => m.columnTypes); }
  get declaredTypes() { return this.getMeta().then(m => m.declaredTypes); }
}

export class AsyncDatabase {
  private readonly worker: Worker;
  private requestId = 0;
  // Flat tuple [resolve, reject] avoids allocating a wrapper object per pending request.
  private readonly pending = new Map<number, [(value: unknown) => void, (reason?: unknown) => void]>();
  private _isReady = false;
  private readonly ready: Promise<unknown>;

  constructor(filename?: string, options?: number | DatabaseOptions) {
    this.worker = new Worker(new URL("./sqlite-worker.ts", import.meta.url).href, { type: "module" });
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const response = event.data;
      const pending = this.pending.get(response.id);
      if (!pending) return;
      this.pending.delete(response.id);
      if (response.ok) pending[0](response.result);
      else {
        const error = new Error(response.error.message);
        error.name = response.error.name;
        error.stack = response.error.stack;
        pending[1](error);
      }
    };
    this.worker.onerror = event => {
      for (const p of this.pending.values()) p[1](event.error ?? new Error(event.message));
      this.pending.clear();
    };
    this.ready = this.request({ a: A.Open, filename, options });
  }

  static open(filename: string, options?: number | DatabaseOptions) { return new AsyncDatabase(filename, options); }

  // Mutate payload in-place to add the request id, avoiding an object spread copy.
  request<T = unknown>(payload: RequestPayload): Promise<T> {
    const id = ++this.requestId;
    payload.id = id;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, [resolve as (v: unknown) => void, reject]);
      this.worker.postMessage(payload);
    });
  }

  // Synchronous fast-path once the database is open: skip the microtask tick that
  // `await this.ready` would add even when the promise is already resolved.
  private afterReady<T>(payload: RequestPayload): Promise<T> {
    if (this._isReady) return this.request<T>(payload);
    return this.ready.then(() => {
      this._isReady = true;
      return this.request<T>(payload);
    }) as Promise<T>;
  }

  run(sql: string, ...bindings: QueryArg[]): Promise<Changes> { return this.afterReady({ a: A.Run, sql, b: bindings }); }
  exec(sql: string, ...bindings: QueryArg[]): Promise<Changes> { return this.afterReady({ a: A.Exec, sql, b: bindings }); }
  async query<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string): Promise<AsyncStatement<ReturnType, ParamsType>> { const { s } = await this.afterReady<{ s: number }>({ a: A.Prepare, sql, c: true }); return new AsyncStatement(this, s); }
  async prepare<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string, params?: SQLQueryBindings): Promise<AsyncStatement<ReturnType, ParamsType>> { const { s } = await this.afterReady<{ s: number }>({ a: A.Prepare, sql, c: false, params }); return new AsyncStatement(this, s); }

  get inTransaction() { return this.afterReady<boolean>({ a: A.InTransaction }); }
  get filename() { return this.afterReady<string>({ a: A.Filename }); }
  get handle() { return this.afterReady<number>({ a: A.Handle }); }
  loadExtension(extension: string, entryPoint?: string) { return this.afterReady<void>({ a: A.LoadExtension, extension, entryPoint }); }
  serialize(name?: string) { return this.afterReady<Buffer>({ a: A.Serialize, name }); }
  fileControl(...args: any[]) { return this.afterReady<number>({ a: A.FileControl, args }); }

  transaction<A extends any[], T>(insideTransaction: (...args: A) => Promise<T>) {
    const mk = (begin: string) => async (...args: A) => { await this.exec(begin); try { const v = await insideTransaction(...args); await this.exec("COMMIT"); return v; } catch (e) { await this.exec("ROLLBACK"); throw e; } };
    return Object.assign(mk("BEGIN"), { deferred: mk("BEGIN DEFERRED"), immediate: mk("BEGIN IMMEDIATE"), exclusive: mk("BEGIN EXCLUSIVE") });
  }

  async close(throwOnError?: boolean) { await this.ready; await this.request({ a: A.Close, throwOnError }); this.worker.terminate(); }
}
