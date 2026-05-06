import type { Changes, DatabaseOptions, SQLQueryBindings } from "bun:sqlite";

export type QueryArg = SQLQueryBindings;

type RequestPayload = { action: string; [key: string]: any };
type WorkerResponse = { id: number; ok: true; result: unknown } | { id: number; ok: false; error: { name: string; message: string; stack?: string } };

type StatementMeta = {
  columnNames: string[];
  paramsCount: number;
  columnTypes: Array<"INTEGER" | "FLOAT" | "TEXT" | "BLOB" | "NULL" | null>;
  declaredTypes: Array<string | null>;
};

export class AsyncStatement<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]> {
  #meta?: Promise<StatementMeta>;
  constructor(private readonly database: AsyncDatabase, private readonly statementId: number) {}
  private getMeta() { return (this.#meta ??= this.database.request<StatementMeta>({ action: "statement:meta", statementId: this.statementId })); }

  all(...params: ParamsType): Promise<ReturnType[]> { return this.database.request({ action: "statement:all", statementId: this.statementId, params }); }
  get(...params: ParamsType): Promise<ReturnType | null> { return this.database.request({ action: "statement:get", statementId: this.statementId, params }); }
  iterate(...params: ParamsType): Promise<ReturnType[]> { return this.database.request({ action: "statement:iterate", statementId: this.statementId, params }); }
  run(...params: ParamsType): Promise<Changes> { return this.database.request({ action: "statement:run", statementId: this.statementId, params }); }
  values(...params: ParamsType): Promise<Array<Array<string | bigint | number | boolean | Uint8Array>>> { return this.database.request({ action: "statement:values", statementId: this.statementId, params }); }
  raw(...params: ParamsType): Promise<Array<Array<Uint8Array | null>>> { return this.database.request({ action: "statement:raw", statementId: this.statementId, params }); }
  finalize(): Promise<void> { return this.database.request({ action: "statement:finalize", statementId: this.statementId }); }
  toString(): Promise<string> { return this.database.request({ action: "statement:toString", statementId: this.statementId }); }

  get columnNames() { return this.getMeta().then(m => m.columnNames); }
  get paramsCount() { return this.getMeta().then(m => m.paramsCount); }
  get columnTypes() { return this.getMeta().then(m => m.columnTypes); }
  get declaredTypes() { return this.getMeta().then(m => m.declaredTypes); }
}

export class AsyncDatabase {
  private readonly worker: Worker;
  private requestId = 0;
  private readonly pending = new Map<number, { resolve: (value: unknown) => void; reject: (reason?: unknown) => void }>();
  private readonly ready: Promise<unknown>;

  constructor(filename?: string, options?: number | DatabaseOptions) {
    this.worker = new Worker(new URL("./sqlite-worker.ts", import.meta.url).href, { type: "module" });
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const response = event.data;
      const pending = this.pending.get(response.id);
      if (!pending) return;
      this.pending.delete(response.id);
      if (response.ok) pending.resolve(response.result);
      else {
        const error = new Error(response.error.message);
        error.name = response.error.name;
        error.stack = response.error.stack;
        pending.reject(error);
      }
    };
    this.worker.onerror = event => {
      for (const p of this.pending.values()) p.reject(event.error ?? new Error(event.message));
      this.pending.clear();
    };
    this.ready = this.request({ action: "open", filename, options });
  }

  static open(filename: string, options?: number | DatabaseOptions) { return new AsyncDatabase(filename, options); }

  request<T = unknown>(payload: RequestPayload): Promise<T> {
    const id = ++this.requestId;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (v: unknown)=>void, reject });
      this.worker.postMessage({ id, ...payload });
    });
  }

  private async afterReady<T>(payload: RequestPayload) { await this.ready; return this.request<T>(payload); }

  run(sql: string, ...bindings: QueryArg[]): Promise<Changes> { return this.afterReady({ action: "run", sql, bindings }); }
  exec(sql: string, ...bindings: QueryArg[]): Promise<Changes> { return this.afterReady({ action: "exec", sql, bindings }); }
  async query<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string): Promise<AsyncStatement<ReturnType, ParamsType>> { const { statementId } = await this.afterReady<{statementId:number}>({ action: "prepare", sql, willCache: true }); return new AsyncStatement(this, statementId); }
  async prepare<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string, params?: SQLQueryBindings): Promise<AsyncStatement<ReturnType, ParamsType>> { const { statementId } = await this.afterReady<{statementId:number}>({ action: "prepare", sql, willCache: false, params }); return new AsyncStatement(this, statementId); }

  get inTransaction() { return this.afterReady<boolean>({ action: "inTransaction" }); }
  get filename() { return this.afterReady<string>({ action: "filename" }); }
  get handle() { return this.afterReady<number>({ action: "handle" }); }
  loadExtension(extension: string, entryPoint?: string) { return this.afterReady<void>({ action: "loadExtension", extension, entryPoint }); }
  serialize(name?: string) { return this.afterReady<Buffer>({ action: "serialize", name }); }
  fileControl(...args: any[]) { return this.afterReady<number>({ action: "fileControl", args }); }

  transaction<A extends any[], T>(insideTransaction: (...args: A) => Promise<T>) {
    const mk = (begin: string) => async (...args: A) => { await this.exec(begin); try { const v = await insideTransaction(...args); await this.exec("COMMIT"); return v; } catch (e) { await this.exec("ROLLBACK"); throw e; } };
    return Object.assign(mk("BEGIN"), { deferred: mk("BEGIN DEFERRED"), immediate: mk("BEGIN IMMEDIATE"), exclusive: mk("BEGIN EXCLUSIVE") });
  }

  async close(throwOnError?: boolean) { await this.ready; await this.request({ action: "close", throwOnError }); this.worker.terminate(); }
}
