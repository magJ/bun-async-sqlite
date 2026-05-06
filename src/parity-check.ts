import type { Changes, Database, DatabaseOptions, SQLQueryBindings, Statement } from "bun:sqlite";
import type { AsyncDatabase, AsyncStatement, QueryArg } from "./async-database";

type Assert<T extends true> = T;
type IsAssignable<A, B> = A extends B ? true : false;

// Methods/properties we expect to keep in sync with async equivalents.
type DbParityContract = {
  run(sql: string, ...bindings: QueryArg[]): Promise<Changes>;
  exec(sql: string, ...bindings: QueryArg[]): Promise<Changes>;
  query<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string): Promise<AsyncStatement<ReturnType, ParamsType>>;
  prepare<ReturnType = unknown, ParamsType extends SQLQueryBindings[] = SQLQueryBindings[]>(sql: string, params?: QueryArg): Promise<AsyncStatement<ReturnType, ParamsType>>;
  readonly inTransaction: Promise<boolean>;
  readonly filename: Promise<string>;
  readonly handle: Promise<number>;
  loadExtension(extension: string, entryPoint?: string): Promise<void>;
  serialize(name?: string): Promise<Buffer>;
  close(throwOnError?: boolean): Promise<void>;
};

type StmtParityContract = {
  all(...params: SQLQueryBindings[]): Promise<unknown[]>;
  get(...params: SQLQueryBindings[]): Promise<unknown | null>;
  iterate(...params: SQLQueryBindings[]): Promise<unknown[]>;
  run(...params: SQLQueryBindings[]): Promise<Changes>;
  values(...params: SQLQueryBindings[]): Promise<Array<Array<string | bigint | number | boolean | Uint8Array>>>;
  raw(...params: SQLQueryBindings[]): Promise<Array<Array<Uint8Array | null>>>;
  finalize(): Promise<void>;
  toString(): Promise<string>;
};

type _DbParityAssignable = Assert<IsAssignable<AsyncDatabase, DbParityContract>>;
type _StmtParityAssignable = Assert<IsAssignable<AsyncStatement, StmtParityContract>>;

// Keep references to Bun types to make intended comparison explicit.
type _BunDb = Pick<Database, "run" | "exec" | "query" | "prepare" | "inTransaction" | "filename" | "handle" | "loadExtension" | "serialize" | "close">;
type _BunStmt = Pick<Statement, "all" | "get" | "iterate" | "run" | "values" | "raw" | "finalize" | "toString">;
