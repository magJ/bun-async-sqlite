import { Database } from "bun:sqlite";
import { A } from "./actions";

let db: Database | null = null;
let statementSeq = 0;
const statements = new Map<number, ReturnType<Database["prepare"]>>();

const ensureDb = () => { if (!db) throw new Error("Database is not open"); return db; };
const ensureStmt = (id: number) => { const s = statements.get(id); if (!s) throw new Error(`Unknown statement id: ${id}`); return s; };

// Unpack params from compact message format: scalar `p`, array `ps`, or none.
function getParams(m: any): any[] {
  if ("ps" in m) return m.ps;
  if ("p" in m) return [m.p];
  return [];
}

onmessage = (event: MessageEvent<any>) => {
  const m = event.data;
  try {
    let result: unknown;
    switch (m.a) {
      case A.Open: db = new Database(m.filename, m.options); result = null; break;
      case A.Close: for (const s of statements.values()) s.finalize(); statements.clear(); db?.close(m.throwOnError); db = null; result = null; break;
      case A.Run: result = ensureDb().run(m.sql, ...m.b); break;
      case A.Exec: result = ensureDb().exec(m.sql, ...m.b); break;
      case A.Prepare: {
        const stmt = m.c ? ensureDb().query(m.sql) : ensureDb().prepare(m.sql, m.params);
        const s = ++statementSeq;
        statements.set(s, stmt);
        result = { s };
        break;
      }
      case A.StmtMeta: {
        const s = ensureStmt(m.s);
        result = { columnNames: s.columnNames, paramsCount: s.paramsCount, columnTypes: s.columnTypes, declaredTypes: s.declaredTypes };
        break;
      }
      case A.StmtAll: result = ensureStmt(m.s).all(...getParams(m)); break;
      case A.StmtGet: result = ensureStmt(m.s).get(...getParams(m)); break;
      case A.StmtIterate: result = Array.from(ensureStmt(m.s).iterate(...getParams(m))); break;
      case A.StmtRun: result = ensureStmt(m.s).run(...getParams(m)); break;
      case A.StmtValues: result = ensureStmt(m.s).values(...getParams(m)); break;
      case A.StmtRaw: result = ensureStmt(m.s).raw(...getParams(m)); break;
      case A.StmtToString: result = ensureStmt(m.s).toString(); break;
      case A.StmtFinalize: ensureStmt(m.s).finalize(); statements.delete(m.s); result = null; break;
      case A.InTransaction: result = ensureDb().inTransaction; break;
      case A.Filename: result = ensureDb().filename; break;
      case A.Handle: result = ensureDb().handle; break;
      case A.LoadExtension: ensureDb().loadExtension(m.extension, m.entryPoint); result = null; break;
      case A.Serialize: result = ensureDb().serialize(m.name); break;
      case A.FileControl: result = (ensureDb().fileControl as any)(...(m.args as any[])); break;
      default: throw new Error(`Unknown action ${m.a}`);
    }
    postMessage({ id: m.id, ok: true, result });
  } catch (error) {
    const e = error as Error;
    postMessage({ id: m.id, ok: false, error: { name: e.name, message: e.message, stack: e.stack } });
  }
};
