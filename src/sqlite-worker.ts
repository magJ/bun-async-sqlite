import { Database } from "bun:sqlite";

let db: Database | null = null;
let statementSeq = 0;
const statements = new Map<number, ReturnType<Database["prepare"]>>();

const ensureDb = () => { if (!db) throw new Error("Database is not open"); return db; };
const ensureStmt = (id: number) => { const s = statements.get(id); if (!s) throw new Error(`Unknown statement id: ${id}`); return s; };

onmessage = (event: MessageEvent<any>) => {
  const m = event.data;
  try {
    let result: unknown;
    switch (m.action) {
      case "open": db = new Database(m.filename, m.options); result = null; break;
      case "close": for (const s of statements.values()) s.finalize(); statements.clear(); db?.close(m.throwOnError); db = null; result = null; break;
      case "run": result = ensureDb().run(m.sql, ...m.bindings); break;
      case "exec": result = ensureDb().exec(m.sql, ...m.bindings); break;
      case "prepare": {
        const stmt = m.willCache ? ensureDb().query(m.sql) : ensureDb().prepare(m.sql, m.params);
        const statementId = ++statementSeq;
        statements.set(statementId, stmt);
        result = { statementId };
        break;
      }
      case "statement:meta": {
        const s = ensureStmt(m.statementId);
        result = { columnNames: s.columnNames, paramsCount: s.paramsCount, columnTypes: s.columnTypes, declaredTypes: s.declaredTypes };
        break;
      }
      case "statement:all": result = ensureStmt(m.statementId).all(...m.params); break;
      case "statement:get": result = ensureStmt(m.statementId).get(...m.params); break;
      case "statement:iterate": result = Array.from(ensureStmt(m.statementId).iterate(...m.params)); break;
      case "statement:run": result = ensureStmt(m.statementId).run(...m.params); break;
      case "statement:values": result = ensureStmt(m.statementId).values(...m.params); break;
      case "statement:raw": result = ensureStmt(m.statementId).raw(...m.params); break;
      case "statement:toString": result = ensureStmt(m.statementId).toString(); break;
      case "statement:finalize": ensureStmt(m.statementId).finalize(); statements.delete(m.statementId); result = null; break;
      case "inTransaction": result = ensureDb().inTransaction; break;
      case "filename": result = ensureDb().filename; break;
      case "handle": result = ensureDb().handle; break;
      case "loadExtension": ensureDb().loadExtension(m.extension, m.entryPoint); result = null; break;
      case "serialize": result = ensureDb().serialize(m.name); break;
      case "fileControl": result = (ensureDb().fileControl as any)(...(m.args as any[])); break;
      default: throw new Error(`Unknown action ${m.action}`);
    }
    postMessage({ id: m.id, ok: true, result });
  } catch (error) {
    const e = error as Error;
    postMessage({ id: m.id, ok: false, error: { name: e.name, message: e.message, stack: e.stack } });
  }
};
