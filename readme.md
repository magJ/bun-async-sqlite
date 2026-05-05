# bun-async-sqlite

An async wrapper around Bun's `bun:sqlite` `Database` API.

## What it does

`AsyncDatabase` mirrors the most common Bun sqlite primitives (`exec`, `run`, `query`/`prepare`, statement `all`/`get`/`values`/`run`/`finalize`) but executes every DB operation inside a dedicated worker thread.

This keeps the main thread responsive even when queries are slow.

## Example

```ts
import { AsyncDatabase } from "./src";

const db = new AsyncDatabase(":memory:");
await db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
await db.run("INSERT INTO users (name) VALUES (?)", "Ada");

const stmt = await db.query("SELECT id, name FROM users WHERE name = ?");
const row = await stmt.get<{ id: number; name: string }>("Ada");
await stmt.finalize();

await db.close();
```

## Notes

- The API is intentionally close to Bun's sync sqlite API, but all calls return `Promise`s.
- Statements are prepared and retained in the worker until `finalize()`.
- `transaction()` helper is provided for async callback workflows.
