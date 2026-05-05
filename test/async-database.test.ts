import { describe, expect, test } from "bun:test";
import { AsyncDatabase } from "../src";

describe("AsyncDatabase", () => {
  test("run/exec/query lifecycle", async () => {
    const db = new AsyncDatabase(":memory:");

    const create = await db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    expect(create.changes).toBe(0);

    const insert = await db.run("INSERT INTO users (name) VALUES (?)", "Ada");
    expect(insert.changes).toBe(1);

    const stmt = await db.query<{ id: number; name: string }>("SELECT id, name FROM users WHERE name = ?");
    const row = await stmt.get("Ada");
    expect(row).toEqual({ id: 1, name: "Ada" });

    const rows = await stmt.all("Ada");
    expect(rows).toHaveLength(1);

    await stmt.finalize();
    await db.close();
  });

  test("transaction variants commit and rollback", async () => {
    const db = new AsyncDatabase(":memory:");
    await db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");

    const insertDeferred = db.transaction(async (name: string) => {
      await db.run("INSERT INTO items (name) VALUES (?)", name);
    }).deferred;

    await insertDeferred("ok");

    const failing = db.transaction(async () => {
      await db.run("INSERT INTO items (name) VALUES ('bad')");
      throw new Error("boom");
    });

    await expect(failing()).rejects.toThrow("boom");

    const stmt = await db.query<{ c: number }>("SELECT COUNT(*) as c FROM items");
    const result = await stmt.get();
    expect(result?.c).toBe(1);

    await stmt.finalize();
    await db.close();
  });
});
