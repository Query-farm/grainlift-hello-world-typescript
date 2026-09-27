// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { AdbcError } from "@query-farm/grainlift";
import { SyntheticWorker, validateWorkload } from "../backend.js";

test("synthetic worker yields matching rows, batches, and payload bytes lazily", async () => {
  const worker = new SyntheticWorker({ rows: 513, batchRows: 512, payloadBytes: 64 });
  const connection = await worker.open({
    target: "default",
    principal: "test",
    databaseOptions: new Map(),
    connectionOptions: new Map(),
  });
  const statement = await connection.newStatement();
  await statement.setSqlQuery("QUERY");
  const result = await statement.execute();
  const batches = [];
  for await (const value of result.batches) batches.push(value);
  assert.deepEqual(
    batches.map((b) => b.numRows),
    [512, 1],
  );
  assert.equal(batches[1]!.getChild("number")!.get(0), 512n);
  assert.equal(batches[0]!.getChild("payload")!.get(0)!.length, 64);
  await statement.setSqlQuery("FAIL");
  await assert.rejects(
    statement.execute(),
    (error: unknown) => error instanceof AdbcError && error.status === "invalid_data",
  );
  await statement.close();
  await connection.close();
});
test("workload limits reject unsafe and oversized configurations", () => {
  for (const value of [-1, 1.5, Number.MAX_SAFE_INTEGER]) {
    assert.throws(() => validateWorkload({ rows: value, batchRows: 512, payloadBytes: 64 }));
  }
  assert.throws(() => validateWorkload({ rows: 100, batchRows: 65536, payloadBytes: 65536 }));
});
