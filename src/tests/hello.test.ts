// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// Unit tests for the hello-world worker, called in-process without a transport.
import assert from "node:assert/strict";
import { test } from "node:test";
import { AdbcError, type QueryResult, type RecordBatch, ResultProducer } from "@query-farm/grainlift";
import { HelloConnection, type HelloStatement, RunningTotal } from "../worker.js";

async function statement(sql: string): Promise<HelloStatement> {
  const created = await new HelloConnection().newStatement();
  await created.setSqlQuery(sql);
  return created;
}
/** Execute a query the way the service does: through a new statement. */
async function execute(sql: string): Promise<QueryResult> {
  return (await statement(sql)).execute();
}
async function collect(result: QueryResult): Promise<RecordBatch[]> {
  const batches: RecordBatch[] = [];
  for await (const value of result.batches) batches.push(value);
  return batches;
}
const column = (batches: RecordBatch[], name: string) =>
  batches.flatMap((value) => [...value.getChild(name)!].map(Number));
const status = (expected: string) => (error: unknown) =>
  error instanceof AdbcError && error.status === expected;

for (const count of [0, 1, 1023, 1024, 1025, 100000]) {
  test(`numbers(${count}) yields exactly n sequential rows in batches of at most 1024`, async () => {
    const batches = await collect(await execute(`SELECT * FROM numbers(${count})`));
    assert.equal(
      batches.reduce((rows, value) => rows + value.numRows, 0),
      count,
    );
    assert.ok(batches.every((value) => value.numRows <= 1024));
    assert.deepEqual(
      column(batches, "number"),
      Array.from({ length: count }, (_, i) => i),
    );
  });
}

test("unsupported or out-of-range queries fail preparation and execution with invalid_arguments", async () => {
  for (const query of [
    "SELECT * FROM numbers(100001)",
    "SELECT * FROM numbers(-1)",
    "SELECT * FROM running_total(100001)",
    "DROP TABLE x",
  ]) {
    await assert.rejects((await statement(query)).prepare(), status("invalid_arguments"));
    await assert.rejects((await statement(query)).execute(), (error: unknown) => {
      assert.ok(status("invalid_arguments")(error));
      assert.deepEqual(JSON.parse((error as Error).message).sqlstate, [...Buffer.from("42000")]);
      return true;
    });
  }
});

test("preparation, parameter and result schemas work before execution, as adbc_scanner requires", async () => {
  for (const [query, columns] of [
    ["SELECT 'Hello, world!' AS message;", ["message"]],
    ["select * from NUMBERS(5)", ["number"]],
    ["  SELECT * FROM running_total(5) ; ", ["number", "total"]],
  ] as const) {
    const prepared = await statement(query);
    await prepared.prepare();
    assert.equal((await prepared.getParameterSchema()).fields.length, 0);
    assert.deepEqual(
      (await prepared.executeSchema()).fields.map((f) => f.name),
      columns,
    );
    assert.deepEqual(
      (await prepared.execute()).schema.fields.map((f) => f.name),
      columns,
    );
  }
});

test("executing before setting a query is an ADBC INVALID_STATE error", async () => {
  const created = await new HelloConnection().newStatement();
  await assert.rejects(created.execute(), status("invalid_state"));
});

test("the hello query returns one row", async () => {
  const batches = await collect(await execute("SELECT 'Hello, world!' AS message"));
  assert.deepEqual(
    batches.flatMap((value) => [...value.getChild("message")!]),
    ["Hello, world!"],
  );
});

for (const count of [0, 1, 1024, 2500]) {
  test(`running_total(${count}) carries its sum across batch boundaries`, async () => {
    const result = await execute(`SELECT * FROM running_total(${count})`);
    assert.ok(result.producer instanceof RunningTotal);
    const batches = await collect(result);
    assert.ok(batches.every((value) => value.numRows <= 1024));
    assert.deepEqual(
      column(batches, "number"),
      Array.from({ length: count }, (_, i) => i),
    );
    assert.deepEqual(
      column(batches, "total"),
      Array.from({ length: count }, (_, n) => (n * (n + 1)) / 2),
    );
  });
}

test("the producer resumes identically after serialization, as it does between HTTP fetches", async () => {
  const producer = new RunningTotal(3000);
  const first = producer.produce();
  const resumed = ResultProducer.decode(producer.encode());
  assert.ok(first !== null && resumed instanceof RunningTotal);
  assert.deepEqual({ ...resumed }, { ...producer });
  assert.deepEqual(column([resumed.produce()!], "total"), column([producer.produce()!], "total"));
});
