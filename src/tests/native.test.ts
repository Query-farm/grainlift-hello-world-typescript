// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// Real C-ABI coverage through the native Grainlift ADBC driver; no VGI client substitutes for it.
import assert from "node:assert/strict";
import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { once } from "node:events";
import { realpathSync } from "node:fs";
import { createInterface, type Interface } from "node:readline";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  type AdbcConnection,
  AdbcDatabase,
  type AdbcError as ClientError,
} from "@apache-arrow/adbc-driver-manager";
import type { RecordBatch } from "apache-arrow";
import { TARGET } from "../worker.js";

const DRIVER = process.env.GRAINLIFT_DRIVER;
const skip = DRIVER ? false : "Set GRAINLIFT_DRIVER";
const MAIN = fileURLToPath(new URL("../main.js", import.meta.url));
const FIXTURE = fileURLToPath(new URL("./server-fixture.js", import.meta.url));
const TOKEN = "test-token-0123456789";

interface Report {
  principals: string[];
  paths: string[];
  snapshot: { sessions: number; statements: number; results: number; uploads: number };
}
interface Served {
  endpoint: string;
  report(): Promise<Report>;
}

/** Wait for the next stdout line of a child process. */
async function line(lines: Interface, child: ChildProcessWithoutNullStreams): Promise<string> {
  const [value] = (await Promise.race([
    once(lines, "line"),
    once(child, "exit").then(() => {
      throw new Error("Server exited");
    }),
  ])) as [string];
  return value;
}

/**
 * Serve the hello worker over HTTP on an ephemeral port in a separate process;
 * the ADBC driver manager blocks the event loop during some driver calls.
 *
 * @param access `tokens` (alice and bob) or `anonymous` (public, plus alice's token).
 * @param behaviour `fail` makes every execution raise a structured ADBC error.
 */
async function serving(
  access: "tokens" | "anonymous",
  body: (served: Served) => Promise<void>,
  behaviour = "normal",
): Promise<void> {
  const child = spawn(process.execPath, [FIXTURE, access, behaviour]);
  const lines = createInterface({ input: child.stdout });
  try {
    const { endpoint } = JSON.parse(await line(lines, child)) as { endpoint: string };
    await body({
      endpoint,
      report: async () => {
        child.stdin.write("report\n");
        return JSON.parse(await line(lines, child)) as Report;
      },
    });
    child.stdin.end();
    const [code] = await once(child, "exit");
    assert.equal(code, 0);
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
  }
}

/** Connect through the native Grainlift ADBC driver, anonymously when no token is given. */
async function connect(endpoint: string, token: string | null = TOKEN): Promise<AdbcConnection> {
  const database = new AdbcDatabase({
    driver: realpathSync(DRIVER!),
    entrypoint: "AdbcDriverGrainliftInit",
    databaseOptions: {
      "grainlift.uri": endpoint,
      "grainlift.target": TARGET,
      ...(token === null ? {} : { "grainlift.auth.bearer_token": token }),
    },
  });
  try {
    const connection = await database.connect();
    const close = connection.close.bind(connection);
    connection.close = async () => {
      await close();
      await database.close();
    };
    return connection;
  } catch (error) {
    await database.close();
    throw error;
  }
}
async function batches(connection: AdbcConnection, sql: string): Promise<RecordBatch[]> {
  const collected: RecordBatch[] = [];
  for await (const value of await connection.queryStream(sql)) collected.push(value);
  return collected;
}
const sizes = (values: RecordBatch[]) => values.map((value) => value.numRows);
const last = (values: RecordBatch[], name: string) => {
  const final = values.at(-1)!;
  return Number(final.getChild(name)!.get(final.numRows - 1));
};
const idle = { sessions: 0, statements: 0, results: 0, uploads: 0 };

test("queries, schemas, errors and cursor cleanup work through real ADBC", { skip }, async () => {
  await serving("tokens", async (served) => {
    const connection = await connect(served.endpoint);
    try {
      const hello = await connection.query("SELECT 'Hello, world!' AS message");
      assert.deepEqual(hello.getChild("message")!.toArray(), ["Hello, world!"]);
      const numbers = await batches(connection, "SELECT * FROM numbers(2500)");
      assert.deepEqual(sizes(numbers), [1024, 1024, 452]);
      assert.equal(last(numbers, "number"), 2499);
      const empty = await connection.query("SELECT * FROM numbers(0)");
      assert.equal(empty.numRows, 0);
      assert.deepEqual(
        empty.schema.fields.map((field) => field.name),
        ["number"],
      );
      await assert.rejects(connection.query("unsupported"), (error: unknown) => {
        assert.equal((error as ClientError).sqlState, "42000");
        return true;
      });
    } finally {
      await connection.close();
    }
    assert.deepEqual((await served.report()).snapshot, idle);
    // Abandoning a reader mid-stream releases its server-side statement and result.
    const partial = await connect(served.endpoint);
    try {
      for await (const value of await partial.queryStream("SELECT * FROM numbers(100000)")) {
        assert.equal(value.numRows, 1024);
        break;
      }
      const { snapshot } = await served.report();
      assert.deepEqual([snapshot.statements, snapshot.results], [0, 0]);
    } finally {
      await partial.close();
    }
  });
});

test("running_total resumes across continuation tokens", { skip }, async () => {
  await serving("tokens", async (served) => {
    const connection = await connect(served.endpoint);
    try {
      const before = (await served.report()).paths.length;
      const totals = await batches(connection, "SELECT * FROM running_total(2500)");
      assert.deepEqual(sizes(totals), [1024, 1024, 452]);
      assert.equal(last(totals, "total"), (2499 * 2500) / 2);
      // Each batch after the first arrives through a sealed continuation token.
      const continuations = (await served.report()).paths
        .slice(before)
        .filter((path) => path.endsWith("/read_result/exchange"));
      assert.ok(continuations.length >= 2, `${continuations.length} continuation requests`);
    } finally {
      await connection.close();
    }
  });
});

test("an anonymous client queries without a token, even across continuations", { skip }, async () => {
  await serving("anonymous", async (served) => {
    const connection = await connect(served.endpoint, null);
    try {
      const hello = await connection.query("SELECT 'Hello, world!' AS message");
      assert.deepEqual(hello.getChild("message")!.toArray(), ["Hello, world!"]);
      assert.deepEqual(sizes(await batches(connection, "SELECT * FROM numbers(2500)")), [1024, 1024, 452]);
      assert.equal(last(await batches(connection, "SELECT * FROM running_total(2500)"), "total"), 3123750);
      assert.deepEqual((await served.report()).principals, ["public"]);
    } finally {
      await connection.close();
    }
    assert.deepEqual((await served.report()).snapshot, idle);
  });
});

test("token and anonymous clients coexist; a wrong token is rejected, not downgraded", { skip }, async () => {
  await serving("anonymous", async (served) => {
    const alice = await connect(served.endpoint);
    const anonymous = await connect(served.endpoint, null);
    try {
      assert.deepEqual([...(await served.report()).principals].sort(), ["alice", "public"]);
      for (const connection of [alice, anonymous]) {
        const table = await connection.query("SELECT * FROM numbers(3)");
        assert.deepEqual(Array.from(table.getChild("number")!.toArray(), Number), [0, 1, 2]);
      }
    } finally {
      await alice.close();
      await anonymous.close();
    }
    await assert.rejects(connect(served.endpoint, "wrong-token-0123456789"));
    assert.deepEqual((await served.report()).snapshot, idle);
  });
});

for (const token of ["wrong-token-0123456789", null]) {
  test(`a token-only server rejects a ${token ? "wrong" : "missing"} token`, { skip }, async () => {
    await serving("tokens", async (served) => {
      await assert.rejects(connect(served.endpoint, token));
      assert.deepEqual((await served.report()).snapshot, idle);
    });
  });
}

test("structured ADBC errors reach the client", { skip }, async () => {
  await serving(
    "tokens",
    async (served) => {
      const connection = await connect(served.endpoint);
      try {
        await assert.rejects(connection.query("SELECT 'Hello, world!' AS message"), (error: unknown) => {
          const failure = error as ClientError;
          assert.equal(failure.code, "InvalidData");
          assert.equal(failure.sqlState, "22000");
          assert.match(failure.message, /Invalid data/);
          return true;
        });
      } finally {
        await connection.close();
      }
    },
    "fail",
  );
});

for (const token of [TOKEN, null]) {
  test(`the command serves ${token ? "an exported token" : "anonymous clients"} by default`, {
    skip,
  }, async () => {
    const env = { ...process.env };
    delete env.GRAINLIFT_TOKEN;
    if (token) env.GRAINLIFT_TOKEN = token;
    const child = spawn(process.execPath, [MAIN, "--port", "0"], { env });
    let stdout = "";
    child.stdout.on("data", (data: Buffer) => {
      stdout += data;
    });
    try {
      const deadline = Date.now() + 20_000;
      while (!/listening on /.test(stdout)) {
        assert.equal(child.exitCode, null);
        assert.ok(Date.now() < deadline, "No listening line");
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.match(stdout, /Anonymous access enabled/);
      const connection = await connect(/listening on (\S+)/.exec(stdout)![1]!, token);
      try {
        const hello = await connection.query("SELECT 'Hello, world!' AS message");
        assert.deepEqual(hello.getChild("message")!.toArray(), ["Hello, world!"]);
        assert.deepEqual(sizes(await batches(connection, "SELECT * FROM numbers(2500)")), [1024, 1024, 452]);
      } finally {
        await connection.close();
      }
      child.kill("SIGTERM");
      const [code] = await once(child, "exit");
      assert.equal(code, 0);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
    }
  });
}
