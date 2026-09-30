// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// Run the shipped SQL example with the Haybarn CLI and its adbc_scanner extension.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { readFileSync, realpathSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { authenticateAnonymous, GrainliftService, serveHttp } from "@query-farm/grainlift";
import { HelloWorker, TARGET } from "../worker.js";

const EXAMPLE = fileURLToPath(new URL("../../../examples/query.sql", import.meta.url));
/** An explicit HAYBARN executable, or the pinned CLI through uvx. */
const HAYBARN = process.env.HAYBARN
  ? [process.env.HAYBARN]
  : spawnSync("uvx", ["--version"]).status === 0
    ? ["uvx", "--from", "haybarn-cli>=1.5.5rc1", "haybarn"]
    : undefined;
const skip = !process.env.GRAINLIFT_DRIVER
  ? "Set GRAINLIFT_DRIVER"
  : HAYBARN === undefined
    ? "Install uv or set HAYBARN"
    : false;

/** Split the CLI's JSON output into one row list per result set. */
function results(output: string): Record<string, unknown>[][] {
  const parsed: Record<string, unknown>[][] = [];
  let depth = 0;
  let start = 0;
  let quoted = false;
  for (let i = 0; i < output.length; i++) {
    const character = output[i];
    if (quoted) {
      if (character === "\\") i++;
      else if (character === '"') quoted = false;
    } else if (character === '"') quoted = true;
    else if (character === "[" || character === "{") {
      if (depth++ === 0) start = i;
    } else if ((character === "]" || character === "}") && --depth === 0) {
      parsed.push(JSON.parse(output.slice(start, i + 1)));
    }
  }
  return parsed;
}

test("`uvx haybarn-cli < examples/query.sql` returns the documented results", { skip }, async () => {
  const service = new GrainliftService(new HelloWorker(), {
    authorize: (_principal, target) => target === TARGET,
  });
  // Serve anonymously, as `npm start` does by default.
  const host = await serveHttp(service, authenticateAnonymous("anonymous"));
  try {
    const [command, ...args] = HAYBARN!;
    const child = spawn(command!, [...args, "-json"], {
      env: { ...process.env, GRAINLIFT_DRIVER: realpathSync(process.env.GRAINLIFT_DRIVER!) },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (data: Buffer) => {
      stdout += data;
    });
    child.stderr.on("data", (data: Buffer) => {
      stderr += data;
    });
    child.stdin.end(readFileSync(EXAMPLE, "utf8").replace("http://127.0.0.1:8080", host.endpoint));
    const code = await new Promise<number | null>((resolve, reject) => {
      const timer = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error("Haybarn timed out"));
      }, 120_000);
      child.once("exit", (value) => {
        clearTimeout(timer);
        resolve(value);
      });
    });
    assert.equal(code, 0, stderr);
    const [hello, numbers, runningTotal, disconnected] = results(stdout);
    assert.deepEqual(hello, [{ message: "Hello, world!" }]);
    assert.deepEqual(numbers, [{ numbers: 100000, total: "4999950000" }]); // HUGEINT sums are emitted as strings.
    assert.deepEqual(runningTotal, [
      { number: 2499, total: 3123750 },
      { number: 2498, total: 3121251 },
      { number: 2497, total: 3118753 },
    ]);
    assert.deepEqual(Object.values(disconnected![0]!), [true]);
  } finally {
    await host.close();
  }
});
