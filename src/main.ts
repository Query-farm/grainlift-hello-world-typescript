#!/usr/bin/env node
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * Command-line entry point: `npm start` or `grainlift-hello-world`.
 *
 * Serves HelloWorker on loopback; run with `--help` for hosting options. The
 * service is read-only, so it accepts anonymous clients by default; pass
 * `--auth token` to require a bearer token.
 */
import { run } from "@query-farm/grainlift/cli";
import { HelloWorker, TARGET } from "./worker.js";

run(new HelloWorker(), {
  target: TARGET,
  name: "grainlift-hello-world",
  description: "Grainlift hello-world ADBC service",
  auth: "anonymous",
}).catch((error: unknown) => {
  process.stderr.write(`grainlift-hello-world: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
