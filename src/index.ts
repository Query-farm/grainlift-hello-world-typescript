// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * A minimal ADBC service written in TypeScript with the Grainlift SDK.
 *
 * Run it with `npm start` or `grainlift-hello-world`; the worker itself lives
 * in `./worker.ts`.
 */
export { HelloConnection, HelloStatement, HelloWorker, numbers, Query, RunningTotal } from "./worker.js";
