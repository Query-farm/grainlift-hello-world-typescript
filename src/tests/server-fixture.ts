// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// Serves the hello worker for native.test.ts in its own process: the Node ADBC
// driver manager makes some blocking calls, so client and server cannot share
// an event loop. Prints {"endpoint"} once listening; each "report" line on stdin
// prints the recorded principals, request paths and handle counts; EOF stops it.
import { createInterface } from "node:readline";
import {
  AdbcError,
  AuthContext,
  type AuthenticateFn,
  authenticateAnonymous,
  bearerAuthenticateStatic,
  GrainliftService,
  serveHttp,
} from "@query-farm/grainlift";
import { HelloStatement, HelloWorker, TARGET } from "../worker.js";

const [access = "tokens", behaviour = "normal"] = process.argv.slice(2);
const alice = new AuthContext("bearer", true, "alice");
const tokens = new Map([
  ["test-token-0123456789", alice],
  ["other-token-0123456789", new AuthContext("bearer", true, "bob")],
]);
const authenticate: AuthenticateFn =
  access === "anonymous"
    ? authenticateAnonymous("public", new Map([["test-token-0123456789", alice]]))
    : bearerAuthenticateStatic(tokens);
if (behaviour === "fail") {
  HelloStatement.prototype.execute = async () => {
    throw new AdbcError("Invalid data", "invalid_data", { sqlstate: "22000", vendorCode: 42 });
  };
}

const principals: string[] = [];
const paths: string[] = [];
const worker = new HelloWorker();
const service = new GrainliftService(
  {
    open: (options) => {
      principals.push(options.principal);
      return worker.open(options);
    },
  },
  { authorize: (_principal, target) => target === TARGET },
);
const host = await serveHttp(service, (request) => {
  paths.push(new URL(request.url).pathname);
  return authenticate(request);
});
process.stdout.write(`${JSON.stringify({ endpoint: host.endpoint })}\n`);
for await (const line of createInterface({ input: process.stdin })) {
  if (line === "report")
    process.stdout.write(`${JSON.stringify({ principals, paths, snapshot: service.snapshot() })}\n`);
}
await host.close();
