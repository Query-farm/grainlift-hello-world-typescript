// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * Query the hello-world service from Node.js with the ADBC driver manager.
 *
 * Set GRAINLIFT_DRIVER to the native driver library, start the service, then
 * run `npm run client`. Optionally set GRAINLIFT_ENDPOINT, and GRAINLIFT_TOKEN
 * (omit it to connect anonymously).
 */
import { realpathSync } from "node:fs";
import { AdbcDatabase } from "@apache-arrow/adbc-driver-manager";

const endpoint = process.env.GRAINLIFT_ENDPOINT ?? "http://127.0.0.1:8080";
const options: Record<string, string> = { "grainlift.uri": endpoint, "grainlift.target": "hello" };
const required = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`Set ${name}`);
  return value;
};
if (endpoint.startsWith("tls+tcp://")) {
  options["grainlift.tls.ca"] = required("GRAINLIFT_TLS_CA");
  options["grainlift.tls.cert"] = required("GRAINLIFT_TLS_CERT");
  options["grainlift.tls.key"] = required("GRAINLIFT_TLS_KEY");
  options["grainlift.tls.server_name"] = required("GRAINLIFT_TLS_SERVER_NAME");
} else if (process.env.GRAINLIFT_TOKEN) {
  options["grainlift.auth.bearer_token"] = process.env.GRAINLIFT_TOKEN;
}

const database = new AdbcDatabase({
  driver: realpathSync(required("GRAINLIFT_DRIVER")),
  entrypoint: "AdbcDriverGrainliftInit",
  databaseOptions: options,
});
const connection = await database.connect();
try {
  const hello = await connection.query("SELECT 'Hello, world!' AS message");
  console.log({ message: hello.getChild("message")!.toArray() });

  const sizes: number[] = [];
  for await (const batch of await connection.queryStream("SELECT * FROM numbers(2500)"))
    sizes.push(batch.numRows);
  console.log(`numbers(2500): [${sizes.join(", ")}] rows per Arrow batch`);

  const totals = await connection.query("SELECT * FROM running_total(2500)");
  console.log(
    `running_total(2500): last row ${JSON.stringify(totals.get(totals.numRows - 1)!.toJSON(), bigints)}`,
  );

  const empty = await connection.query("SELECT * FROM numbers(0)");
  console.log(`Empty result: ${empty.numRows} rows, schema: ${empty.schema.fields.join(", ")}`);
} finally {
  await connection.close();
  await database.close();
}

function bigints(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? Number(value) : value;
}
