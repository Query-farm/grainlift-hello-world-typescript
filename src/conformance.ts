#!/usr/bin/env node
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * The TypeScript worker for Grainlift's shared conformance suite
 * (`validation/conformance` in the grainlift repository); see its README for
 * the worker process contract. Not part of the hello-world example.
 *
 * `QUERY` yields `number: int64, payload: binary` rows in bounded batches;
 * `FAIL` returns `INVALID_DATA` with SQLSTATE 22000; `STORE` keeps the bound
 * parameters in memory and `STORED` returns them.
 */
import { X509Certificate } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";
import {
  AdbcError,
  AuthContext,
  batch,
  bearerAuthenticateStatic,
  binary,
  Connection,
  ExternalStorageConfig,
  field,
  GrainliftService,
  int64,
  type OpenOptions,
  type OptionKind,
  type OptionValue,
  type QueryResult,
  type RecordBatch,
  type Schema,
  Statement,
  type StreamServer,
  schema,
  serveHttp,
  serveIroh,
  serveMutualTls,
  serveTcp,
  type Worker,
} from "@query-farm/grainlift";

export interface Workload {
  rows: number;
  batchRows: number;
  payloadBytes: number;
}
export const resultSchema: Schema = schema([
  field("number", int64(), true),
  field("payload", binary(), true),
]);

export function validateWorkload(value: Workload): Workload {
  if (
    !Number.isSafeInteger(value.rows) ||
    value.rows < 0 ||
    value.rows > 10_000_000 ||
    !Number.isSafeInteger(value.batchRows) ||
    value.batchRows < 1 ||
    value.batchRows > 65_536 ||
    !Number.isSafeInteger(value.payloadBytes) ||
    value.payloadBytes < 0 ||
    value.payloadBytes > 65_536 ||
    value.batchRows * (value.payloadBytes + 16) > 8 * 1024 * 1024
  ) {
    throw new TypeError("Invalid or oversized workload");
  }
  return value;
}

/** The parameters last bound to `STORE`, shared by every connection. */
interface Stored {
  schema: Schema | undefined;
  batches: readonly RecordBatch[];
}

class ConformanceStatement extends Statement {
  private sql: string | undefined;
  private bound: { schema: Schema; batches: readonly RecordBatch[] } | undefined;
  constructor(
    private readonly workload: Workload,
    private readonly stored: Stored,
  ) {
    super();
  }
  override async setSqlQuery(sql: string): Promise<void> {
    if (!["QUERY", "FAIL", "STORE", "STORED"].includes(sql))
      throw new AdbcError("Unknown query", "invalid_arguments");
    this.sql = sql;
  }
  override async bind(boundSchema: Schema, value: RecordBatch): Promise<void> {
    this.bound = { schema: boundSchema, batches: [value] };
  }
  override async bindStream(boundSchema: Schema, values: readonly RecordBatch[]): Promise<void> {
    this.bound = { schema: boundSchema, batches: values };
  }
  /** `STORE`: keep the bound rows, replacing the previous ones. */
  private store(): bigint {
    if (!this.bound) throw new AdbcError("STORE needs bound parameters", "invalid_state");
    this.stored.schema = this.bound.schema;
    this.stored.batches = this.bound.batches;
    this.bound = undefined;
    return BigInt(this.stored.batches.reduce((rows, value) => rows + value.numRows, 0));
  }
  override async executeUpdate(): Promise<bigint> {
    if (this.sql !== "STORE") throw new AdbcError("Only STORE is an update", "invalid_state");
    return this.store();
  }
  override async execute(): Promise<QueryResult> {
    if (this.sql === "FAIL") throw new AdbcError("Synthetic failure", "invalid_data", { sqlstate: "22000" });
    if (this.sql === "STORE") {
      const rows = this.store();
      return { schema: schema([]), batches: [], rowsAffected: rows };
    }
    if (this.sql === "STORED") {
      if (!this.stored.schema) throw new AdbcError("Nothing is stored", "invalid_state");
      return { schema: this.stored.schema, batches: [...this.stored.batches] };
    }
    if (this.sql !== "QUERY") throw new AdbcError("Set a query before execution", "invalid_state");
    const { rows, batchRows, payloadBytes } = this.workload;
    function* batches() {
      const payload = new Uint8Array(payloadBytes).fill(120);
      for (let start = 0; start < rows; start += batchRows) {
        const count = Math.min(batchRows, rows - start);
        yield batch(resultSchema, {
          number: Array.from({ length: count }, (_, i) => BigInt(start + i)),
          payload: Array.from({ length: count }, () => payload),
        });
      }
    }
    return { schema: resultSchema, batches: batches(), rowsAffected: BigInt(rows) };
  }
  override async executeSchema(): Promise<Schema> {
    if (!this.sql) throw new AdbcError("Set a query before execution", "invalid_state");
    if (this.sql === "STORED") return this.stored.schema ?? schema([]);
    if (this.sql === "STORE") return schema([]);
    return resultSchema;
  }
}

class ConformanceConnection extends Connection {
  constructor(
    private readonly workload: Workload,
    private readonly stored: Stored,
  ) {
    super();
  }
  override async newStatement(): Promise<Statement> {
    return new ConformanceStatement(this.workload, this.stored);
  }
  override async setOption(key: string, value: OptionValue): Promise<void> {
    if (key === "adbc.connection.autocommit" && value === "true") return;
    throw new AdbcError("Connection option is not implemented", "not_implemented");
  }
  override async getOption(key: string, kind: OptionKind): Promise<OptionValue> {
    if (key === "adbc.connection.autocommit" && kind === "string") return "true";
    throw new AdbcError("Connection option is not implemented", "not_implemented");
  }
}

export class ConformanceWorker implements Worker {
  private readonly workload: Workload;
  private readonly stored: Stored = { schema: undefined, batches: [] };
  constructor(workload: Workload) {
    this.workload = validateWorkload(workload);
  }
  async open(options: OpenOptions): Promise<Connection> {
    const connection = new ConformanceConnection(this.workload, this.stored);
    for (const [key, value] of options.connectionOptions) await connection.setOption(key, value);
    return connection;
  }
}

function positiveInteger(name: string, value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error(`--${name} must be a positive integer`);
  return parsed;
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      port: { type: "string", default: "0" },
      rows: { type: "string", default: "4096" },
      "batch-rows": { type: "string", default: "512" },
      "payload-bytes": { type: "string", default: "64" },
      report: { type: "string" },
      transport: { type: "string", default: "http" },
      "tls-dir": { type: "string" },
      "max-request-bytes": { type: "string" },
      "storage-endpoint": { type: "string" },
      "storage-bucket": { type: "string" },
      "storage-region": { type: "string", default: "auto" },
      "storage-prefix": { type: "string", default: "" },
      "storage-threshold-bytes": { type: "string", default: "1048576" },
    },
  });
  const token = process.env.GRAINLIFT_HELLO_TOKEN ?? "";
  if (Buffer.byteLength(token) < 16) throw new Error("Token must contain at least 16 bytes");
  const identities = new Map([[token, new AuthContext("bearer", true, "load-principal")]]);
  const other = process.env.GRAINLIFT_HELLO_OTHER_TOKEN;
  if (other) {
    if (Buffer.byteLength(other) < 16 || other === token) throw new Error("Invalid second token");
    identities.set(other, new AuthContext("bearer", true, "other-principal"));
  }
  const requestBytes = positiveInteger("max-request-bytes", values["max-request-bytes"]);
  const http = values.transport === "http" || values.transport === "https";
  if ((values["storage-endpoint"] === undefined) !== (values["storage-bucket"] === undefined))
    throw new Error("--storage-endpoint and --storage-bucket go together");
  const externalStorage = values["storage-endpoint"]
    ? new ExternalStorageConfig({
        endpoint: values["storage-endpoint"],
        bucket: values["storage-bucket"]!,
        region: values["storage-region"],
        prefix: values["storage-prefix"],
        thresholdBytes: positiveInteger("storage-threshold-bytes", values["storage-threshold-bytes"]),
      })
    : undefined;
  if (externalStorage && !http) throw new Error("Object storage applies to HTTP only");
  const service = new GrainliftService(
    new ConformanceWorker({
      rows: Number(values.rows),
      batchRows: Number(values["batch-rows"]),
      payloadBytes: Number(values["payload-bytes"]),
    }),
    {
      authorize: (principal, target) =>
        target === "default" && ["load-principal", "other-principal"].includes(principal),
      allowedConnectionOptions: new Set(["adbc.connection.autocommit"]),
      ...(requestBytes ? { limits: { requestBytes } } : {}),
    },
  );
  let running: StreamServer;
  let extra: Record<string, string> = {};
  const port = Number(values.port);
  const file = async (name: string) => {
    if (!values["tls-dir"]) throw new Error("TLS directory is required");
    return readFile(join(values["tls-dir"], name));
  };
  if (values.transport === "tcp") {
    running = await serveTcp(service, new AuthContext("local", true, "load-principal"), { port });
  } else if (values.transport === "mtls") {
    const peers = new Map([
      [new X509Certificate(await file("client.pem")).fingerprint256, "load-principal"],
      [new X509Certificate(await file("other.pem")).fingerprint256, "other-principal"],
    ]);
    running = await serveMutualTls(service, {
      port,
      ca: await file("ca.pem"),
      cert: await file("server.pem"),
      key: await file("server-key.pem"),
      authenticatePeer: (certificate) => {
        const owner = peers.get(certificate.fingerprint256);
        if (!owner) throw new Error("Client certificate is not authorized");
        return new AuthContext("mtls", true, owner);
      },
    });
  } else if (values.transport === "iroh") {
    const peers = new Map<string, string>();
    for (const [name, principal] of [
      ["GRAINLIFT_HELLO_IROH_CLIENT_ID", "load-principal"],
      ["GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID", "other-principal"],
    ] as const) {
      const value = process.env[name];
      if (value && /^[a-f0-9]{64}$/.test(value)) peers.set(value, principal);
    }
    if (peers.size === 0) throw new Error("Iroh client endpoint allowlist is required");
    const iroh = await serveIroh(service, {
      bridgePath: process.env.GRAINLIFT_IROH_BRIDGE ?? "",
      ephemeral: true,
      noRelay: true,
      issuer: "grainlift-conformance",
      authenticateEndpoint: (id) => peers.get(id) ?? null,
    });
    running = iroh;
    extra = { direct_address: iroh.directAddress, endpoint_id: iroh.endpointId };
  } else if (http) {
    running = await serveHttp(service, bearerAuthenticateStatic(identities), {
      port,
      ...(values.transport === "https"
        ? { tls: { cert: await file("server.pem"), key: await file("server-key.pem") } }
        : {}),
      ...(externalStorage ? { http: { externalStorage } } : {}),
    });
  } else throw new Error("Unknown transport");
  process.stdout.write(
    `${JSON.stringify({ endpoint: running.endpoint, sample_pid: process.pid, ...extra })}\n`,
  );
  await new Promise<void>((resolve) => {
    process.stdin.once("data", () => resolve());
    process.stdin.once("end", () => resolve());
    process.stdin.resume();
    process.once("SIGINT", () => resolve());
    process.once("SIGTERM", () => resolve());
  });
  process.stdin.pause();
  await running.close();
  if (values.report) await writeFile(values.report, JSON.stringify({ after_shutdown: service.snapshot() }));
}

main().catch(() => {
  // Never echo errors: they may carry configuration or credentials.
  process.stderr.write("Conformance worker failed\n");
  process.exitCode = 1;
});
