// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import {
  AdbcError,
  Binary,
  batch,
  Connection,
  Field,
  Int64,
  type OpenOptions,
  type OptionKind,
  type OptionValue,
  type QueryResult,
  Schema,
  Statement,
  type Worker,
} from "@query-farm/grainlift";

export interface Workload {
  rows: number;
  batchRows: number;
  payloadBytes: number;
}
export const resultSchema = new Schema([
  new Field("number", new Int64(), true),
  new Field("payload", new Binary(), true),
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
class SyntheticStatement extends Statement {
  private sql: string | undefined;
  constructor(private readonly workload: Workload) {
    super();
  }
  override async setSqlQuery(sql: string): Promise<void> {
    if (sql !== "QUERY" && sql !== "FAIL") throw new AdbcError("Unknown query", "invalid_arguments");
    this.sql = sql;
  }
  override async execute(): Promise<QueryResult> {
    if (this.sql === "FAIL") throw new AdbcError("Synthetic failure", "invalid_data", { sqlstate: "22000" });
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
    return resultSchema;
  }
}
class SyntheticConnection extends Connection {
  constructor(private readonly workload: Workload) {
    super();
  }
  override async newStatement(): Promise<Statement> {
    return new SyntheticStatement(this.workload);
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
export class SyntheticWorker implements Worker {
  private readonly workload: Workload;
  constructor(workload: Workload) {
    this.workload = validateWorkload(workload);
  }
  async open(options: OpenOptions): Promise<Connection> {
    const connection = new SyntheticConnection(this.workload);
    for (const [key, value] of options.connectionOptions) await connection.setOption(key, value);
    return connection;
  }
}
