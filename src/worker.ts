// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * The hello-world worker: three queries, no SQL engine.
 *
 * `SELECT 'Hello, world!' AS message`
 *   A single-row result.
 *
 * `SELECT * FROM numbers(n)`
 *   Rows 0..n-1 from a **generator**. Simple, and fine whenever the server keeps
 *   the cursor in memory; it can also hold resources such as a database cursor.
 *
 * `SELECT * FROM running_total(n)`
 *   Rows 0..n-1 with a running sum, from a serializable **ResultProducer**. Over
 *   HTTP the producer's fields travel in the encrypted continuation token after
 *   every batch, so the server keeps no per-result iterator, and a retried fetch
 *   recomputes its batch from the token.
 */
import {
  AdbcError,
  batch,
  Connection,
  field,
  int64,
  type OpenOptions,
  QueryResult,
  type RecordBatch,
  ResultProducer,
  type Schema,
  Statement,
  schema,
  utf8,
  type Worker,
} from "@query-farm/grainlift";

export const TARGET = "hello";
export const MAX_ROWS = 100_000;
export const BATCH_ROWS = 1024;

export const HELLO_SCHEMA: Schema = schema([field("message", utf8(), true)]);
export const NUMBERS_SCHEMA: Schema = schema([field("number", int64(), true)]);
export const RUNNING_TOTAL_SCHEMA: Schema = schema([
  field("number", int64(), true),
  field("total", int64(), true),
]);

const TABLE_FUNCTION = /^select \* from (numbers|running_total)\(([0-9]{1,6})\)$/;

/** Generate 0..count-1 in batches of at most BATCH_ROWS rows. */
export function* numbers(count: number): Generator<RecordBatch> {
  for (let start = 0; start < count; start += BATCH_ROWS) {
    const end = Math.min(start + BATCH_ROWS, count);
    yield batch(NUMBERS_SCHEMA, { number: Array.from({ length: end - start }, (_, i) => BigInt(start + i)) });
  }
}

/** Resumable state for `running_total(n)`; each field survives between batches. */
export class RunningTotal extends ResultProducer {
  /** Next number to emit. */
  position = 0;
  /** Sum of every number emitted so far. */
  total = 0n;

  /** @param count Number of rows to produce. */
  constructor(readonly count: number) {
    super();
  }

  /** Emit the next batch and advance the state, or return null when done. */
  produce(): RecordBatch | null {
    if (this.position >= this.count) return null;
    const end = Math.min(this.position + BATCH_ROWS, this.count);
    const number: bigint[] = [];
    const total: bigint[] = [];
    for (let value = this.position; value < end; value++) {
      this.total += BigInt(value);
      number.push(BigInt(value));
      total.push(this.total);
    }
    this.position = end;
    return batch(RUNNING_TOTAL_SCHEMA, { number, total });
  }
}
// Register once under a stable name so the service can restore the state from a token.
ResultProducer.register("grainlift-hello-world:RunningTotal", RunningTotal);

/** A recognized query. */
export class Query {
  private constructor(
    /** `hello`, `numbers` or `running_total`. */
    readonly name: "hello" | "numbers" | "running_total",
    /** Requested row count for the table functions. */
    readonly count = 0,
  ) {}

  /** Recognize one of the supported queries (case-insensitive, optional trailing semicolon). */
  static parse(sql: string): Query {
    let text = sql.trim();
    if (text.endsWith(";")) text = text.slice(0, -1);
    text = text.trim().toLowerCase();
    if (text === "select 'hello, world!' as message") return new Query("hello");
    const match = TABLE_FUNCTION.exec(text);
    if (match === null || Number(match[2]) > MAX_ROWS) {
      throw new AdbcError(
        "Supported queries: SELECT 'Hello, world!' AS message; " +
          `SELECT * FROM numbers(n) or running_total(n), where 0 <= n <= ${MAX_ROWS}`,
        "invalid_arguments",
        { sqlstate: "42000" },
      );
    }
    return new Query(match[1] as "numbers" | "running_total", Number(match[2]));
  }

  /** The Arrow schema of this query's result. */
  get schema(): Schema {
    return { hello: HELLO_SCHEMA, numbers: NUMBERS_SCHEMA, running_total: RUNNING_TOTAL_SCHEMA }[this.name];
  }

  /** Start producing this query's result. */
  run(): QueryResult {
    if (this.name === "hello")
      return { schema: HELLO_SCHEMA, batches: [batch(HELLO_SCHEMA, { message: ["Hello, world!"] })] };
    if (this.name === "numbers") return { schema: NUMBERS_SCHEMA, batches: numbers(this.count) };
    return QueryResult.fromProducer(RUNNING_TOTAL_SCHEMA, new RunningTotal(this.count));
  }
}

/** One ADBC statement: set a query, optionally prepare it, then execute it. */
export class HelloStatement extends Statement {
  private sql: string | undefined;

  /** Store the query text; it is validated when prepared or executed. */
  override async setSqlQuery(sql: string): Promise<void> {
    this.sql = sql;
  }

  private query(): Query {
    if (this.sql === undefined)
      throw new AdbcError("Set a query before executing the statement", "invalid_state");
    return Query.parse(this.sql);
  }

  /** Validate the query; clients such as DuckDB's adbc_scanner prepare before executing. */
  override async prepare(): Promise<void> {
    this.query();
  }

  /** Report that the supported queries take no parameters. */
  override async getParameterSchema(): Promise<Schema> {
    this.query();
    return schema([]);
  }

  /** Return the result schema without producing rows. */
  override async executeSchema(): Promise<Schema> {
    return this.query().schema;
  }

  /** Execute the query. */
  override async execute(): Promise<QueryResult> {
    return this.query().run();
  }
}

/** A client connection; each statement is independent. */
export class HelloConnection extends Connection {
  override async newStatement(): Promise<HelloStatement> {
    return new HelloStatement();
  }
}

/** Serve the `hello` target; each client connection gets its own HelloConnection. */
export class HelloWorker implements Worker {
  /** Open a connection for an authenticated (or anonymous) principal. */
  async open(_options: OpenOptions): Promise<HelloConnection> {
    return new HelloConnection();
  }
}
