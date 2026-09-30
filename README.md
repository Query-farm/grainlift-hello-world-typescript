# grainlift-hello-world (TypeScript)

A complete ADBC service in about 200 lines of TypeScript, built with the
[Grainlift TypeScript SDK](https://github.com/Query-farm/grainlift-typescript)
(`@query-farm/grainlift`). Any ADBC application connects to it through the
native Grainlift driver; the service itself needs no database, SQL engine or
downstream driver.

## Quickstart

Requires Node.js 22+ and Rust 1.97+ (to build the native Grainlift ADBC driver
once). The SDK is not on npm yet; `npm ci` installs it from a pinned GitHub
commit and builds it.

    git clone https://github.com/Query-farm/grainlift.git ../grainlift
    (cd ../grainlift && cargo build --locked -p adbc-driver-grainlift)
    npm ci

Start the service (`node dist/src/main.js` works too once built):

    npm start

No credentials are needed. The service is read-only, so it accepts anonymous
clients (see [Authentication](#authentication)).

### Query it from SQL

[Haybarn](https://github.com/Query-farm-haybarn/haybarn), Query.Farm's DuckDB
distribution, loads the Grainlift driver through the `adbc_scanner` extension.
In a second terminal, run [`examples/query.sql`](examples/query.sql) (this uses
[uv](https://docs.astral.sh/uv/)):

    export GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib  # .so on Linux
    uvx haybarn-cli < examples/query.sql

The same script runs unchanged in the DuckDB CLI. It prints:

    ┌───────────────┐
    │    message    │
    │    varchar    │
    ├───────────────┤
    │ Hello, world! │
    └───────────────┘
    ┌─────────┬────────────┐
    │ numbers │   total    │
    │  int64  │   int128   │
    ├─────────┼────────────┤
    │  100000 │ 4999950000 │
    └─────────┴────────────┘
    ...

`adbc_scan` sends its quoted SQL to this service. The rows come back as an
ordinary relation that you can join, aggregate or export locally.

### Query it from TypeScript

[`examples/client.ts`](examples/client.ts) uses the standard ADBC driver manager
for Node.js ([`@apache-arrow/adbc-driver-manager`](https://www.npmjs.com/package/@apache-arrow/adbc-driver-manager)):

    npm run client

It prints:

    { message: [ 'Hello, world!' ] }
    numbers(2500): [1024, 1024, 452] rows per Arrow batch
    running_total(2500): last row {"number":2499,"total":3123750}
    Empty result: 0 rows, schema: number: Int64

## What's in the package

| Module | Contents |
| --- | --- |
| [`src/worker.ts`](src/worker.ts) | The service: `HelloWorker` → `HelloConnection` → `HelloStatement`, plus the two result styles below |
| [`src/main.ts`](src/main.ts) | The `grainlift-hello-world` command (`npm start`) |

The service answers three queries:

| Query | Result | Demonstrates |
| --- | --- | --- |
| `SELECT 'Hello, world!' AS message` | one row | the smallest possible result |
| `SELECT * FROM numbers(n)` | 0..n-1 | a **generator** of Arrow batches |
| `SELECT * FROM running_total(n)` | 0..n-1 with a running sum | a serializable **`ResultProducer`** |

`n` ranges from 0 to 100000. Anything else is an ADBC `INVALID_ARGUMENT` error
with SQLSTATE 42000. The example matches these queries exactly rather than
pretending to parse SQL.

`HelloStatement` implements the ADBC statement lifecycle: set the SQL, then
`prepare`, `executeSchema` and `execute`. Preparation matters because clients
such as `adbc_scanner` prepare every query before running it.

### Generators vs. producers

Both styles stream lazily in batches of at most 1024 rows, and you can mix them
freely within one service.

- **Generator** (`numbers`): return `{ schema, batches: generator }`. Any
  iterable or async iterable of batches works. It's the simplest option, and it
  can hold resources such as an open database cursor (release them in the
  result's `close` callback). The iterator lives in server memory until the
  client finishes or releases the result.
- **Producer** (`running_total`): subclass `ResultProducer` with fields that are
  the entire resumable state, implement `produce()`, register the class once
  with `ResultProducer.register(name, RunningTotal)`, and return
  `QueryResult.fromProducer(schema, state)`. Over HTTP the state is serialized
  into the encrypted continuation token after each batch. The server keeps no
  iterator or replay batch between fetches, and a retried fetch recomputes its
  batch from the token. This is the same approach VGI-RPC streams use.

Pick a producer when the state is small and serializable (JSON values and
`bigint`), such as offsets, keyset cursors or counters. Pick a generator when it
isn't.

## Authentication

Anonymous access is opt-in in the Grainlift SDK. This example enables it because
it only serves public, read-only data: its command calls
`run(new HelloWorker(), { target: "hello", auth: "anonymous" })` from
`@query-farm/grainlift/cli`. Requests without credentials act as the shared
`anonymous` principal.

- Set `GRAINLIFT_TOKEN` (at least 16 bytes) on both sides to connect as an
  authenticated principal instead. A client that sends a wrong token is
  rejected, never downgraded to anonymous.
- Run `npm start -- --auth token` to require a token. The server prints a
  generated token when `GRAINLIFT_TOKEN` is unset.

For a service that can write data or expose private data, keep the default
token authentication. In your own hosting code, anonymous access is
`serveHttp(service, authenticateAnonymous("anonymous", tokens))`, where the
optional `tokens` maps bearer secrets to identities as in
`bearerAuthenticateStatic`.

## Hosting options

`npm start -- --help` lists them. Any worker gets the same command-line host by
calling `run()` from `@query-farm/grainlift/cli` in its own entry point.

- `--host http` (default): loopback HTTP for development. SIGINT/SIGTERM drain
  requests and close handles.
- `--host mtls`: verified TCP/mTLS; client certificates identify callers.
- `--port`: listening port (default 8080). Point the client at a different
  port with `GRAINLIFT_ENDPOINT`.

For mTLS, supply the server chain, key, client CA and authorized client URI SAN:

    npm start -- --host mtls --port 8443 \
      --tls-cert server.pem --tls-key server-key.pem \
      --client-ca clients-ca.pem --client-uri spiffe://example.org/client

    export GRAINLIFT_ENDPOINT=tls+tcp://127.0.0.1:8443
    export GRAINLIFT_TLS_CA=server-ca.pem GRAINLIFT_TLS_CERT=client.pem GRAINLIFT_TLS_KEY=client-key.pem
    export GRAINLIFT_TLS_SERVER_NAME=localhost   # the DNS name in the server certificate
    npm run client

These hosts are for development and bind to loopback. For production
deployment, limits and the security contract, see the SDK's
[README](https://github.com/Query-farm/grainlift-typescript#readme).

## Development

    npm run check    # tsc and Biome (lint and format)
    GRAINLIFT_DRIVER=../grainlift/target/debug/libadbc_driver_grainlift.dylib npm test

Native integration tests skip when `GRAINLIFT_DRIVER` is unset. They include
running `examples/query.sql` in the Haybarn CLI through `uvx` (or the executable
named by `HAYBARN`), which downloads the `adbc_scanner` extension on first use.
The native tests run the service in a separate process, because the Node.js
driver manager blocks the event loop during some driver calls. CI builds a
pinned native-driver revision and runs everything on Linux and macOS with
Node.js 22 and 24.
