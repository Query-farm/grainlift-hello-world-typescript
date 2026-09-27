# Grainlift hello world (TypeScript)

A small Node.js ADBC worker consuming the separate `@query-farm/grainlift`
toolkit. Connect using the ordinary
[Grainlift ADBC driver](https://github.com/Query-farm/grainlift) over HTTP, HTTPS,
TCP, mTLS, or Iroh. Node.js 22 or newer is required.

## Status

This is a prerelease example and benchmark fixture, not a production database.
The [TypeScript toolkit](https://github.com/Query-farm/grainlift-typescript) is
not published to npm; this repository intentionally uses a sibling file
dependency. The example package is private and is not intended for npm publication.

The synthetic workload matches the Rust/Python/Go comparison fixture:
4096 rows, 512 rows per Arrow batch, 64 binary payload bytes per row. `QUERY`
returns nullable `number:int64` and `payload:binary`; `FAIL` produces a recoverable
ADBC `INVALID_DATA` with SQLSTATE 22000. Payload bytes contain ASCII `x`.

This example supports SQL/query execution and schema inference, with autocommit
enabled. The framework exposes the complete protocol, but this deliberately
small backend returns `NOT_IMPLEMENTED` for transactions, metadata, preparation,
binding, ingestion, partitions and Substrait.

## Quickstart

Clone and build the toolkit beside this repository:

```sh
git clone https://github.com/Query-farm/grainlift-typescript.git
git clone https://github.com/Query-farm/grainlift-hello-world-typescript.git
cd grainlift-typescript
npm ci
npm run build
cd ../grainlift-hello-world-typescript
npm ci
npm run build
export GRAINLIFT_HELLO_TOKEN="$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')"
node dist/main.js --transport http --port 0 --report report.json
```

Read the JSON readiness line for `endpoint` and `sample_pid`. Connect with the
ordinary native Grainlift ADBC driver, target `default`, bearer token from the
environment, and `autocommit=True`, then execute `QUERY`. Keep stdin open while
using the server. A newline/EOF on stdin or SIGINT/SIGTERM shuts down and writes
zero retained-handle counts to `report.json`. An optional
`GRAINLIFT_HELLO_OTHER_TOKEN` authenticates a second principal for ownership tests.

### Connect with an ordinary ADBC client

For this HTTP quickstart, use a second terminal with Python and install the
standard client dependencies:

```sh
python -m pip install adbc-driver-manager pyarrow
```

Install or build the [native Grainlift driver](https://github.com/Query-farm/grainlift)
for your platform. Set `GRAINLIFT_DRIVER` to its absolute shared-library path
(`.so`, `.dylib`, or `.dll`), `GRAINLIFT_ENDPOINT` to the printed endpoint, and
`GRAINLIFT_HELLO_TOKEN` to the same generated token used by the server. Then run:

```python
import os

from adbc_driver_manager import dbapi

with dbapi.connect(
    driver=os.environ["GRAINLIFT_DRIVER"],
    entrypoint="AdbcDriverGrainliftInit",
    autocommit=True,
    db_kwargs={
        "grainlift.uri": os.environ["GRAINLIFT_ENDPOINT"],
        "grainlift.target": "default",
        "grainlift.auth.bearer_token": os.environ["GRAINLIFT_HELLO_TOKEN"],
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("QUERY")
        result = cursor.fetch_arrow_table()
        print(result.schema)
        print(f"Received {result.num_rows} rows")
```

The default workload returns 4096 rows. Python is only the client in this example;
the server and backend run in Node.js.

## Usage and configuration

| Argument | Default | Purpose |
| --- | --- | --- |
| `--transport` | `http` | `http`, `https`, `tcp`, `mtls`, or `iroh` |
| `--port` | `0` | Let the OS choose a free listener port; Iroh uses its own endpoint |
| `--rows` | `4096` | Rows returned by `QUERY` |
| `--batch-rows` | `512` | Maximum rows per generated Arrow batch |
| `--payload-bytes` | `64` | Binary payload bytes per row |
| `--report` | Unset | Write retained-handle counts after shutdown |
| `--tls-dir` | Unset | Certificate directory for HTTPS or mTLS |

The example requires `GRAINLIFT_HELLO_TOKEN` of at least 16 bytes in every mode.
It authenticates HTTP/HTTPS calls only; the other transports use the identities
described below. Tokens and TLS private keys belong outside source control.

### Transports

HTTP, HTTPS, and TCP bind loopback. HTTPS takes `--tls-dir PATH` containing
`server.pem` and `server-key.pem`. mTLS additionally needs `ca.pem`, `client.pem`,
and `other.pem`. Clients need the corresponding private client key and trusted
CA; they must verify the server hostname. mTLS verifies the
certificate chain and authorizes only the exact client/other certificates from
that directory, mapping them to separate principals. Plain TCP explicitly trusts
local clients as `load-principal`; bearer tokens do not authenticate raw TCP.

Iroh requires the released `vgi-iroh-bridge` 0.27.3, an explicit
`GRAINLIFT_IROH_BRIDGE` executable path, and
`GRAINLIFT_HELLO_IROH_CLIENT_ID`/`GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID` EndpointId
allowlists (at least one is required). EndpointIds are 64 lowercase hexadecimal
characters. It runs the VGI bridge over a private Unix socket with
ephemeral identity and no relay. Readiness includes `endpoint_id` and
`direct_address` for native client configuration. This example setting is for
testing; deployed services should configure a persistent Iroh secret key through
the toolkit. The Unix-socket adapter supports Linux/macOS, not Windows, and
trusts processes running under the same OS account.

### Limits and lifecycle

All transports reuse connections. State is process-local and requires session
affinity. The example uses the toolkit's bounded session, request, batch, binding,
and admission defaults. Raw transports cap cumulative input at 128 MiB per
connection lifetime; exhausting this budget closes the socket and can produce
an ADBC `IO` error. Recreate the ADBC connection; there is no transparent retry.
See [toolkit hosting and limits](https://github.com/Query-farm/grainlift-typescript#hosting-and-limits)
for configuration and shutdown semantics. CLI workload controls do not override
the toolkit's memory and message limits.

## Testing

After building the sibling toolkit:

```sh
npm run check
npm test
```

The shared compatibility suite runs from a separate Grainlift checkout and needs
a built native driver plus that repository's Python validation dependencies:

```sh
python -m pytest validation/conformance \
  --worker-command '["node","/absolute/path/grainlift-hello-world-typescript/dist/main.js"]' \
  --native-driver /absolute/path/libadbc_driver_grainlift.so
```

Pass `--worker-transport tcp|mtls|https|iroh` to test another adapter,
`--worker-tls-dir PATH` for TLS fixtures, and `--iroh-bridge PATH` for Iroh.
The shared suite provisions independent client EndpointIds and credentials.

Run benchmarks and profiling on EC2 or another designated test host, not a
developer workstation. Do not use synthetic throughput as evidence for real
backend performance. The [CI workflow](.github/workflows/ci.yml) checks Node 22/24,
builds the sibling SDK and native driver, and exercises all five transports.
See [GitHub Actions](https://github.com/Query-farm/grainlift-hello-world-typescript/actions)
for hosted run results and the toolkit's
[recorded validation](https://github.com/Query-farm/grainlift-typescript/blob/main/VALIDATION.md)
for the completed EC2 run.

## Documentation

- [Toolkit API, authentication, limits, and deployment assumptions](https://github.com/Query-farm/grainlift-typescript)
- [Behavior coverage and remaining release work](https://github.com/Query-farm/grainlift-typescript/blob/main/docs/COVERAGE.md)
- [Synthetic backend implementation](src/backend.ts) and [executable host](src/main.ts)
- [Grainlift native driver and shared compatibility suite](https://github.com/Query-farm/grainlift)

## License

Licensed under [Apache License 2.0](LICENSE).
