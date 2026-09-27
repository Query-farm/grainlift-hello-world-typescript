// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { X509Certificate } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";
import {
  AuthContext,
  bearerAuthenticateStatic,
  GrainliftService,
  type StreamServer,
  serveHttp,
  serveIroh,
  serveMutualTls,
  serveTcp,
} from "@query-farm/grainlift";
import { SyntheticWorker } from "./backend.js";

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
  const service = new GrainliftService(
    new SyntheticWorker({
      rows: Number(values.rows),
      batchRows: Number(values["batch-rows"]),
      payloadBytes: Number(values["payload-bytes"]),
    }),
    {
      authorize: (principal, target) =>
        target === "default" && ["load-principal", "other-principal"].includes(principal),
      allowedConnectionOptions: new Set(["adbc.connection.autocommit"]),
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
    ]) {
      const value = process.env[name!];
      if (value && /^[a-f0-9]{64}$/.test(value)) peers.set(value, principal!);
    }
    if (peers.size === 0) throw new Error("Iroh client endpoint allowlist is required");
    const iroh = await serveIroh(service, {
      bridgePath: process.env.GRAINLIFT_IROH_BRIDGE ?? "",
      ephemeral: true,
      noRelay: true,
      issuer: "grainlift-hello-world",
      authenticateEndpoint: (id) => peers.get(id) ?? null,
    });
    running = iroh;
    extra = { direct_address: iroh.directAddress, endpoint_id: iroh.endpointId };
  } else if (values.transport === "http" || values.transport === "https") {
    running = await serveHttp(service, bearerAuthenticateStatic(identities), {
      port,
      ...(values.transport === "https"
        ? { tls: { cert: await file("server.pem"), key: await file("server-key.pem") } }
        : {}),
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
  process.stderr.write("Synthetic server failed\n");
  process.exitCode = 1;
});
