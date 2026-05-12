# Testing Strategy

PartDB's highest-value tests are the ones that exercise the whole stack:

```text
client -> public gRPC -> server -> node -> consensus/Raft -> Raft peer gRPC -> storage
```

The test taxonomy keeps fast feedback, behavioral confidence, packaged-artifact
confidence, and performance measurement separate.

## Unit Tests

Run:

```bash
./gradlew test
```

Unit tests live in `src/test`. They should be fast, deterministic, and local to
one module. Use them for value objects, config validation, codecs, Raft state
transitions, storage invariants, command parsing, and small server/client
behavior.

## Project Checks

Run:

```bash
./gradlew check
```

`check` is the normal local quality gate. It runs unit tests and is the right
place to attach future formatting, static analysis, dependency, and license
checks. It intentionally does not run the heavier integration suites.

## In-Process Integration Tests

Run:

```bash
./gradlew integrationTest
```

Integration tests live in `src/integrationTest`. They exercise multiple PartDB
modules together inside the Gradle test JVM. These tests may start real
`PartDbServer` instances with public gRPC and Raft peer gRPC transports, but they do
not use the installed application distribution.

Use this suite for:

- 3-node startup
- leader election
- linearizable writes and reads through the public client
- leader stop and failover
- post-failover writes
- old leader restart and catch-up
- follower restart and catch-up
- full cluster restart
- range scan through the public client
- health endpoint lifecycle
- startup configuration failures

## Packaged Integration Tests

Run:

```bash
./gradlew packagedIntegrationTest
```

Packaged integration tests live in `src/packagedIntegrationTest`. They install
the application distribution, start real OS processes from the generated
executable, and drive the cluster through the packaged CLI and public client API.

Use this suite for:

- installed executable startup
- generated start scripts
- runtime classpath and logging dependencies
- default JVM arguments
- 3-node leader election
- CLI `put`, `get`, `status`, and `members`
- leader stop and failover
- post-failover writes
- old leader process restart and catch-up
- range scan through the public client

Keep this suite focused on shipped-artifact smoke coverage. Most behavioral
cluster scenarios belong in `integrationTest`.

## CI Gate

Run:

```bash
./gradlew ci
```

`ci` is the required pre-merge/release gate. It runs `check`,
`integrationTest`, and `packagedIntegrationTest`.

## Benchmarks

Run JMH benchmarks explicitly. They are performance tools, not correctness
checks, and must not be attached to `check` or `ci`. See
[benchmarks.md](benchmarks.md) for benchmark tasks and conventions.

## Known Gaps

- Tests use local ephemeral ports but do not simulate port conflicts or partial
  bind failures during startup.
- Process tests use graceful process termination. They do not yet cover forced
  kills during log writes, compaction, snapshot creation, or shutdown.
- Network partitions are not simulated. Current failover tests stop a node
  rather than dropping or delaying Raft traffic.
- Startup readiness is inferred through cluster status polling. There is no
  dedicated test that exercises `/livez` and `/readyz` for all lifecycle states.
- Snapshot transfer is not forced in an end-to-end cluster test. Catch-up
  currently relies on retained log replay.
- Linearizable multi-key read views are not implemented yet, so there is no
  consistency proof for multi-key reads beyond scan behavior.
- The packaged process harness does not yet preserve per-node logs as build
  artifacts on failure.

## Next Testing Goals

1. Add a forced snapshot-transfer cluster test by creating enough log pressure to
   require snapshot installation for a restarted peer.
2. Add startup failure tests for occupied ports and invalid peer configuration.
3. Add health endpoint lifecycle tests for starting, ready, leaderless, and
   shutting-down states.
4. Add crash-recovery tests that kill a process without graceful shutdown.
5. Add network fault injection once the Raft transport boundary is clearer.
