# Testing Strategy

PartDB's highest-value tests are the ones that exercise the whole stack:

```text
client -> public gRPC -> server -> node -> Raft runtime -> Raft peer gRPC -> storage
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

### `partdb-raft`

Tests of the pure core (`io.partdb.raft`) drive `Raft` through `step` and
`ready` exactly as the runtime does. They must not use real files, sockets,
clocks, threads, transports, or PartDB command semantics. `TestNode` wraps one
core with an `InMemoryRaftLog`, persists every `Ready`, acknowledges it
with `Persisted`, and returns the combined output; `ClusterHarness` connects
several cores through an in-memory message queue with isolation and
compaction.

Keep focused suites by protocol concern:

- `RaftElectionTest`: pre-vote, voting, leases, check-quorum, terms.
- `RaftReplicationTest`: append handling, conflict hints, batching,
  pipelining, inflight limits, commit rules, heartbeats.
- `RaftReadIndexTest`: read rounds, deferral until the leader commits in its
  term, forwarding, rejection and expiry.
- `RaftMembershipTest`: single-server changes, learners, quorum changes,
  leader self-removal and hand-off.
- `RaftSnapshotTest`, `RaftRecoveryTest`, `RaftLeadershipTransferTest`.
- `ProgressTest`, `UnstableLogTest`, and `RaftCorePurityTest`: helper and
  purity coverage. The purity test keeps the core package free of JDK IO,
  time, threading, and runtime imports, which a package boundary inside one
  module cannot enforce.
- `ClusterTest`: deterministic multi-node scenarios (elections, partitions,
  catch-up, snapshots, reads, membership, leadership transfer).
- `SimulationTest`: randomized, seeded cluster histories. Each seed drives
  three or five cores through message loss, delay, duplication, reordering,
  partitions, crashes, unreachable reports, leadership transfers, membership
  changes (including leader self-removal), and log compaction while clients
  issue reads and writes. Every run checks election
  safety, state machine safety, commit monotonicity, log matching,
  convergence after faults heal, and linearizability of the client history
  (`LinearizabilityChecker`, a WGL search over per-key register histories).
  `test` runs 200 seeds; `./gradlew :partdb-raft:simulationTest` runs 5000,
  and `-Pseeds=N -PfirstSeed=S` selects a range. Replay a failing seed with
  `new Simulation(seed, Simulation.Settings.forSeed(seed)).trace(System.out).run()`.

Tests of the runtime (`io.partdb.raft.runtime`) may use real files and
threads. `WriteAheadLogTest` and `LogStoreTest` cover durability, replay,
torn tails, snapshot resets, and compaction. `RaftRuntimeTest` runs real
runtimes over `LocalNetwork`, an in-memory transport, and covers replication,
follower reads, restart replay, the cluster file, leadership transfer,
membership changes including leader self-removal, partitions, entry- and
byte-triggered compaction, and streamed snapshot catch-up.

## Project Checks

Run:

```bash
./gradlew check
```

`check` is the normal local quality gate. It runs unit tests and is the right
place to attach future formatting, static analysis, dependency, and license
checks. It intentionally does not run the heavier integration suites.

Test wiring shared by every module lives in
`build-logic/src/main/kotlin/partdb.java-conventions.gradle.kts`: the JUnit
version from `gradle/libs.versions.toml`, JVM arguments, a ten-minute task
timeout, and console output for failed tests. Module build files only declare
dependencies and extra test suites.

## In-Process Integration Tests

Run:

```bash
./gradlew integrationTest
```

Integration tests live in `src/integrationTest`. They exercise multiple PartDB
modules together inside the Gradle test JVM. These tests may start real
`PartDbServer` instances with public gRPC and Raft peer gRPC transports, but they do
not use the installed application distribution.

Both integration suites share `TestCluster` from the `partdb-client` test
fixtures (`partdb-client/src/testFixtures`). It allocates ports, starts nodes
through a `TestClusterNode` implementation, and polls cluster state through the
public client. `integrationTest` plugs in `InProcessNode`;
`packagedIntegrationTest` plugs in `PackagedNode`. Cluster tests keep their
temp directories on failure (`@TempDir(cleanup = ON_SUCCESS)`).

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

Health endpoint and JMX behavior for a single server are covered by
`partdb-server` unit tests; startup configuration validation is covered by
`PartDbServerConfigTest`.

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
- clean shutdown on SIGTERM

Each node process writes its stdout and stderr to per-start log files under the
test's temp directory, which is kept when the test fails. Stopping a node fails
the test unless the process exits within ten seconds and logs `Server stopped`.

Keep this suite focused on shipped-artifact smoke coverage. Behavioral cluster
scenarios such as failover, restart, and catch-up belong in `integrationTest`.

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
- Network partitions are simulated in `partdb-raft` (`SimulationTest`,
  `ClusterTest`, `RaftRuntimeTest`) but not over the gRPC peer transport;
  process-level failover tests stop a node rather than dropping traffic.
- Startup readiness is inferred through cluster status polling. There is no
  dedicated test that exercises `/livez` and `/readyz` for all lifecycle states.
- Linearizable multi-key read views are not implemented yet, so there is no
  consistency proof for multi-key reads beyond scan behavior.

## Next Testing Goals

1. Add startup failure tests for occupied ports and invalid peer configuration.
2. Add health endpoint lifecycle tests for starting, ready, leaderless, and
   shutting-down states.
3. Add crash-recovery tests that kill a process without graceful shutdown.
4. Add fault injection to the gRPC peer transport so process-level tests can
   drop and delay Raft traffic.
