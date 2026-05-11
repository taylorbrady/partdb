# Testing Strategy

PartDB's highest-value tests are the ones that exercise the whole stack:

```text
client -> public gRPC -> server -> node -> consensus/Raft -> Raft gRPC -> storage
```

## Current Proof Tests

### Fast In-Process Cluster Test

Run:

```bash
./gradlew :partdb-app:test --tests io.partdb.app.MultiNodeClusterIntegrationTest
```

This starts three `PartDbServer` instances in one JVM, each with real public gRPC
and Raft gRPC transports. It uses `KvClient` and `ClusterClient` rather than
node internals.

Coverage includes:

- 3-node startup
- leader election
- linearizable writes and reads through the public client
- leader stop and failover
- post-failover writes
- old leader restart and catch-up
- follower restart and catch-up
- full cluster restart
- range scan through the public client

### Packaged Process Cluster Test

Run:

```bash
./gradlew :partdb-app:packagedIntegrationTest
```

This installs the application distribution, starts three real OS processes, and
drives the cluster through the packaged CLI and public client API.

Coverage includes:

- installed executable startup
- 3-node leader election
- CLI `put`, `get`, `status`, and `members`
- leader stop and failover
- post-failover writes
- old leader process restart and catch-up
- range scan through the public client

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
