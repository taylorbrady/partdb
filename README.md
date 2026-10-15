# PartDB

A small, modern Java key-value store built around a Raft-backed node runtime, a versioned state-store layer, and gRPC APIs.

## Status

In active development. The project is not production-ready yet.

PartDB v0 is intentionally small: durable key-value storage, static Raft clusters,
linearizable reads and writes, snapshots, recovery, and simple operational tools.
Deferred features such as leases, conditional writes, dynamic membership, watches,
auth, and transactions are kept out of the public API until their semantics are
implemented and tested.

See [docs/design.md](docs/design.md) for the current design direction,
[docs/modules.md](docs/modules.md) for module boundaries, and
[docs/testing.md](docs/testing.md) for the current proof tests and known gaps.

## Architecture

- **Raft**: `partdb-raft` holds a deterministic Raft core (`step` inputs, drain batched `Ready` effects with `ready`) with pre-vote, check-quorum, pipelined replication, read-index, learners, membership changes, and leadership transfer, plus, in `io.partdb.raft.runtime`, the event-loop runtime that owns the write-ahead log, automatic snapshots and log compaction, snapshot streaming, and the transport SPI
- **Node runtime**: a `partdb-node` module that composes the Raft runtime with the KV state machine for a single PartDB node
- **Storage**: a `partdb-storage` module that exposes a versioned state-store API backed by an internal leveled LSM engine with memtables, tables, compaction, block cache, and self-contained snapshots
- **Server**: public KV and cluster APIs defined in `partdb-grpc`; `partdb-server` implements them, carries Raft peer traffic over its own gRPC protocol, and assembles the process
- **Client**: a blocking Java client (`PartDbClient`) that follows the leader, bounds retries by one deadline, and never silently retries a write whose outcome is unknown

Module dependency direction and charters are in [docs/modules.md](docs/modules.md).

## Modules

- **partdb-bytes**: shared immutable byte values
- **partdb-raft**: pure Raft core plus the durable Raft runtime
- **partdb-storage**: internal state-store module backed by an LSM engine
- **partdb-node**: PartDB node runtime and state-machine composition
- **partdb-grpc**: public `.proto` schemas and generated classes
- **partdb-client**: Java client API for KV and cluster operations
- **partdb-server**: gRPC services, Raft peer transport, and process runtime assembly
- **partdb-app**: unified executable for node startup and operational commands
- **partdb-benchmarks**: JMH benchmarks

## CLI

```bash
partdb server start --node-id node1 --data-dir ./data/node1
partdb kv put hello world --endpoints localhost:8101
partdb kv get hello
partdb kv scan a z --limit 10 --output json
partdb cluster status
partdb cluster members
```

Cluster members are configured with repeatable `--peer nodeId=host:raftPort`
arguments (including the local node). Client commands read endpoints from
`--endpoints` or `PARTDB_ENDPOINTS`; exit codes are 0 (ok), 1 (error), 2
(usage), and 3 (key not found).

## Building and testing

```bash
./gradlew build
./gradlew test
```

## Requirements

- Java 25+

## License

Apache 2.0
