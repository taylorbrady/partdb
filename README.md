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

- **Consensus**: a reusable `partdb-raft` module with a pure Raft core, explicit `Ready` effects, snapshots, and pluggable storage/transport SPIs
- **Node runtime**: a `partdb-node` module that composes Raft, the KV state machine, and durable Raft persistence for a single PartDB node
- **Storage**: a `partdb-storage` module that exposes a versioned state-store API backed by an internal leveled LSM engine with memtables, SSTables, compaction, block cache, and self-contained snapshots
- **Transport**: public KV and cluster APIs defined in `partdb-grpc`, with handwritten gRPC adapters in `partdb-transport-grpc`
- **Client**: a Java client library with typed endpoint/config APIs and explicit cursor-based scans

### Module dependency direction

```text
partdb-app
  -> partdb-server
  -> partdb-client

partdb-server
  -> partdb-node
  -> partdb-consensus
  -> partdb-transport-grpc

partdb-transport-grpc
  -> partdb-node
  -> partdb-grpc
  -> partdb-raft

partdb-node
  -> partdb-consensus
  -> partdb-storage
  -> partdb-cluster
  -> partdb-bytes

partdb-consensus
  -> partdb-raft
  -> partdb-cluster
  -> partdb-bytes

partdb-storage
  -> partdb-bytes

partdb-raft
  -> partdb-bytes

partdb-client
  -> partdb-grpc
  -> partdb-bytes
```

## Modules

- **partdb-bytes**: shared immutable byte values
- **partdb-cluster**: cluster membership domain model
- **partdb-raft**: reusable Raft consensus engine
- **partdb-storage**: internal state-store module backed by an LSM engine
- **partdb-consensus**: durable, effectful Raft runtime
- **partdb-node**: PartDB node runtime and state-machine composition
- **partdb-grpc**: public `.proto` schemas and generated classes
- **partdb-transport-grpc**: gRPC server/bootstrap layer and consensus transport adapter
- **partdb-client**: Java client API for KV and cluster operations
- **partdb-server**: process runtime assembly
- **partdb-app**: unified executable for node startup and operational commands
- **partdb-benchmark**: JMH benchmarks

## CLI

```bash
partdb server start --node-id node1 --data-dir ./data/node1
partdb cluster status --endpoint localhost:8101
partdb cluster members --endpoint localhost:8101
partdb kv put hello world --endpoint localhost:8101
partdb kv get hello --endpoint localhost:8101
```

Raft peers are configured from the app with repeatable `--raft-peer nodeId=endpoint` arguments.

## Building and testing

```bash
./gradlew build
./gradlew test
```

## Requirements

- Java 25+

## License

Apache 2.0
