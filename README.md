# PartDB

A distributed key-value store in Java built around a Raft-backed node runtime, a versioned state-store layer, and gRPC APIs.

## Status

In active development. The core module structure is in place and the main runtime, storage, client, and transport boundaries are established. The project is not production-ready yet.

## Architecture

- **Consensus**: a reusable `partdb-raft` module with a pure Raft core, explicit `Ready` effects, snapshots, and pluggable storage/transport SPIs
- **Node runtime**: a `partdb-node` module that composes Raft, the KV state machine, leases, and durable Raft persistence for a single PartDB node
- **Storage**: a `partdb-storage` module that exposes a versioned state-store API backed by an internal leveled LSM engine with memtables, SSTables, compaction, block cache, and self-contained snapshots
- **Transport**: public KV and cluster APIs defined in `partdb-grpc`, with handwritten gRPC adapters in `partdb-transport-grpc`
- **Client**: a Java client library with typed endpoint/config APIs and explicit cursor-based scans

### Module dependency direction

```text
partdb-app
  -> partdb-client
  -> partdb-transport-grpc

partdb-transport-grpc
  -> partdb-node
  -> partdb-grpc

partdb-node
  -> partdb-storage
  -> partdb-raft

partdb-client
  -> partdb-grpc
```

## Modules

- **partdb-raft**: reusable Raft consensus engine
- **partdb-storage**: internal state-store module backed by an LSM engine
- **partdb-node**: PartDB node runtime and state-machine composition
- **partdb-grpc**: public `.proto` schemas and generated classes
- **partdb-transport-grpc**: gRPC server/bootstrap layer and consensus transport adapter
- **partdb-client**: Java client API for KV and cluster operations
- **partdb-app**: unified executable for node startup and operational commands
- **partdb-benchmark**: JMH benchmarks

## CLI

```bash
partdb start --node-id node1 --data-dir ./data/node1
partdb status --endpoint localhost:8101
partdb member list --endpoint localhost:8101
partdb put hello world --endpoint localhost:8101
partdb get hello --endpoint localhost:8101
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
