# PartDB

A distributed key-value store in Java, implementing Raft consensus and a custom LSM storage engine.

## Status

In active development. Core functionality is implemented; working toward production readiness.

## Architecture

- **Consensus**: Raft for linearizable reads and writes
- **Storage**: LSM tree (WAL, memtable, SSTables, compaction)
- **Leases**: TTL-based expiration with heartbeat
- **Transport**: gRPC with Protocol Buffers

## Modules

- **partdb-node**: PartDB node runtime with KV state machine, lease handling, and durable Raft integration
- **partdb-storage**: LSM storage engine with bloom filters, block cache, and internal key/value primitives
- **partdb-raft**: Raft consensus with log replication and snapshots
- **partdb-protocol**: Protobuf definitions for KV and Raft RPCs
- **partdb-server**: gRPC server and Raft transport adapters on top of the node runtime
- **partdb-client**: Java client library
- **partdb-app**: Unified executable for server startup and admin commands
- **partdb-benchmark**: JMH benchmarks

## Building

```bash
./gradlew build
```

## Requirements

- Java 25+

## License

Apache 2.0
