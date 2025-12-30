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

- **partdb-common**: Core types (Slice, Entry, Lease, HybridClock)
- **partdb-storage**: LSM storage engine with bloom filters and block cache
- **partdb-raft**: Raft consensus with log replication and snapshots
- **partdb-protocol**: Protobuf definitions for KV and Raft RPCs
- **partdb-server**: KV server with Raft integration and lease management
- **partdb-client**: Java client library
- **partdb-cli**: Server startup CLI
- **partdb-ctl**: Admin tool (get, put, delete)
- **partdb-benchmark**: JMH benchmarks

## Building

```bash
./gradlew build
```

## Requirements

- Java 25+

## License

Apache 2.0
