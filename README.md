# PartDB

A distributed key-value store in Java, implementing Raft consensus and a custom LSM storage engine.

## Status

This project is in active development. Core modules (storage engine, Raft consensus, lease system) are implemented. Server integration, client, and CLI modules are planned.

## Architecture

- **Consensus**: Raft replication for linearizable reads and writes
- **Storage**: Custom LSM tree (WAL, memtable, SSTables, compaction)
- **Leases**: Lease-based expiration for deterministic TTL
- **Protocols**: Protocol Buffers for serialization and gRPC for transport

## Modules

**Implemented:**
- **partdb-common**: Core abstractions (Entry, Operation, StateMachine, Lease)
- **partdb-storage**: LSM storage engine with lease-aware filtering
- **partdb-raft**: Raft consensus implementation with log replication and snapshots
- **partdb-protocol**: Protocol Buffer definitions for client-server communication
- **partdb-server**: Lessor for lease lifecycle management

**Planned:**
- **partdb-server**: Full KV server integration (in progress)
- **partdb-client**: Java client library
- **partdb-cli**: Command-line interface

## Building

```bash
./gradlew build
```

## Requirements

- Java 25+

## License

Apache 2.0
