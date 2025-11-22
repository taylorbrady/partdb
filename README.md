# PartDB

A Java-native, distributed key-value store built from scratch.

## Status

This project is in active development. Core modules (storage engine, Raft consensus) are implemented. Server, client, and CLI modules are planned.

## Architecture

- **Consensus**: Raft replication for strong consistency
- **Storage**: Custom LSM storage engine (WAL + memtable + SSTables + compaction)
- **Features**: TTL support

## Modules

**Implemented:**
- **partdb-common**: Shared utilities and abstractions
- **partdb-storage**: LSM storage engine implementation
- **partdb-raft**: Raft consensus protocol with gRPC transport

**Planned:**
- **partdb-server**: KV server (storage + raft + networking)
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
