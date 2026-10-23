# PartDB Design Direction

PartDB is a small, modern Java implementation of a Raft-backed key-value store.
The project favors a clear distributed-systems core over a large feature surface.

## V0 Scope

PartDB v0 supports:

- durable key-value storage
- static Raft clusters
- linearizable `get`, `put`, `delete`, and `scan`
- atomic multi-key transactions with conditions on key existence, value, or
  revision (compare-and-set)
- local reads (`LOCAL` consistency) where the caller explicitly asks for them
- status and member inspection
- snapshots and recovery for the replicated state machine

PartDB v0 does not support:

- leases or TTLs
- dynamic cluster membership changes over the public API (the Raft core and
  runtime implement single-server changes and leader self-removal, but nodes
  are not addressable by the transport until they are configured as peers)
- backups and restores (Raft snapshots cover replica catch-up and recovery)
- transactions across independent revisions
- watch APIs
- authentication or authorization
- multi-tenant namespaces

Deferred features should not appear in the public API until their behavior is
implemented and tested through failure cases.

## Java Style

PartDB should read like idiomatic modern Java:

- Use records for immutable values and sealed interfaces for closed event,
  command, and result families.
- Prefer domain names over infrastructure names at API boundaries.
- Keep acronyms readable: `KvClient`, `GrpcServer`, `LsmConfig`, not all-caps
  identifier blocks unless the Java ecosystem strongly expects them.
- Use typed values such as `Duration`, `Instant`, `Path`, `Bytes`, and domain
  records instead of passing raw primitives through multiple layers.
- Keep transport classes at the edge. Core modules should be testable without
  gRPC.
- Keep Raft event handling explicit: input event, output effects, persistence,
  transport, and application are separate responsibilities.
- Avoid framework magic. Plain Java is a feature of this codebase.
- Return a lazy, resource-backed sequence as a `Stream` the caller closes, as
  `Files.lines` does, not as a hand-written cursor. Pair it with the revision
  it reads at in a `Read<Stream<T>>`.
- Model absence with `Optional`, never `null`, including record components:
  a record's accessors are its API's return types.
- Declare an interface only for an SPI or a family with several
  implementations; otherwise expose a `final class`.
- Return `CompletionStage` from operations that finish later.
- Log through `System.Logger`; no logging library is a dependency. A record is
  a fixed message plus fields passed as `Map.entry(key, value)` parameters:
  `Optional` values appear only when present, `Duration` values render as
  durations, and a `Throwable` parameter is the record's error. The node id is
  added by the process, never at call sites.
- Write exception messages as lower-case fragments with no trailing period
  (`"failed to open table " + path`), keeping acronyms such as `WAL` as they
  are, so a wrapped error reads as one line. Log messages are short
  sentence-case event names (`"Installed snapshot"`) with details in fields.

## Module Intent

The short version:

- `partdb-bytes`: shared immutable byte values
- `partdb-raft`: pure Raft core and the durable Raft runtime
- `partdb-storage`: local LSM-backed state store
- `partdb-node`: PartDB state machine and node API
- `partdb-grpc`: public protobuf schemas
- `partdb-client`: Java client API
- `partdb-server`: gRPC services, Raft peer transport, and process composition
- `partdb-app`: CLI and executable packaging
- `partdb-benchmarks`: JMH benchmarks

See [modules.md](modules.md) for detailed module charters, dependency rules,
and keep/merge/split decisions.

## Design Rules

1. Public APIs must only expose semantics PartDB currently guarantees.
2. Every distributed feature needs a restart, failover, and snapshot story.
3. Static clusters should be boring before dynamic membership exists.
4. Linearizable multi-key reads must use one consistent read point.
5. Recovery formats should be versioned before they become hard to change.
