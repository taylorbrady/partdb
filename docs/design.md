# PartDB Design Direction

PartDB is a small, modern Java implementation of a Raft-backed key-value store.
The project favors a clear distributed-systems core over a large feature surface.

## V0 Scope

PartDB v0 supports:

- durable key-value storage
- static Raft clusters
- linearizable `get`, `put`, `delete`, `scan`, and `batchWrite`
- local/stale reads where the caller explicitly asks for them
- status and member inspection
- snapshots and recovery for the replicated state machine

PartDB v0 does not support:

- leases or TTLs
- compare-and-swap or conditional writes
- dynamic cluster membership changes
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

## Module Intent

The short version:

- `partdb-bytes`: shared immutable byte values
- `partdb-cluster`: cluster membership domain model
- `partdb-raft`: pure Raft state machine and Raft domain types
- `partdb-consensus`: durable, effectful Raft runtime
- `partdb-storage`: local LSM-backed state store
- `partdb-node`: PartDB state machine and node API
- `partdb-grpc`: public protobuf schemas
- `partdb-transport-grpc`: gRPC server and Raft peer transport
- `partdb-client`: Java client API
- `partdb-server`: process-level server composition
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
