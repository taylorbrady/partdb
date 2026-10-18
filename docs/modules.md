# Module Boundaries

PartDB uses Gradle modules only where a boundary pays for itself. A module
should exist when it is one of:

- an engine that can be tested and reused without knowing it is PartDB
  (`partdb-storage`, `partdb-raft`);
- a contract shared by two sides that must not depend on each other
  (`partdb-bytes`, `partdb-grpc`);
- a deployable or published artifact (`partdb-client`, `partdb-app`);
- a layer whose boundary stops a mistake that is otherwise easy to make
  (`partdb-node` keeps gRPC and protobuf out of KV semantics).

Everything else is a package inside one of these modules. When a guarantee
cannot be expressed by a module boundary, write a test for it, as
`RaftCorePurityTest` does for the pure Raft core.

## Dependency Direction

```text
partdb-app
  -> partdb-server
  -> partdb-client

partdb-server
  -> partdb-node
  -> partdb-grpc

partdb-client
  -> partdb-grpc
  -> partdb-bytes

partdb-node
  -> partdb-raft
  -> partdb-storage

partdb-raft
  -> partdb-bytes

partdb-storage
  -> partdb-bytes
```

Rules:

- `partdb-raft`, `partdb-storage`, and `partdb-node` must not depend on gRPC,
  CLI code, process lifecycle, logging configuration, or generated protobuf
  classes.
- `partdb-grpc` must not depend on runtime modules.
- `partdb-client` must not depend on `partdb-node`, `partdb-server`, or Raft.
- `partdb-storage` and `partdb-raft` must remain independently testable.
- `partdb-app` may depend on client and server modules because it is the
  command-line assembly point.

## Module Charters

### `partdb-bytes`

Role: shared immutable byte value type.

`Bytes` is part of the client's public API, so it must be an artifact the
client can depend on without pulling in storage or Raft. It prevents `byte[]`
mutability and ad hoc encoding helpers from leaking through the system.
Adapters convert with one copy in (`Bytes.copyOf(ByteBuffer)` or
`Bytes.copyOf(MemorySegment)`) and zero copies out
(`asReadOnlyByteBuffer()`, wrapped with `UnsafeByteOperations` for protobuf).

Allowed dependencies: Java standard library only.

Must not own: keys, values, storage records, protocol messages, or database
semantics. Bulk data such as snapshots and checkpoints never travels as
`Bytes`; it is streamed through files.

### `partdb-raft`

Role: Raft consensus, from the pure algorithm to the durable runtime.

The module has two packages:

- `io.partdb.raft` is the pure core. `Raft.step(Input)` mutates state and
  `Raft.ready()` drains everything that must happen next as one `Ready`: hard
  state, snapshot and entries to persist, messages to send, committed entries
  to apply, request `Outcome`s, and snapshot requests. Every request (proposal,
  read, membership change, leadership transfer) carries an id and ends in
  exactly one outcome; a rejection names its `Rejection` reason. Any number of
  inputs batch into one `ready`. The leader tracks each peer with a `Progress`
  (probe, replicate with an inflight window, snapshot), confirms reads with
  heartbeat rounds, and supports pre-vote, check-quorum, learners,
  single-server membership changes that take effect when appended, leader
  self-removal with hand-off once the change commits, and leadership transfer,
  either to a named voter or, on step-down, to the recently active voter with
  the most replicated log.
  Nodes are named by `NodeId`. It is deterministic and side-effect free;
  `RaftCorePurityTest` keeps JDK IO, clocks, threads, and runtime imports out
  of it.
- `io.partdb.raft.runtime` drives the core from one virtual-thread event loop:
  it persists each `ready` with one sync, sends one-way messages through the
  `RaftTransport` SPI, applies committed entries to a `StateMachine<R>` (every
  index reaches it: commands through `apply`, no-ops and configuration entries
  through `skip`), and completes proposals, read barriers, membership changes,
  and leadership transfers. `Requests` owns every pending future; `Snapshots`
  owns snapshot creation, compaction, leased sending, staging, and install.
  Storage is a versioned, checksummed write-ahead log replayed into memory plus
  file-based snapshots with a payload CRC; every file create, rename, and
  delete is followed by a directory sync. The first open records the cluster
  id and initial membership in a `cluster` file; later opens use that record
  and refuse a directory from another cluster. A `SnapshotPolicy` bounds the
  in-memory log by entries and by bytes; when either is reached the runtime
  asks the `StateMachine` for a point-in-time snapshot and compacts the log
  behind it. Lagging peers receive that snapshot as a stream from a lease that
  keeps the file alive until the transfer ends.

Membership, role, and status are modeled once (`Membership`, `Role`,
`RaftStatus`) and used as-is by the node API.

Allowed dependencies: `partdb-bytes`, SLF4J.

Must not own: gRPC, sockets, PartDB KV commands, storage engine details, or
process lifecycle.

### `partdb-storage`

Role: local durable versioned state store.

This module owns the LSM engine, memtables, tables, manifests, compaction,
block cache, streamed checkpoints, and storage-level tests/benchmarks.

Its public surface is `Store` and a handful of values (`Snapshot`, `Cursor`,
`KeyValue`, `Mutation`, `KeyRange`, `StoreOptions`, `StoreStats`,
`StoreException`); everything else is package-private in `io.partdb.storage`.

Contract with the replicated state machine:

- A single writer calls `apply(index, mutations)` with strictly increasing
  indexes, including empty mutation lists for entries that change nothing, so
  `appliedIndex()` always equals the last Raft index handed to storage.
- There is no storage WAL; the Raft log is the WAL. Flushes persist the applied
  index of each memtable as the manifest's durable index, and reopening resumes
  at `appliedIndex() == durableIndex()`.
- `checkpoint` streams the state at exactly `appliedIndex()` from any thread;
  `restore` stages the checkpoint into a new generation directory and swaps the
  `CURRENT` pointer atomically.
- The manifest is the only commit point within a generation; files it does not
  reference are deleted on open.

Allowed dependencies: `partdb-bytes`.

Must not own: Raft, cluster membership, node lifecycle, gRPC, or PartDB command
encoding.

### `partdb-node`

Role: replicated PartDB node domain.

This module composes the Raft runtime with the PartDB KV state machine. It
owns node-facing domain APIs: blocking KV operations with explicit timeouts,
cluster status, and metrics. Every write is one `Txn` (conditions plus
mutations); `put`, `delete`, and `write` are unconditional transactions. Reads
return the revision they read at. Failures are one sealed `NodeException`
family that callers can switch over exhaustively. `KvLimits` are enforced
before a command is proposed, so oversized or invalid commands never reach the
log. Packages: `io.partdb.node` (`Node`, `NodeConfig`, `NodeException`,
cluster view, metrics), `io.partdb.node.kv` (KV API types), and
`io.partdb.node.state` (`KvStateMachine` and the versioned, strictly decoded
`TxnCodec`).

Allowed dependencies: `partdb-raft`, `partdb-storage`, `partdb-bytes`.

Must not own: gRPC service implementations, CLI parsing, process ports, JMX
registration, or transport endpoint parsing.

### `partdb-grpc`

Role: public protobuf schemas and generated gRPC classes.

This is the wire contract shared by the client and the server
(`partdb.kv.v1`, `partdb.cluster.v1`). Errors travel as gRPC status codes with
a `google.rpc.ErrorInfo` detail; see [kv-semantics.md](kv-semantics.md). It
does not depend on the runtime.

Allowed dependencies: gRPC/protobuf libraries only.

Must not own: Java client behavior, node logic, storage logic, Raft peer
protocol, or Raft runtime logic.

### `partdb-client`

Role: Java client API.

This module exposes one blocking Java client, `PartDbClient`, with `kv()` and
`cluster()` facets over the public gRPC protocol. It follows `NOT_LEADER`
hints to the leader's endpoint (learned from each endpoint's status), retries
reads and requests the server reports as not applied within one deadline, and
reports any write that may have been applied as `OutcomeUnknown` instead of
retrying it. Reads return the revision they read at (`Read<T>`,
`ScanCursor.readRevision()`). Failures are one sealed `PartDbException` family.
`cluster().status(endpoint)` is a diagnostic view of one node built from the
wire format, so it reports Raft role, term, and indexes without depending on
Raft.

Allowed dependencies: `partdb-grpc`, `partdb-bytes`, gRPC/protobuf libraries.

Must not own: server/node implementation details, Raft types or mechanics in
the KV API, storage concepts, Raft peer transport endpoints, or generated-code
leakage in public APIs.

### `partdb-server`

Role: the PartDB server process.

This is the one place gRPC is implemented on the server side:

- `io.partdb.server` assembles the process: create a node, wire the Raft
  transport, start public gRPC, expose health endpoints, register JMX, and
  shut everything down in order, stepping down first when the node leads.
- `io.partdb.server.grpc` implements the public KV and cluster services.
- `io.partdb.server.peer` implements `RaftTransport` over a private gRPC
  protocol (`raft.proto`, package `partdb.peer.v1`): one long-lived,
  flow-controlled message stream per peer, plus chunked snapshot uploads with
  a size-scaled deadline. Every call carries the cluster id, sender, and
  recipient; a peer rejects calls for another cluster or another node. The
  server owns the transport and closes it after the node.

Allowed dependencies: `partdb-node`, `partdb-grpc`, gRPC/protobuf libraries.

Must not own: CLI parsing, client commands, storage internals, or Raft
algorithm logic.

### `partdb-app`

Role: executable CLI.

This module parses commands, starts servers, and acts as an operational client.
Each command (`StartCommand`, `KvCommand`, `ClusterCommand`) owns its parsing
and usage text; `PartDbApp` only routes namespaces. Server defaults come from
`ServerConfig`, so the CLI never imports Raft or node types.
Its test fixtures hold `TestCluster`, the shared driver for both cluster test
suites.

Allowed dependencies: `partdb-server`, `partdb-client`.

Must not own: node internals, Raft internals, storage internals, or gRPC
service implementations.

### `partdb-benchmarks`

Role: benchmark assembly.

This module should remain outside production runtime dependency paths.

Allowed dependencies: benchmark targets and JMH.

Must not own: product code.

## Decisions

- `partdb-consensus` was merged into `partdb-raft`. Its separate module
  guaranteed nothing the purity test does not, and its boundary kept producing
  duplicate role, status, and membership types.
- `partdb-transport-grpc` was merged into `partdb-server`. It mixed adapters
  for two layers (public KV over node, peer traffic over Raft) and had no
  consumer other than the server.
- `partdb-node` stays separate from `partdb-server` so protocol types cannot
  reach the KV state machine.
- Do not add a type per layer for a concept another layer already models.
- Do not introduce more modules until a responsibility cannot be expressed by a
  package boundary inside an existing module.

## Next Boundary Risks

1. The Raft runtime should remain generic over state machines.
2. `partdb-node` should stay protocol-independent.
3. `partdb-server` should not grow a second application layer.
4. `partdb-grpc` should continue to expose only implemented v0 semantics.
