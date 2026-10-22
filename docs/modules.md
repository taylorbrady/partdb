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
  to apply, request `Outcome`s, and snapshot requests. Messages come in two
  lists: `messages` may be sent before the `Ready` is persisted, and
  `persistedMessages` only after. Only a leader's appends and heartbeats in a
  `Ready` that keeps the term and vote are immediate, so the leader writes to
  its own log while its followers write to theirs; acknowledgments, votes, and
  vote requests promise durable state and wait. The leader counts its own copy
  toward commit only once `Input.Persisted` reports it, and each `Ready` must
  be persisted before the next `ready()`. Every request (proposal,
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
- `io.partdb.raft.runtime` drives the core from one platform-thread event loop:
  for each `ready` it sends the immediate messages, persists with one sync,
  then sends the persisted messages, all one-way through the `RaftTransport`
  SPI. It applies committed entries to a `StateMachine<R>` (every
  index reaches it: commands through `apply`, no-ops and configuration entries
  through `skip`), and completes proposals, read barriers, membership changes,
  and leadership transfers. `Requests` owns every pending future; `Snapshots`
  owns generating snapshots for lagging peers, staging received ones, and
  installing them. Storage is a versioned, checksummed write-ahead log replayed
  into memory; every file create, rename, and delete is followed by a
  directory sync. The runtime holds an exclusive `LOCK` on its directory for
  as long as it is open, and the directory is readable only by its owner. The
  `RaftConfig`'s `Bootstrap` (cluster id and initial membership) is used once:
  the first open records it with the node id in an `IDENTITY` file. Later
  opens use the recorded cluster id and membership, refuse a directory recorded
  for another node, and hand the recorded cluster id to the transport when it
  starts.
  The state machine is the snapshot: there is no on-disk snapshot copy. The
  `StateMachine` reports a `durableIndex()`, and the runtime compacts the log
  only behind it, keeping the last `LogRetention` entries and bytes for peers
  that are a little behind. The WAL records the log's base (index, term,
  membership). When the undurable tail outgrows the retention, the runtime asks
  the state machine to `flush()`. A peer that falls behind the log base gets a
  snapshot the leader generates on demand from the state machine, streams from
  a temporary file, and deletes once every transfer using it ends. A follower
  installs a received snapshot by renaming it into a durable install file,
  resetting the WAL to the snapshot's base, restoring the state machine, and
  deleting the file; reopening finishes an install interrupted between those
  steps.

Membership, role, and status are modeled once (`Membership`, `Role`,
`RaftStatus`) and used as-is by the node API.

Allowed dependencies: `partdb-bytes`.

Must not own: gRPC, sockets, PartDB KV commands, storage engine details, or
process lifecycle.

### `partdb-storage`

Role: local durable versioned state store.

This module owns the LSM engine, memtables, tables, manifests, compaction,
block cache, streamed checkpoints, and storage-level tests/benchmarks.

A `Store` holds an exclusive `LOCK` on its directory while open, and the
directory is readable only by its owner.

Its public surface is `Store` and a handful of values (`Snapshot`,
`KeyValue`, `Mutation`, `KeyRange`, `StoreOptions`, `StoreStats`,
`StoreException`); everything else is package-private in `io.partdb.storage`.
Reads go through a `Snapshot`, which reports the revision it reads at; a scan
is a `Stream<KeyValue>` that pins its own view and must be closed.

Contract with the replicated state machine:

- A single writer calls `apply(revision, mutations)` with strictly increasing
  revisions, including empty mutation lists for entries that change nothing,
  so `appliedRevision()` always equals the last Raft index handed to storage.
- There is no storage WAL; the Raft log is the WAL. Flushes persist the
  revision of each memtable as the manifest's durable revision, and reopening
  resumes at `appliedRevision() == durableRevision()`.
- `checkpoint` streams the state at exactly `appliedRevision()` from any
  thread; `restore` writes the checkpoint's tables beside the live ones and
  swaps to them with one manifest commit.
- The manifest is the only commit point; tables it does not reference are
  deleted on open.

The engine is a leveled LSM in the RocksDB mould. Tables hold one version per
key; only memtables keep several, so snapshots can read past concurrent
applies. Level targets are sized dynamically from the largest level, level
zero compacts as a whole into the base level, writes stall when level zero
reaches `levelZeroStopTrigger`, and a table with nothing beneath it moves down
a level without being rewritten. Flush and compaction run on platform threads;
tables are memory-mapped through the FFM API.

Allowed dependencies: `partdb-bytes`.

Must not own: Raft, cluster membership, node lifecycle, gRPC, or PartDB command
encoding.

### `partdb-node`

Role: replicated PartDB node domain.

This module composes the Raft runtime with the PartDB KV state machine and adds
what replication means for keys: transactions, conditions, read consistency,
limits, and the replicated command format. `Node` is the whole API: blocking
`get`, `scan`, `write`, and `transact` with explicit timeouts, plus `status`,
`membership`, `stepDown`, and `metrics`. It speaks storage's key-value
vocabulary (`KeyValue`, `Mutation`, `KeyRange`, `StoreStats`) and
Raft's cluster vocabulary as-is. Every write is one `Txn` (conditions plus
mutations); `write` is an unconditional transaction. Reads return the revision
they read at, and a scan is a lazy `Stream<KeyValue>` the caller closes when it
has enough. Failures are one sealed `NodeException` family that callers switch
over but never construct. `KvLimits` are enforced before a command is
proposed, so oversized or invalid commands never reach the log. Everything is
in `io.partdb.node`; `KvStateMachine` and the versioned, strictly decoded
`TxnCodec` are package-private, so nothing outside the module can touch the
log format.

A node's data directory holds one directory per engine:

```text
<data-dir>/
  raft/   LOCK, IDENTITY, wal/000001.wal ..., snap/ (snapshots in transit)
  db/     LOCK, MANIFEST, 000001.sst ...
```

Metadata files are upper case; numbered files are zero-padded decimal.

Allowed dependencies: `partdb-raft`, `partdb-storage`, `partdb-bytes`.

Must not own: gRPC service implementations, CLI parsing, process ports, metrics
exposition, or transport endpoint parsing.

### `partdb-grpc`

Role: public protobuf schemas and generated gRPC classes.

This is the wire contract shared by the client and the server
(`partdb.kv.v1`, `partdb.cluster.v1`, `partdb.error.v1`). Errors travel as gRPC
status codes with a `google.rpc.ErrorInfo` detail whose reason is a
`partdb.error.v1.ErrorReason` name, plus a typed `LeaderHint` detail on
`NOT_LEADER`; see [kv-semantics.md](kv-semantics.md). It
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
retrying it. Reads return the revision they read at as a `Read<T>`: `get`
yields `Read<Optional<KeyValue>>` and `scan` yields `Read<Stream<KeyValue>>`,
the same shapes the node API uses; closing a scan's stream cancels the gRPC
call. Failures are one sealed `PartDbException` family.
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
  transport, start public gRPC, serve health checks and Prometheus metrics on
  the admin port, and shut everything down in order, stepping down first when
  the node leads.
- `io.partdb.server.grpc` implements the public KV and cluster services.
- `io.partdb.server.peer` implements `RaftTransport` over a private gRPC
  protocol (`raft.proto`, package `partdb.peer.v1`): one long-lived,
  flow-controlled message stream per peer, plus chunked snapshot uploads with
  a size-scaled deadline. Every call carries the cluster id, sender, and
  recipient; a peer rejects calls for another cluster or another node. The
  transport learns the cluster id from the Raft runtime when it starts, so a
  node always speaks for the cluster its directory belongs to. Without
  `--cluster-id`, a new cluster's id is derived from its node ids and peer
  addresses, so separate clusters with the same node names do not share one. The
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
It owns logging configuration: `Logging` installs one stdout handler on the JUL
root logger with `LogFormatter` (text or JSON, stamped with the node id), so
PartDB's `System.Logger` calls, gRPC, and Netty all share one output.
`ProcessLogManager`, selected by the start scripts, keeps that handler through
JVM shutdown, which the default `LogManager` would reset before the server's
final records are written.
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
- `partdb-node` uses storage's key-value types the way it uses Raft's cluster
  types; it does not keep its own copies. Only `partdb-client`, which cannot
  depend on the engine, models them again.
- Do not add a type per layer for a concept another layer already models.
- Do not introduce more modules until a responsibility cannot be expressed by a
  package boundary inside an existing module.

## Next Boundary Risks

1. The Raft runtime should remain generic over state machines.
2. `partdb-node` should stay protocol-independent.
3. `partdb-server` should not grow a second application layer.
4. `partdb-grpc` should continue to expose only implemented v0 semantics.
