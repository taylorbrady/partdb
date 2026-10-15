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

- `io.partdb.raft` is the pure core: one `RaftInput` in, one `RaftEffects`
  out. It is deterministic and side-effect free. `RaftCorePurityTest` keeps
  JDK IO, clocks, threads, and runtime imports out of it.
- `io.partdb.raft.runtime` adapts the core to a segmented write-ahead log,
  file-based snapshots, a transport SPI with streamed snapshot transfer, read
  barriers, proposal tracking, and state-machine application. It is generic
  over a `StateMachine`.

Membership, role, and status are modeled once (`RaftMembership`, `RaftRole`,
`RaftStatus`) and used as-is by the node API.

Allowed dependencies: `partdb-bytes`, SLF4J.

Must not own: gRPC, sockets, PartDB KV commands, storage engine details, or
process lifecycle.

### `partdb-storage`

Role: local durable versioned state store.

This module owns the LSM engine, memtables, SSTables, manifests, compaction,
block cache, streamed checkpoints, and storage-level tests/benchmarks.

Allowed dependencies: `partdb-bytes`.

Must not own: Raft, cluster membership, node lifecycle, gRPC, or PartDB command
encoding.

### `partdb-node`

Role: replicated PartDB node domain.

This module composes the Raft runtime with the PartDB KV state machine. It
owns node-facing domain APIs: KV operations, cluster status, backup, and
restore. Packages: `io.partdb.node` (node, config, admin and cluster views),
`io.partdb.node.kv` (KV API types), and `io.partdb.node.state` (state machine
and command codecs).

Allowed dependencies: `partdb-raft`, `partdb-storage`, `partdb-bytes`.

Must not own: gRPC service implementations, CLI parsing, process ports, JMX
registration, or transport endpoint parsing.

### `partdb-grpc`

Role: public protobuf schemas and generated gRPC classes.

This is the wire contract shared by the client and the server. It does not
depend on the runtime.

Allowed dependencies: gRPC/protobuf libraries only.

Must not own: Java client behavior, node logic, storage logic, Raft peer
protocol, or Raft runtime logic.

### `partdb-client`

Role: Java client API.

This module exposes typed Java APIs over the public gRPC protocol. Its test
fixtures hold `TestCluster`, the shared driver for both cluster test suites.

Allowed dependencies: `partdb-grpc`, `partdb-bytes`, gRPC/protobuf libraries.

Must not own: server/node implementation details, Raft concepts, storage
concepts, Raft peer transport endpoints, or generated-code leakage in public
APIs.

### `partdb-server`

Role: the PartDB server process.

This is the one place gRPC is implemented on the server side:

- `io.partdb.server` assembles the process: create a node, wire the Raft
  transport, start public gRPC, expose health endpoints, register JMX, and
  shut everything down in order.
- `io.partdb.server.grpc` implements the public KV and cluster services.
- `io.partdb.server.peer` implements `RaftTransport` over a private gRPC
  protocol (`raft.proto`), including chunked snapshot streaming with flow
  control.

Allowed dependencies: `partdb-node`, `partdb-grpc`, gRPC/protobuf libraries.

Must not own: CLI parsing, client commands, storage internals, or Raft
algorithm logic.

### `partdb-app`

Role: executable CLI.

This module parses commands, starts servers, and acts as an operational client.

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
