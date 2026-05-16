# Module Boundaries

PartDB uses Gradle modules to separate domain concepts, technology adapters, and
process assembly. A module should exist only when it owns a stable concept,
isolates an external technology, or assembles a runnable process.

Small modules are acceptable when they protect an important boundary. Thin
modules are suspect when they only forward calls without owning a responsibility.

## Dependency Direction

The intended dependency direction is:

```text
partdb-app
  -> partdb-server
  -> partdb-client

partdb-server
  -> partdb-node
  -> partdb-consensus
  -> partdb-transport-grpc

partdb-transport-grpc
  -> partdb-node
  -> partdb-grpc
  -> partdb-raft

partdb-client
  -> partdb-grpc
  -> partdb-bytes

partdb-node
  -> partdb-consensus
  -> partdb-storage
  -> partdb-bytes

partdb-consensus
  -> partdb-raft
  -> partdb-bytes

partdb-storage
  -> partdb-bytes

partdb-raft
  -> partdb-bytes
```

Rules:

- Core modules must not depend on gRPC, CLI, process lifecycle, logging
  configuration, or generated protobuf classes.
- `partdb-grpc` must not depend on runtime modules.
- `partdb-client` must not depend on `partdb-node`, `partdb-server`, or
  consensus internals.
- `partdb-storage` and `partdb-raft` must remain independently testable.
- `partdb-app` is allowed to depend on client and server modules because it is a
  command-line assembly point.

## Module Charters

### `partdb-bytes`

Role: shared immutable byte value type.

Decision: keep.

This module is intentionally small. It prevents `byte[]` mutability and ad hoc
encoding helpers from leaking through the system.

Allowed dependencies: Java standard library only.

Must not own: keys, values, storage records, protocol messages, or database
semantics.

### `partdb-raft`

Role: pure Raft algorithm and Raft domain types.

Decision: keep.

This is a core module. It owns the Raft protocol state machine: one explicit
`RaftInput` in, one `RaftEffects` description out. It should stay deterministic
and side-effect-free. Persistence, IO, clocks, threads, application execution,
and network transport belong outside this module.

Allowed dependencies: `partdb-bytes`.

Must not own: files, sockets, gRPC, thread pools, PartDB KV commands, storage
engine details, or process lifecycle.

### `partdb-consensus`

Role: durable, effectful Raft runtime and consensus membership model.

Decision: keep.

This module adapts the pure Raft core to persistence, transport, snapshots,
read barriers, proposal tracking, state-machine application, and the
runtime-facing `ConsensusMembership` model. It is generic over a
`ReplicatedStateMachine`.

Allowed dependencies: `partdb-raft`, `partdb-bytes`.

Must not own: PartDB KV semantics, public gRPC schemas, CLI behavior, or storage
engine internals.

Watch point: do not let PartDB-specific command/result handling move into this
module.

### `partdb-storage`

Role: local durable versioned state store.

Decision: keep.

This module owns the LSM engine, memtables, SSTables, manifests, compaction,
block cache, checkpoints, and storage-level tests/benchmarks.

Allowed dependencies: `partdb-bytes`.

Must not own: Raft, cluster membership, node lifecycle, gRPC, or PartDB command
encoding.

### `partdb-node`

Role: replicated PartDB node domain.

Decision: keep.

This module composes the consensus runtime with the PartDB KV state machine. It
owns node-facing domain APIs such as KV operations, cluster status, maintenance,
backup, and recovery.

Allowed dependencies: `partdb-consensus`, `partdb-storage`, `partdb-bytes`.

Must not own: gRPC service implementations, CLI parsing, process ports, JMX
registration, or transport endpoint parsing.

Watch point: if the public Java API grows, consider a separate API package
inside this module before creating another module.

### `partdb-grpc`

Role: public protobuf schemas and generated gRPC classes.

Decision: keep.

This module is a technology boundary. It provides generated classes to clients
and server-side adapters without depending on the runtime.

Allowed dependencies: gRPC/protobuf libraries only.

Must not own: Java client behavior, node logic, storage logic, or Raft runtime
logic.

### `partdb-transport-grpc`

Role: gRPC adapters for public APIs and Raft peer transport.

Decision: keep for now, split later only if it becomes confusing.

This module currently contains two gRPC adapters:

- public KV/cluster service adapters
- internal Raft peer transport adapter

That is acceptable while both are small and share gRPC infrastructure. If either
side grows substantially, split into public API transport and Raft transport
modules.

Allowed dependencies: `partdb-grpc`, `partdb-node`, `partdb-consensus`,
`partdb-raft`.

Must not own: process assembly, CLI behavior, storage internals, or consensus
runtime policy.

Watch point: `GrpcServer` is public API transport; `GrpcRaftTransport` is
internal replication transport. Keep their responsibilities separate even while
they live in one module.

### `partdb-client`

Role: Java client API.

Decision: keep.

This module exposes typed Java APIs over the public gRPC protocol.

Allowed dependencies: `partdb-grpc`, `partdb-bytes`, gRPC/protobuf libraries.

Must not own: server/node implementation details, consensus concepts, storage
concepts, or generated-code leakage in public APIs.

### `partdb-server`

Role: process runtime assembly.

Decision: keep, but hold it to this charter.

This module is thin by design if it owns process composition: create a node,
wire the Raft transport, start public gRPC, expose health endpoints, register
observability, and shut everything down in order.

Allowed dependencies: `partdb-node`, `partdb-consensus`,
`partdb-transport-grpc`.

Must not own: CLI parsing, client commands, storage internals, Raft algorithm
logic, or public Java client behavior.

Merge condition: if this module stops owning health/observability/lifecycle and
only wraps `GrpcServer`, fold it into `partdb-app`.

### `partdb-app`

Role: executable CLI.

Decision: keep.

This module parses commands, starts servers, and acts as an operational client.

Allowed dependencies: `partdb-server`, `partdb-client`.

Must not own: node internals, consensus internals, storage internals, or gRPC
service implementations.

### `partdb-benchmarks`

Role: benchmark assembly.

Decision: keep.

This module should remain outside production runtime dependency paths.

Allowed dependencies: benchmark targets and JMH.

Must not own: product code.

## Current Keep/Merge/Split Decisions

- Keep `partdb-bytes` despite being small because it owns a foundational value
  boundary.
- Remove `partdb-cluster`; its only type was consensus runtime membership, which
  now lives in `partdb-consensus` as `ConsensusMembership`.
- Keep `partdb-server` only as the process runtime assembly module.
- Keep `partdb-transport-grpc` as one module for now. Revisit a split when
  public API transport or Raft peer transport grows.
- Do not introduce more modules until a responsibility cannot be expressed by a
  package boundary inside an existing module.

## Next Boundary Risks

1. `partdb-consensus` should remain generic over replicated state machines.
2. `partdb-node` should stay protocol-independent.
3. `partdb-server` should not become a second application layer.
4. `partdb-transport-grpc` should not become a process assembly module.
5. `partdb-grpc` should continue to expose only implemented v0 semantics.
