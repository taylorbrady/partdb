# PartDB Vision

## What is PartDB?

PartDB is a distributed state store built in pure Java. Under the hood it's a strongly consistent key-value store with real-time watches, leases, and distributed locks — the primitives needed for coordination, configuration, and AI agent infrastructure.

**One-liner**: Real-time state for applications and AI agents. Strongly consistent. Embeddable. Open source.

---

## The Problem

Modern applications need distributed state, but current options force painful trade-offs:

| Solution | Trade-off |
|----------|-----------|
| etcd | Go-centric, not embeddable, ops burden |
| Redis | Not strongly consistent, memory-bound |
| DynamoDB | Vendor lock-in, eventually consistent by default, no ordered scans |
| Firebase | Proprietary, scaling limits, no self-host |
| ZooKeeper | Ancient API, operational complexity |

**No existing solution offers**: strong consistency + real-time watches + embeddable + open source + serverless-friendly.

### The AI Agent Gap

AI agents are proliferating, but their infrastructure is immature:

| Need | Current Hack | Problem |
|------|--------------|---------|
| Durable memory | Vectors + Postgres | Fragmented, no unified model |
| Workflow checkpoints | Temporal, or nothing | Overkill or nothing |
| Multi-agent coordination | Redis locks, polling | Brittle, no real primitives |
| Human-in-the-loop | Custom app code | Everyone rebuilds approval flows |

Agent frameworks (LangGraph, CrewAI, AutoGen) are maturing, but the state infrastructure underneath them hasn't caught up.

---

## PartDB's Position

```
                    Strongly      Ordered     Real-time    Open      Embeddable   Agent
                    Consistent    Scans       Watches      Source                 Ready

etcd                Yes           Yes         Yes          Yes       No           No
Redis               No            No          Pub/sub      Partial   No           No
DynamoDB            Opt-in        No          No           No        No           No
Firebase            Partial       No          Yes          No        No           Partial
Temporal            N/A           N/A         N/A          Yes       No           Partial

PartDB              Yes           Yes         Yes          Yes       Yes          Yes
```

---

## Architecture

PartDB follows a two-layer architecture: a simple, solid core with higher-level libraries built on top.

```
┌─────────────────────────────────────────────────────┐
│  Agent Libraries (Python, JS, Java)                 │
│  Memory, Checkpoints, Presence, Workflow helpers    │
├─────────────────────────────────────────────────────┤
│  PartDB Core (Java server)                          │
│  KV, Watch, Lease, Lock, Scan                       │
└─────────────────────────────────────────────────────┘
```

**Why this split?**
- The core is hard (consensus, storage, concurrency) — do it once, do it right
- Agent patterns are evolving — iterate on libraries without changing the server
- Simpler core = easier to test, embed, and operate
- Users can use just the primitives or the full agent libraries

---

## Core Primitives

The server provides five primitives. Everything else is built on top.

| Primitive | Purpose |
|-----------|---------|
| **KV** | Put, get, delete with revision tracking |
| **Scan** | Ordered range queries over keys |
| **Watch** | Real-time change notifications from any revision |
| **Lease** | TTL-based expiration with heartbeat |
| **Lock** | Distributed mutex with fencing tokens |

```java
// Core API
public interface PartDB {
    // KV
    long put(Slice key, Slice value);
    long put(Slice key, Slice value, Lease lease);
    Optional<Entry> get(Slice key);
    long delete(Slice key);

    // Scan
    Stream<Entry> scan(Slice start, Slice end);

    // Watch
    Watcher watch(Slice prefix, long fromRevision, Consumer<WatchEvent> callback);

    // Lease
    Lease createLease(Duration ttl);
    void keepAlive(Lease lease);
    void revokeLease(Lease lease);

    // Lock
    Lock lock(Slice key);
}
```

---

## Core Principles

### 1. Strongly Consistent
Raft consensus ensures linearizable operations. When a write returns, it's durable and visible to all readers. No "eventual consistency" surprises.

### 2. Real-time by Default
Watches are first-class. Subscribe to key prefixes and receive every change, in order, with no gaps. Build live UIs without polling.

### 3. Embeddable
Run a full cluster in-process for testing, or as a library in your application. Same API whether embedded or connecting to external cluster.

### 4. Java-Native
Built in pure Java for the JVM ecosystem. Debug with familiar tools. Profile with JFR. Deploy with GraalVM native-image.

### 5. Simple Core, Extensible Libraries
The server does five things well. Agent-specific abstractions live in client libraries that can evolve independently.

---

## Target Use Cases

### AI Agent Infrastructure
Durable state for AI agents: memory, checkpoints, coordination, and human-in-the-loop.

```python
from partdb.agents import Memory, Checkpoints

# Structured agent memory (built on KV + Scan)
memory = Memory(db, agent_id="support-bot")
memory.remember("user:123:preference", {"tone": "formal"}, ttl=days(30))
facts = memory.query(prefix="user:123:", since=hours(24))

# Durable checkpoints (built on KV)
async with Checkpoints(db, "refund:" + order_id) as cp:
    purchase = await cp.step("verify", verify_purchase)
    await cp.step("process", process_refund)
```

### Configuration & Feature Flags
Store application configuration with real-time updates. No polling, no stale cache.

```java
db.put(key("flags/new-checkout"), value(percentage(10)));
db.watch(prefix("flags/"), event -> flagCache.update(event));
```

### Coordination & Leader Election
Distributed locking with fencing tokens. Leader election built on leases.

```java
try (Lock lock = db.lock(key("resource/123")).acquire()) {
    processWithFencingToken(lock.fencingToken());
}
```

### Service Discovery
Register services with automatic health-based deregistration.

```java
Lease lease = db.createLease(Duration.ofSeconds(30));
db.put(key("services/api/" + instanceId), value(endpoint), lease);
db.watch(prefix("services/api/"), registry::update);
```

### Real-time Collaboration
Presence, cursors, live updates for collaborative applications.

```java
db.put(key("doc/123/cursors/" + userId), value(position), lease);
db.watch(prefix("doc/123/cursors/"), event -> renderCursor(event));
```

---

## Agent Libraries

Client libraries that compose the core primitives into agent-friendly abstractions.

| Library | Built On | Purpose |
|---------|----------|---------|
| **Memory** | KV, Scan | Structured facts with metadata, time-range queries |
| **Checkpoints** | KV | Durable execution state, crash recovery |
| **Presence** | KV, Lease, Watch | Agent discovery, health tracking |
| **Workflow** | KV, Watch, Lock | Orchestration helpers, approval gates |

These are **client-side libraries**, not server features. The server stays simple.

```python
# Python client with agent libraries
from partdb import PartDB
from partdb.agents import Memory, Checkpoints, Presence

db = PartDB(url="partdb://localhost:9000")

memory = Memory(db, agent_id="researcher")
checkpoints = Checkpoints(db, workflow_id="analysis-123")
presence = Presence(db, agent_type="researcher")
```

---

## Differentiation

### vs. etcd
- Java-native (not Go with gRPC bindings)
- Embeddable (run in-process for testing)
- Agent libraries for AI use cases
- Simpler operational model

### vs. Redis
- Strongly consistent (not eventually consistent)
- Ordered range scans (keys are sorted)
- Durable by default (not memory-only)
- True distributed consensus (not Redis Cluster)

### vs. Temporal
- Much simpler (5 primitives vs full workflow engine)
- Embeddable (Temporal requires infrastructure)
- Agent-native (not enterprise workflow-centric)
- Lower overhead for simple use cases

### vs. Agent Framework Built-ins
- Durable (survives crashes, not just in-memory)
- Consistent (real consensus, not optimistic)
- Framework-agnostic (works with LangGraph, CrewAI, etc.)

---

## Technical Foundation

### Storage: LSM Tree
Log-structured merge tree for write-optimized persistent storage. Efficient range scans, compression, and compaction.

### Consensus: Raft
Proven consensus protocol for leader election, log replication, and membership changes. Battle-tested design.

### No MVCC
Single-version storage. Watches provide change history; snapshots aren't needed without transactions. Simpler, faster.

---

## Roadmap

### Phase 1: Solid Core (Current)
- [x] Raft consensus
- [x] LSM storage
- [ ] Clean KV API (put/get/delete/scan)
- [ ] Watches with revision tracking
- [ ] Leases (TTL, heartbeat)
- [ ] Locks with fencing tokens
- [ ] Single JAR deployment

### Phase 2: Usable
- [ ] gRPC API
- [ ] Python client
- [ ] Embeddable mode (in-process for testing)
- [ ] Metrics and observability
- [ ] Docker image

### Phase 3: Agent Libraries
- [ ] Memory library (Python)
- [ ] Checkpoints library (Python)
- [ ] Presence library (Python)
- [ ] LangGraph integration example
- [ ] Documentation and tutorials

### Phase 4: Production Ready
- [ ] JavaScript/TypeScript client
- [ ] Workflow helpers
- [ ] Benchmarks vs etcd
- [ ] Security (TLS, auth)

### Phase 5: Managed Service
- [ ] PartDB Cloud
- [ ] Multi-region support
- [ ] Enterprise features

---

## Market Opportunity

### Target Segments
1. **AI/Agent builders** — need durable state for agents without duct-taping Redis + Postgres
2. **Java backend developers** — need distributed state without leaving JVM ecosystem
3. **Platform teams** — replacing ZooKeeper/etcd with modern tooling
4. **Startups** — Firebase alternative without lock-in

### Business Model
- **Open source core** — Apache 2.0, build community
- **Agent libraries** — Open source, drive adoption
- **Managed service** — PartDB Cloud, usage-based pricing
- **Enterprise** — Support contracts, compliance features

---

## Why Now?

1. **AI agents are real** — LangGraph, CrewAI, AutoGen are in production. Infrastructure hasn't caught up.
2. **Modern Java** — Java 21+ with virtual threads, records, sealed types makes this viable
3. **Real-time expectations** — Users expect live updates, polling is dead
4. **Cloud skepticism** — Vendor lock-in concerns drive open source adoption
5. **No incumbent for agent state** — Everyone's hacking it together. First good solution wins.

---

## What We're Not Building

To stay focused, we explicitly deprioritize:

- **Full MVCC / snapshot isolation** — not needed without transactions
- **Multi-key transactions** — adds complexity, not core value
- **Vector search** — different problem, use Pinecone/pgvector
- **Full workflow engine** — we're not Temporal; just provide primitives
- **SQL / document query layer** — different product
- **Multi-Raft / sharding** — later, after single-group is solid

---

## Summary

PartDB is a strongly consistent, real-time state store with first-class support for AI agents. The core is simple (five primitives), the agent libraries make it useful for the emerging AI infrastructure market.

**Two-layer strategy:**
1. **Core** — Compete on fundamentals: consistency, reliability, embeddability, Java-native
2. **Agent libraries** — Capture the emerging market: durable memory, checkpoints, coordination

**The goal**: Become the default state infrastructure for AI agents, starting with the JVM ecosystem and expanding from there.
