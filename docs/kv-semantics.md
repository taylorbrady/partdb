# Key-Value Semantics

PartDB is a strongly consistent operational key-value store. The key-value API
is designed for metadata, coordination, control-plane state, and service-local
replicated state.

## Revisions

A revision is the durable ordering token for visible key-value changes.

- Every successful write command creates one revision.
- A batch write applies all operations at the same revision.
- A successful conditional transaction applies all operations at the same
  revision.
- A failed conditional transaction does not mutate state and does not create a
  visible key-value revision.
- Revisions are monotonically increasing and currently correspond to the
  consensus log index that committed the write.
- Reads return the revision at which each key was last modified.
- Backups record the state-machine index they include.

## Reads

Reads are local storage reads with an explicit consistency mode.

- `LINEARIZABLE` reads first pass through a consensus read barrier, then read
  local storage.
- `LOCAL` reads skip the consensus read barrier and may observe stale local
  state.
- The default read consistency is `LINEARIZABLE`.

## Writes

Writes are submitted through consensus before they mutate storage.

- `put` stores a value for a key.
- `delete` records a tombstone for a key.
- `write` applies a non-empty batch of unique-key operations atomically at one
  revision.

## Conditional Transactions

Conditional transactions compare the current key-value state and apply a
non-empty write batch only when all conditions match.

Supported conditions:

- key exists
- key is missing
- key value equals an expected value
- key revision equals an expected revision

If every condition matches, PartDB applies the transaction writes at one
revision and returns a successful transaction result with that revision. If any
condition fails, PartDB applies no writes and returns an unsuccessful transaction
result without a revision.

Failed conditional transactions may still be committed in the consensus log so
all replicas make the same decision, but they do not create a visible
key-value revision.

## Future Semantics

The next product-level semantics should build on revisions:

- watches that resume from a revision
- compaction that bounds historical watch state
- leases and TTL-backed ephemeral keys
- richer transaction operations if needed by control-plane workloads
