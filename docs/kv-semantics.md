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

## Reads

Reads are local storage reads with an explicit consistency mode.

- `LINEARIZABLE` reads first pass through a consensus read barrier, then read
  local storage.
- `LOCAL` reads skip the consensus read barrier and may observe stale local
  state.
- The default read consistency is `LINEARIZABLE`.
- Every read returns the revision of the state it read.
- A scan reads one consistent view for its whole range and streams it in
  batches under gRPC flow control; `limit` bounds the number of keys and `0`
  means no limit.

## Writes

Writes are submitted through consensus before they mutate storage.

- `put` stores a value for a key.
- `delete` records a tombstone for a key.
- `write` applies a non-empty batch of unique-key operations atomically at one
  revision.
- Keys must be non-empty. `KvLimits` bound key size, value size, the number of
  mutations and conditions, and the total bytes of one transaction; requests
  over a limit are rejected before they are proposed.
- Every write returns the revision it created.

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

## Errors

The public API reports failures as gRPC status codes. Where the reason matters
to a client, the status carries a `google.rpc.ErrorInfo` detail in the
`partdb.io` domain:

| Status | `ErrorInfo.reason` | Meaning |
| --- | --- | --- |
| `UNAVAILABLE` | `NOT_LEADER` | The node is not the leader; `leader_id` metadata names the leader when known. The request was not applied. |
| `UNAVAILABLE` | `BUSY` | A leadership transfer or membership change is in progress. The request was not applied. |
| `UNAVAILABLE` | none | The node is unreachable, shutting down, or has failed. |
| `INVALID_ARGUMENT` | none | The request is malformed or over a limit. |
| `DEADLINE_EXCEEDED` | none | A read did not finish before its deadline. |
| `UNKNOWN` | `OUTCOME_UNKNOWN` | A write may or may not have been applied. |
| `INTERNAL` | none | An unexpected server error; details stay in the server log. |

Clients may retry any request that failed with `NOT_LEADER` or `BUSY`, and
any read. A write that timed out or lost its connection after it was sent has
an unknown outcome; the Java client reports it as `OutcomeUnknown` rather than
retrying, because PartDB does not yet deduplicate requests.

## Future Semantics

The next product-level semantics should build on revisions:

- watches that resume from a revision
- compaction that bounds historical watch state
- leases and TTL-backed ephemeral keys
- richer transaction operations if needed by control-plane workloads
