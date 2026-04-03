package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.PartDbException;
import io.partdb.node.kv.DeleteResult;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteBatchResult;
import io.partdb.node.lease.LeaseGrant;
import io.partdb.node.lease.LeaseId;
import io.partdb.node.lease.LeaseKeepAliveResult;
import io.partdb.node.lease.LeaseRevokeResult;

import java.time.Duration;
import java.util.Objects;

public final class PartDbCommands {
    private PartDbCommands() {
    }

    public static ReplicatedCommand<PutResult> put(Bytes key, Bytes value) {
        return new Put(key, value, 0L);
    }

    public static ReplicatedCommand<PutResult> put(Bytes key, Bytes value, LeaseId leaseId) {
        Objects.requireNonNull(leaseId, "leaseId must not be null");
        return new Put(key, value, leaseId.value());
    }

    public static ReplicatedCommand<DeleteResult> delete(Bytes key) {
        return new Delete(key);
    }

    public static ReplicatedCommand<WriteBatchResult> writeBatch(WriteBatch batch) {
        return new BatchWrite(batch);
    }

    public static ReplicatedCommand<LeaseGrant> grantLease(Duration ttl) {
        return new GrantLease(ttl);
    }

    public static ReplicatedCommand<LeaseKeepAliveResult> keepAliveLease(LeaseId leaseId) {
        return new KeepAliveLease(leaseId);
    }

    public static ReplicatedCommand<LeaseRevokeResult> revokeLease(LeaseId leaseId) {
        return new RevokeLease(leaseId);
    }

    public static ReplicatedCommand<LeaseRevokeResult> expireLease(LeaseId leaseId) {
        return new ExpireLease(leaseId);
    }

    private record Put(Bytes key, Bytes value, long leaseId) implements ReplicatedCommand<PutResult> {
        private Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.Put(key, value, leaseId);
        }

        @Override
        public PutResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.PutApplied(long modRevision) -> new PutResult(modRevision);
                case PartDbCommandResult.LeaseNotFound(long missingLeaseId) ->
                    throw new PartDbException.LeaseNotFound(missingLeaseId);
                default -> throw unexpected("put", result);
            };
        }
    }

    private record Delete(Bytes key) implements ReplicatedCommand<DeleteResult> {
        private Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.Delete(key);
        }

        @Override
        public DeleteResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.DeleteApplied(long modRevision) -> new DeleteResult(modRevision);
                default -> throw unexpected("delete", result);
            };
        }
    }

    private record BatchWrite(WriteBatch batch) implements ReplicatedCommand<WriteBatchResult> {
        private BatchWrite {
            batch = Objects.requireNonNull(batch, "batch must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.BatchWrite(batch);
        }

        @Override
        public WriteBatchResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.BatchWriteApplied(long modRevision) -> new WriteBatchResult(modRevision);
                case PartDbCommandResult.LeaseNotFound(long missingLeaseId) ->
                    throw new PartDbException.LeaseNotFound(missingLeaseId);
                default -> throw unexpected("writeBatch", result);
            };
        }
    }

    private record GrantLease(Duration ttl) implements ReplicatedCommand<LeaseGrant> {
        private GrantLease {
            ttl = Objects.requireNonNull(ttl, "ttl must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.GrantLease(ttl.toNanos());
        }

        @Override
        public LeaseGrant mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.LeaseGranted(long modRevision, long leaseId, long ttlNanos) ->
                    new LeaseGrant(LeaseId.of(leaseId), Duration.ofNanos(ttlNanos), modRevision);
                default -> throw unexpected("grantLease", result);
            };
        }
    }

    private record KeepAliveLease(LeaseId leaseId) implements ReplicatedCommand<LeaseKeepAliveResult> {
        private KeepAliveLease {
            leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.KeepAliveLease(leaseId.value());
        }

        @Override
        public LeaseKeepAliveResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.LeaseKeptAlive(long modRevision, long keptAliveLeaseId, long ttlNanos) ->
                    new LeaseKeepAliveResult(LeaseId.of(keptAliveLeaseId), Duration.ofNanos(ttlNanos), modRevision);
                case PartDbCommandResult.LeaseNotFound(long missingLeaseId) ->
                    throw new PartDbException.LeaseNotFound(missingLeaseId);
                default -> throw unexpected("keepAliveLease", result);
            };
        }
    }

    private record RevokeLease(LeaseId leaseId) implements ReplicatedCommand<LeaseRevokeResult> {
        private RevokeLease {
            leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.RevokeLease(leaseId.value());
        }

        @Override
        public LeaseRevokeResult mapResult(PartDbCommandResult result) {
            return mapLeaseRevocation("revokeLease", result);
        }
    }

    private record ExpireLease(LeaseId leaseId) implements ReplicatedCommand<LeaseRevokeResult> {
        private ExpireLease {
            leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.ExpireLease(leaseId.value());
        }

        @Override
        public LeaseRevokeResult mapResult(PartDbCommandResult result) {
            return mapLeaseRevocation("expireLease", result);
        }
    }

    private static LeaseRevokeResult mapLeaseRevocation(String operation, PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.LeaseRevoked(long modRevision, long revokedLeaseId, long deletedKeyCount) ->
                new LeaseRevokeResult(LeaseId.of(revokedLeaseId), modRevision, deletedKeyCount);
            case PartDbCommandResult.LeaseNotFound(long missingLeaseId) ->
                throw new PartDbException.LeaseNotFound(missingLeaseId);
            default -> throw unexpected(operation, result);
        };
    }

    private static IllegalStateException unexpected(String operation, PartDbCommandResult result) {
        return new IllegalStateException("Unexpected command result for " + operation + ": " + result);
    }
}
