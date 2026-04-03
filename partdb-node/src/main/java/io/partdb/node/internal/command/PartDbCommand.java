package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface PartDbCommand
    permits PartDbCommand.Put,
            PartDbCommand.Delete,
            PartDbCommand.GrantLease,
            PartDbCommand.RevokeLease,
            PartDbCommand.KeepAliveLease,
            PartDbCommand.ExpireLease {

    record Put(Bytes key, Bytes value, long leaseId) implements PartDbCommand {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
            if (leaseId < 0) {
                throw new IllegalArgumentException("leaseId must not be negative");
            }
        }
    }

    record Delete(Bytes key) implements PartDbCommand {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }

    record GrantLease(long ttlNanos) implements PartDbCommand {
        public GrantLease {
            if (ttlNanos <= 0) {
                throw new IllegalArgumentException("ttlNanos must be positive");
            }
        }
    }

    record RevokeLease(long leaseId) implements PartDbCommand {
        public RevokeLease {
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
        }
    }

    record KeepAliveLease(long leaseId) implements PartDbCommand {
        public KeepAliveLease {
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
        }
    }

    record ExpireLease(long leaseId) implements PartDbCommand {
        public ExpireLease {
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
        }
    }
}
