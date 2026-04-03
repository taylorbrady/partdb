package io.partdb.node.lease;

public record LeaseId(long value) {
    public LeaseId {
        if (value <= 0) {
            throw new IllegalArgumentException("value must be positive");
        }
    }

    public static LeaseId of(long value) {
        return new LeaseId(value);
    }
}
