package io.partdb.node.recovery;

public record RecoveryOptions(boolean invalidateLeases) {
    public static RecoveryOptions defaults() {
        return new RecoveryOptions(true);
    }
}
