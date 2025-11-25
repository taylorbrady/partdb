package io.partdb.common;

@FunctionalInterface
public interface LeaseProvider {

    boolean isLeaseActive(long leaseId);

    static LeaseProvider alwaysActive() {
        return leaseId -> true;
    }

    static LeaseProvider neverActive() {
        return leaseId -> leaseId == 0;
    }
}
