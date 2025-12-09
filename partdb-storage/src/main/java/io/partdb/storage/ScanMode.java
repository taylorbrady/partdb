package io.partdb.storage;

import io.partdb.common.Timestamp;

public sealed interface ScanMode {
    record Snapshot(Timestamp readTimestamp) implements ScanMode {}
    record AllVersions() implements ScanMode {}
}
