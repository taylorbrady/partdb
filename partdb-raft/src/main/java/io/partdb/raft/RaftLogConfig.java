package io.partdb.raft;

import java.nio.file.Path;
import java.util.Objects;

public record RaftLogConfig(
    Path logDirectory,
    long segmentSize
) {
    public static final long DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024;

    public RaftLogConfig {
        Objects.requireNonNull(logDirectory, "logDirectory must not be null");
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("segmentSize must be positive");
        }
    }

    public static RaftLogConfig create(Path logDirectory) {
        return new RaftLogConfig(logDirectory, DEFAULT_SEGMENT_SIZE);
    }
}
