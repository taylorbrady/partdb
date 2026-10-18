package io.partdb.node;

import io.partdb.raft.runtime.RaftConfig;

import java.nio.file.Path;
import java.util.Objects;

public record NodeConfig(Path dataDirectory, RaftConfig raft, KvLimits limits) {
    public NodeConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(raft, "raft must not be null");
        Objects.requireNonNull(limits, "limits must not be null");
    }
}
