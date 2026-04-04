package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftSnapshot;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.io.IOException;

public final class ConsensusBootstrap {
    private ConsensusBootstrap() {
    }

    public static void initializeFromSnapshot(
        Path dataDirectory,
        ConsensusConfig config,
        Bytes snapshotData,
        long snapshotIndex
    ) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(snapshotData, "snapshotData must not be null");
        if (snapshotIndex < 0) {
            throw new IllegalArgumentException("snapshotIndex must not be negative");
        }

        try {
            ensureDirectoryIsEmpty(dataDirectory);
            Files.createDirectories(dataDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to prepare consensus data directory", e);
        }

        if (snapshotIndex == 0) {
            return;
        }

        RaftMembership membership = ClusterMemberships.toRaftMembership(config.membership());
        try (RaftStore store = DurableRaftStore.create(dataDirectory, membership)) {
            store.saveSnapshot(new RaftSnapshot(
                snapshotIndex,
                0,
                membership,
                snapshotData
            ));
        }
    }

    private static void ensureDirectoryIsEmpty(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var entries = Files.list(directory)) {
            if (entries.findAny().isPresent()) {
                throw new IllegalArgumentException("Consensus data directory must be empty: " + directory);
            }
        }
    }
}
