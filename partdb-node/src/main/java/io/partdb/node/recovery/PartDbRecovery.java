package io.partdb.node.recovery;

import io.partdb.consensus.ConsensusBootstrap;
import io.partdb.node.PartDbNodeConfig;
import io.partdb.node.state.PartDbStateMachine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

public final class PartDbRecovery {
    private PartDbRecovery() {
    }

    public static RecoveryResult restore(
        PartDbNodeConfig config,
        LogicalBackup backup
    ) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(backup, "backup must not be null");

        Path dataDirectory = config.dataDirectory();
        boolean newDirectory = !Files.exists(dataDirectory);
        try {
            ensureEmptyDataDirectory(dataDirectory);
            Files.createDirectories(dataDirectory);

            LogicalBackup recoveredBackup;
            RecoveryResult result;
            try (PartDbStateMachine stateMachine = PartDbStateMachine.open(
                dataDirectory.resolve("db"),
                config.storage().toStorageOptions()
            )) {
                stateMachine.restore(backup.appliedIndex(), backup.snapshotBytes());
                result = new RecoveryResult(stateMachine.lastAppliedIndex());
                recoveredBackup = new LogicalBackup(stateMachine.snapshot(), stateMachine.lastAppliedIndex());
            }

            ConsensusBootstrap.initializeFromSnapshot(
                dataDirectory.resolve("consensus"),
                config.replication().toConsensusConfig(config.nodeId(), config.membership()),
                recoveredBackup.snapshotBytes(),
                recoveredBackup.appliedIndex()
            );

            return result;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to recover cluster backup", e);
        } catch (RuntimeException e) {
            if (newDirectory) {
                try {
                    deleteRecursively(dataDirectory);
                } catch (IOException cleanupError) {
                    e.addSuppressed(cleanupError);
                }
            }
            throw e;
        }
    }

    private static void ensureEmptyDataDirectory(Path dataDirectory) throws IOException {
        if (!Files.exists(dataDirectory)) {
            return;
        }
        try (Stream<Path> entries = Files.list(dataDirectory)) {
            if (entries.findAny().isPresent()) {
                throw new IllegalArgumentException("dataDirectory must be empty for disaster recovery: " + dataDirectory);
            }
        }
    }

    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }
}
