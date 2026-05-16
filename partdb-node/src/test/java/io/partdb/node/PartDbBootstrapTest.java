package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.node.admin.BackupRestorer;
import io.partdb.node.admin.PartDbBackup;
import io.partdb.node.admin.RestoreResult;
import io.partdb.node.cluster.NodeRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbBootstrapTest {

    @TempDir
    Path tempDir;

    @Test
    void recoverFromBackupPreservesDurableKeys() throws Exception {
        var sourceConfig = PartDbNodeConfig.builder("node-1", tempDir.resolve("source"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        PartDbBackup backup;
        try (var node = PartDbNode.open(sourceConfig)) {
            awaitLeader(node);

            node.keyValues().put(bytes("plain-key"), bytes("plain-value"))
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

            backup = node.admin().createBackup().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }

        var recoveredConfig = PartDbNodeConfig.builder("node-1", tempDir.resolve("recovered"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        RestoreResult result = BackupRestorer.restore(recoveredConfig, backup);

        assertTrue(result.revision() >= backup.appliedIndex());

        try (var recovered = PartDbNode.open(recoveredConfig)) {
            awaitLeader(recovered);

            assertEquals(
                bytes("plain-value"),
                recovered.keyValues().get(bytes("plain-key")).toCompletableFuture().get(5, TimeUnit.SECONDS)
                    .orElseThrow()
                    .value()
            );
            long nextWriteRevision = recovered.keyValues()
                .put(bytes("post-recovery"), bytes("value"))
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS)
                .revision();
            assertTrue(nextWriteRevision > backup.appliedIndex());
        }
    }

    private static void awaitLeader(PartDbNode node) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            if (node.cluster().status().role() == NodeRole.LEADER) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("Node did not become leader");
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }
}
