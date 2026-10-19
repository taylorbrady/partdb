package io.partdb.raft.runtime;

import io.partdb.raft.Ids;
import io.partdb.raft.SnapshotMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotsTest {

    @TempDir
    Path directory;

    @Test
    void recoveryFinishesAnInstallInterruptedAfterTheLogWasReset() throws IOException {
        var base = new SnapshotMetadata(5, 2, Ids.voters("n1"));
        try (var storage = LogStore.open(directory)) {
            var files = SnapshotFiles.open(directory.resolve("snap"));
            Path staged = files.stagingFile("incoming");
            Files.writeString(staged, "a\nb");
            files.promote(staged, base.index());
            storage.reset(base);
        }

        var machine = new RaftRuntimeTest.RecordingStateMachine();
        try (var storage = LogStore.open(directory)) {
            var files = SnapshotFiles.open(directory.resolve("snap"));
            Snapshots.recover(machine, storage, files);
        }

        assertEquals(5, machine.appliedIndex());
        assertEquals(List.of("a", "b"), machine.commands);
        try (var files = Files.list(directory.resolve("snap"))) {
            assertEquals(List.of(), files.toList());
        }
    }

    @Test
    void recoveryDropsAnInstallFileTheStateMachineAlreadyApplied() throws IOException {
        var base = new SnapshotMetadata(5, 2, Ids.voters("n1"));
        var machine = new RaftRuntimeTest.RecordingStateMachine();
        try (var storage = LogStore.open(directory)) {
            var files = SnapshotFiles.open(directory.resolve("snap"));
            Path staged = files.stagingFile("incoming");
            Files.writeString(staged, "a");
            Path installing = files.promote(staged, base.index());
            storage.reset(base);
            try (var in = Files.newInputStream(installing)) {
                machine.restore(base.index(), in);
            }
        }

        try (var storage = LogStore.open(directory)) {
            var files = SnapshotFiles.open(directory.resolve("snap"));
            Snapshots.recover(machine, storage, files);
        }

        try (var files = Files.list(directory.resolve("snap"))) {
            assertTrue(files.toList().isEmpty());
        }
    }

    @Test
    void recoveryRefusesAStateMachineBehindTheLogWithoutAnInstall() {
        try (var storage = LogStore.open(directory)) {
            storage.reset(new SnapshotMetadata(5, 2, Ids.voters("n1")));
        }

        try (var storage = LogStore.open(directory)) {
            var files = SnapshotFiles.open(directory.resolve("snap"));
            assertThrows(RaftStorageException.Corruption.class,
                () -> Snapshots.recover(new RaftRuntimeTest.RecordingStateMachine(), storage, files));
        }
    }

    @Test
    void openDiscardsStagedTransfers() throws IOException {
        var files = SnapshotFiles.open(directory);
        Path incoming = files.stagingFile("incoming");
        Path outgoing = files.stagingFile("outgoing");

        SnapshotFiles.open(directory);

        assertTrue(Files.notExists(incoming));
        assertTrue(Files.notExists(outgoing));
    }
}
