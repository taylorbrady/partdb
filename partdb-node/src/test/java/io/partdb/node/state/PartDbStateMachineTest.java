package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.node.command.PartDbCommand;
import io.partdb.node.command.PartDbCommandCodec;
import io.partdb.storage.StorageOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbStateMachineTest {

    @TempDir
    Path tempDir;

    @Test
    void restoreRebuildsLeaseKeyIndex() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        Bytes snapshot;
        Bytes key = Bytes.utf8("lease-key");
        Bytes value = Bytes.utf8("lease-value");

        try (PartDbStateMachine source = PartDbStateMachine.open(sourceDir, StorageOptions.defaults())) {
            source.apply(1, PartDbCommandCodec.encode(new PartDbCommand.GrantLease(1_000_000_000)));
            source.apply(2, PartDbCommandCodec.encode(new PartDbCommand.Put(key, value, 1)));

            assertTrue(source.getLocal(key).isPresent());
            snapshot = source.snapshot();
        }

        try (PartDbStateMachine restored = PartDbStateMachine.open(restoredDir, StorageOptions.defaults())) {
            restored.restore(2, snapshot);

            assertTrue(restored.getLocal(key).isPresent());
            assertEquals(value, restored.getLocal(key).orElseThrow());

            restored.apply(3, PartDbCommandCodec.encode(new PartDbCommand.RevokeLease(1)));

            assertFalse(restored.getLocal(key).isPresent());
        }
    }
}
