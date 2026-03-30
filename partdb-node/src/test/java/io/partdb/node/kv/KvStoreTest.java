package io.partdb.node.kv;

import com.google.protobuf.ByteString;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.command.proto.CommandProto.GrantLease;
import io.partdb.node.command.proto.CommandProto.Put;
import io.partdb.node.command.proto.CommandProto.RevokeLease;
import io.partdb.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void restoreRebuildsLeaseKeyIndex() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        byte[] snapshot;
        byte[] key = "lease-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "lease-value".getBytes(StandardCharsets.UTF_8);

        try (KvStore source = KvStore.open(sourceDir, StorageConfig.defaults())) {
            source.apply(1, grantLease(7, 1_000_000_000).toByteArray());
            source.apply(2, put(key, value, 7).toByteArray());

            assertTrue(source.get(key).isPresent());
            snapshot = source.snapshot();
        }

        try (KvStore restored = KvStore.open(restoredDir, StorageConfig.defaults())) {
            restored.restore(2, snapshot);

            assertTrue(restored.get(key).isPresent());
            assertArrayEquals(value, restored.get(key).orElseThrow());

            restored.apply(3, revokeLease(7).toByteArray());

            assertFalse(restored.get(key).isPresent());
        }
    }

    private static Command grantLease(long leaseId, long ttlNanos) {
        return Command.newBuilder()
            .setGrantLease(GrantLease.newBuilder()
                .setLeaseId(leaseId)
                .setTtlNanos(ttlNanos))
            .build();
    }

    private static Command put(byte[] key, byte[] value, long leaseId) {
        return Command.newBuilder()
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .setLeaseId(leaseId))
            .build();
    }

    private static Command revokeLease(long leaseId) {
        return Command.newBuilder()
            .setRevokeLease(RevokeLease.newBuilder()
                .setLeaseId(leaseId))
            .build();
    }
}
