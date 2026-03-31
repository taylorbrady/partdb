package io.partdb.node.kv;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.command.proto.CommandProto.GrantLease;
import io.partdb.node.command.proto.CommandProto.Put;
import io.partdb.node.command.proto.CommandProto.RevokeLease;
import io.partdb.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void restoreRebuildsLeaseKeyIndex() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        Bytes snapshot;
        Bytes key = Bytes.utf8("lease-key");
        Bytes value = Bytes.utf8("lease-value");

        try (KvStore source = KvStore.open(sourceDir, StorageConfig.defaults())) {
            source.apply(1, Bytes.copyOf(grantLease(7, 1_000_000_000).toByteArray()));
            source.apply(2, Bytes.copyOf(put(key.toByteArray(), value.toByteArray(), 7).toByteArray()));

            assertTrue(source.get(key).isPresent());
            snapshot = source.snapshot();
        }

        try (KvStore restored = KvStore.open(restoredDir, StorageConfig.defaults())) {
            restored.restore(2, snapshot);

            assertTrue(restored.get(key).isPresent());
            assertEquals(value, restored.get(key).orElseThrow());

            restored.apply(3, Bytes.copyOf(revokeLease(7).toByteArray()));

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
