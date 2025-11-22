package io.partdb.common.statemachine;

import io.partdb.common.exception.CorruptionException;

import java.util.Objects;
import java.util.zip.CRC32C;

public record StateSnapshot(
    long lastAppliedIndex,
    byte[] data,
    long checksum
) {
    public StateSnapshot {
        if (lastAppliedIndex < 0) {
            throw new IllegalArgumentException("lastAppliedIndex must be non-negative");
        }
        Objects.requireNonNull(data, "data must not be null");
        data = data.clone();
    }

    public static StateSnapshot create(long lastAppliedIndex, byte[] data) {
        var crc = new CRC32C();
        crc.update(data);
        return new StateSnapshot(lastAppliedIndex, data, crc.getValue());
    }

    public static StateSnapshot restore(long lastAppliedIndex, byte[] data, long checksum) {
        return new StateSnapshot(lastAppliedIndex, data, checksum);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    public void verify() {
        var crc = new CRC32C();
        crc.update(data);
        if (crc.getValue() != checksum) {
            throw new CorruptionException(
                "Snapshot checksum mismatch: expected=%d, actual=%d"
                    .formatted(checksum, crc.getValue())
            );
        }
    }
}
