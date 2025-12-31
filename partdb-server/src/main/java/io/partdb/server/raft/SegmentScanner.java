package io.partdb.server.raft;

import io.partdb.storage.StorageException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import static io.partdb.server.raft.LogCodec.BYTE_ORDER;

public final class SegmentScanner {

    private static final int RECORD_HEADER_SIZE = LogSegment.RECORD_HEADER_SIZE;

    private static final ValueLayout.OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfByte BYTE_LE = ValueLayout.JAVA_BYTE;

    private SegmentScanner() {}

    public static ScanResult scan(Path path, Consumer<ScannedRecord> consumer) {
        try (Arena arena = Arena.ofConfined();
             FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

            long fileSize = channel.size();
            if (fileSize == 0) {
                return new ScanResult.Complete(0);
            }

            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            return scanMapped(mapped, consumer);

        } catch (IOException e) {
            throw new StorageException.IO("Failed to scan WAL segment: " + path, e);
        }
    }

    private static ScanResult scanMapped(MemorySegment mapped, Consumer<ScannedRecord> consumer) {
        long fileSize = mapped.byteSize();
        long offset = 0;
        CRC32C crc = new CRC32C();

        while (offset < fileSize) {
            long remaining = fileSize - offset;

            if (remaining < RECORD_HEADER_SIZE) {
                return new ScanResult.Incomplete(offset, fileSize, IncompleteReason.TRUNCATED_HEADER, offset);
            }

            int payloadLength = mapped.get(INT_LE, offset);
            int storedCrc = mapped.get(INT_LE, offset + 4);
            byte recordType = mapped.get(BYTE_LE, offset + 8);

            long recordSize = RECORD_HEADER_SIZE + payloadLength;
            if (remaining < recordSize) {
                return new ScanResult.Incomplete(offset, fileSize, IncompleteReason.TRUNCATED_PAYLOAD, offset);
            }

            byte[] payload = mapped.asSlice(offset + RECORD_HEADER_SIZE, payloadLength)
                    .toArray(ValueLayout.JAVA_BYTE);

            crc.reset();
            crc.update(recordType);
            crc.update(payload);
            if ((int) crc.getValue() != storedCrc) {
                return new ScanResult.Incomplete(offset, fileSize, IncompleteReason.CRC_MISMATCH, offset);
            }

            LogRecord record = decodeRecord(recordType, payload);
            consumer.accept(new ScannedRecord(offset, record));

            offset += recordSize;
        }

        return new ScanResult.Complete(fileSize);
    }

    private static LogRecord decodeRecord(byte type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(BYTE_ORDER);
        return switch (type) {
            case LogRecord.TYPE_ENTRY -> new LogRecord.Entry(LogCodec.readEntry(buf));
            case LogRecord.TYPE_STATE -> new LogRecord.State(LogCodec.readHardState(buf));
            case LogRecord.TYPE_SNAPSHOT_MARKER -> {
                long index = buf.getLong();
                long term = buf.getLong();
                yield new LogRecord.SnapshotMarker(index, term);
            }
            default -> throw new StorageException.Corruption("Unknown record type: " + type);
        };
    }

    public static void repair(Path path, long truncateToOffset) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.truncate(truncateToOffset);
            channel.force(true);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to repair WAL segment: " + path, e);
        }
    }

    public sealed interface ScanResult {

        record Complete(long fileSize) implements ScanResult {}

        record Incomplete(
                long validEndOffset,
                long originalFileSize,
                IncompleteReason reason,
                long problemOffset
        ) implements ScanResult {}
    }

    public record ScannedRecord(long offset, LogRecord record) {}

    public enum IncompleteReason {
        TRUNCATED_HEADER,
        TRUNCATED_PAYLOAD,
        CRC_MISMATCH
    }
}
