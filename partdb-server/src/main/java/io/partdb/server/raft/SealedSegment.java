package io.partdb.server.raft;

import io.partdb.raft.LogEntry;
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

public final class SealedSegment implements LogSegment {

    private static final ValueLayout.OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfByte BYTE_LE = ValueLayout.JAVA_BYTE;

    private final Path path;
    private final int sequence;
    private final long firstIndex;
    private final long lastIndex;
    private final Arena arena;
    private final MemorySegment mapped;

    private SealedSegment(Path path, int sequence, long firstIndex, long lastIndex,
                          Arena arena, MemorySegment mapped) {
        this.path = path;
        this.sequence = sequence;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
        this.arena = arena;
        this.mapped = mapped;
    }

    public static SealedSegment open(Path path, int sequence, long firstIndex, long lastIndex) {
        Arena arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            return new SealedSegment(path, sequence, firstIndex, lastIndex, arena, mapped);
        } catch (IOException e) {
            arena.close();
            throw new StorageException.IO("Failed to open WAL segment: " + path, e);
        }
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public int sequence() {
        return sequence;
    }

    @Override
    public long firstIndex() {
        return firstIndex;
    }

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public long fileSize() {
        return mapped.byteSize();
    }

    @Override
    public void close() {
        arena.close();
    }

    public LogEntry readEntry(long offset) {
        RecordHeader header = readHeader(offset);
        if (header.type() != LogRecord.TYPE_ENTRY) {
            throw new IllegalArgumentException("Record at offset " + offset + " is not an entry");
        }
        byte[] payload = readPayload(offset + RECORD_HEADER_SIZE, header.length());
        ByteBuffer buf = ByteBuffer.wrap(payload).order(BYTE_ORDER);
        return LogCodec.readEntry(buf);
    }

    public void forEachRecord(Consumer<RecordWithOffset> consumer) {
        long offset = 0;
        while (offset < mapped.byteSize()) {
            RecordHeader header = readHeader(offset);
            byte[] payload = readPayload(offset + RECORD_HEADER_SIZE, header.length());

            if (!verifyCrc(header, payload)) {
                throw new StorageException.Corruption("CRC mismatch at WAL offset " + offset);
            }

            LogRecord record = decodeRecord(header.type(), payload);
            consumer.accept(new RecordWithOffset(offset, record));

            offset += RECORD_HEADER_SIZE + header.length();
        }
    }

    private RecordHeader readHeader(long offset) {
        int length = mapped.get(INT_LE, offset);
        int crc = mapped.get(INT_LE, offset + 4);
        byte type = mapped.get(BYTE_LE, offset + 8);
        return new RecordHeader(length, crc, type);
    }

    private byte[] readPayload(long offset, int length) {
        return mapped.asSlice(offset, length).toArray(ValueLayout.JAVA_BYTE);
    }

    private boolean verifyCrc(RecordHeader header, byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(header.type());
        crc.update(payload);
        return (int) crc.getValue() == header.crc();
    }

    private LogRecord decodeRecord(byte type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(BYTE_ORDER);
        return switch (type) {
            case LogRecord.TYPE_ENTRY -> new LogRecord.Entry(LogCodec.readEntry(buf));
            case LogRecord.TYPE_STATE -> new LogRecord.State(LogCodec.readHardState(buf));
            case LogRecord.TYPE_SNAPSHOT_MARKER -> {
                long index = buf.getLong();
                long term = buf.getLong();
                yield new LogRecord.SnapshotMarker(index, term);
            }
            default -> throw new IllegalArgumentException("Unknown record type: " + type);
        };
    }

    public record RecordHeader(int length, int crc, byte type) {}

    public record RecordWithOffset(long offset, LogRecord record) {}
}
