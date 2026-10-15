package io.partdb.raft.runtime;

import io.partdb.raft.RaftLogEntry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.partdb.raft.runtime.LogRecordCodec.BYTE_ORDER;

final class SealedSegment implements LogSegment {

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
            throw new RaftStorageException.IO("Failed to open WAL segment: " + path, e);
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

    public RaftLogEntry readEntry(long offset) {
        RecordHeader header = readHeader(offset);
        if (header.type() != LogRecord.TYPE_ENTRY) {
            throw new IllegalArgumentException("Record at offset " + offset + " is not an entry");
        }
        byte[] payload = readPayload(offset + RECORD_HEADER_SIZE, header.length());
        ByteBuffer buf = ByteBuffer.wrap(payload).order(BYTE_ORDER);
        return LogRecordCodec.readEntry(buf);
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

    public record RecordHeader(int length, int crc, byte type) {}
}
