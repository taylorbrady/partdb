package io.partdb.raft;

import io.partdb.common.ByteArray;
import io.partdb.common.statemachine.Delete;
import io.partdb.common.statemachine.GrantLease;
import io.partdb.common.statemachine.KeepAliveLease;
import io.partdb.common.statemachine.Operation;
import io.partdb.common.statemachine.Put;
import io.partdb.common.statemachine.RevokeLease;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32C;

final class RaftLogSegment implements AutoCloseable {
    private static final int MAGIC_NUMBER = 0x5241464C;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 12;
    private static final int ENTRY_HEADER_SIZE = 16;

    private final Path path;
    private final long segmentId;
    private final FileChannel channel;
    private final Map<Long, Long> indexToOffset;
    private long firstIndex;
    private long lastIndex;

    private final Arena writeArena;
    private Arena arena;
    private MemorySegment mappedSegment;

    private RaftLogSegment(
        Path path,
        long segmentId,
        FileChannel channel,
        Map<Long, Long> indexToOffset,
        long firstIndex,
        long lastIndex
    ) {
        this.path = path;
        this.segmentId = segmentId;
        this.channel = channel;
        this.indexToOffset = indexToOffset;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
        this.writeArena = Arena.ofConfined();
        this.arena = null;
        this.mappedSegment = null;
    }

    static RaftLogSegment create(Path path, long segmentId, long firstIndex) {
        try {
            FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.putInt(MAGIC_NUMBER);
            header.putInt(VERSION);
            header.putLong(firstIndex);
            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }

            channel.force(true);

            return new RaftLogSegment(path, segmentId, channel, new HashMap<>(), firstIndex, firstIndex - 1);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to create log segment: " + path, e);
        }
    }

    static RaftLogSegment open(Path path, long segmentId) {
        try {
            FileChannel channel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

            long fileSize = channel.size();
            Arena arena = Arena.ofShared();
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);

            int magic = mapped.get(ValueLayout.JAVA_INT, 0);
            if (magic != MAGIC_NUMBER) {
                arena.close();
                throw new RaftException.LogException("Invalid magic number in segment: " + path);
            }

            int version = mapped.get(ValueLayout.JAVA_INT, 4);
            if (version != VERSION) {
                arena.close();
                throw new RaftException.LogException("Unsupported version in segment: " + path);
            }

            long firstIndex = mapped.get(ValueLayout.JAVA_LONG, 8);
            Map<Long, Long> indexToOffset = new HashMap<>();
            long lastIndex = firstIndex - 1;

            long position = HEADER_SIZE;
            while (position < fileSize) {
                try {
                    long entryOffset = position;

                    if (position + ENTRY_HEADER_SIZE > fileSize) {
                        break;
                    }

                    int crc = mapped.get(ValueLayout.JAVA_INT, position);
                    int length = mapped.get(ValueLayout.JAVA_INT, position + 4);
                    long index = mapped.get(ValueLayout.JAVA_LONG, position + 8);

                    if (position + ENTRY_HEADER_SIZE + length > fileSize) {
                        break;
                    }

                    byte[] entryData = new byte[length];
                    MemorySegment.copy(mapped, ValueLayout.JAVA_BYTE, position + ENTRY_HEADER_SIZE,
                                     entryData, 0, length);

                    CRC32C crc32c = new CRC32C();
                    crc32c.update(entryData);
                    if ((int) crc32c.getValue() != crc) {
                        throw new RaftException.LogException("CRC mismatch in segment: " + path);
                    }

                    indexToOffset.put(index, entryOffset);
                    lastIndex = index;
                    position += ENTRY_HEADER_SIZE + length;
                } catch (Exception e) {
                    break;
                }
            }

            var segment = new RaftLogSegment(path, segmentId, channel, indexToOffset, firstIndex, lastIndex);
            segment.arena = arena;
            segment.mappedSegment = mapped;

            return segment;
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to open log segment: " + path, e);
        }
    }

    void append(LogEntry entry) {
        try {
            long entryOffset = channel.size();
            MemorySegment serialized = serializeEntryOffHeap(entry, writeArena);
            long serializedSize = serialized.byteSize();

            CRC32C crc = new CRC32C();
            for (long i = 0; i < serializedSize; i++) {
                crc.update(serialized.get(ValueLayout.JAVA_BYTE, i));
            }

            MemorySegment buffer = writeArena.allocate(ENTRY_HEADER_SIZE + serializedSize);
            buffer.set(ValueLayout.JAVA_INT, 0, (int) crc.getValue());
            buffer.set(ValueLayout.JAVA_INT, 4, (int) serializedSize);
            buffer.set(ValueLayout.JAVA_LONG, 8, entry.index());
            MemorySegment.copy(serialized, 0, buffer, ENTRY_HEADER_SIZE, serializedSize);

            channel.position(entryOffset);
            channel.write(buffer.asByteBuffer());
            channel.force(true);

            indexToOffset.put(entry.index(), entryOffset);
            lastIndex = entry.index();
            if (firstIndex < 0) {
                firstIndex = entry.index();
            }
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to append entry to segment", e);
        }
    }

    Optional<LogEntry> get(long index) {
        Long offset = indexToOffset.get(index);
        if (offset == null) {
            return Optional.empty();
        }
        return Optional.of(readEntryAtOffset(offset));
    }

    List<LogEntry> getRange(long startIndex, long endIndex) {
        return indexToOffset.keySet().stream()
            .filter(idx -> idx >= startIndex && idx < endIndex)
            .sorted()
            .map(this::get)
            .flatMap(Optional::stream)
            .toList();
    }

    List<LogEntry> readAll() {
        return indexToOffset.keySet().stream()
            .sorted()
            .map(this::get)
            .flatMap(Optional::stream)
            .toList();
    }

    private LogEntry readEntryAtOffset(long offset) {
        if (mappedSegment != null) {
            return readEntryFromMapped(offset);
        }
        return readEntryFromChannel(offset);
    }

    private LogEntry readEntryFromMapped(long offset) {
        int crc = mappedSegment.get(ValueLayout.JAVA_INT, offset);
        int length = mappedSegment.get(ValueLayout.JAVA_INT, offset + 4);
        long index = mappedSegment.get(ValueLayout.JAVA_LONG, offset + 8);

        byte[] entryData = new byte[length];
        MemorySegment.copy(mappedSegment, ValueLayout.JAVA_BYTE, offset + ENTRY_HEADER_SIZE,
                         entryData, 0, length);

        CRC32C crc32c = new CRC32C();
        crc32c.update(entryData);
        if ((int) crc32c.getValue() != crc) {
            throw new RaftException.LogException("CRC mismatch at offset " + offset);
        }

        return deserializeEntry(index, entryData);
    }

    private LogEntry readEntryFromChannel(long offset) {
        try {
            synchronized (channel) {
                channel.position(offset);

                ByteBuffer entryHeader = ByteBuffer.allocate(ENTRY_HEADER_SIZE);
                if (channel.read(entryHeader) < ENTRY_HEADER_SIZE) {
                    throw new RaftException.LogException("Incomplete entry header at offset " + offset);
                }
                entryHeader.flip();

                int crc = entryHeader.getInt();
                int length = entryHeader.getInt();
                long index = entryHeader.getLong();

                ByteBuffer entryData = ByteBuffer.allocate(length);
                int bytesRead = 0;
                while (bytesRead < length) {
                    int read = channel.read(entryData);
                    if (read == -1) {
                        throw new RaftException.LogException("Incomplete entry data at offset " + offset);
                    }
                    bytesRead += read;
                }
                entryData.flip();

                CRC32C crc32c = new CRC32C();
                crc32c.update(entryData.array());
                if ((int) crc32c.getValue() != crc) {
                    throw new RaftException.LogException("CRC mismatch at offset " + offset);
                }

                return deserializeEntry(index, entryData.array());
            }
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to read entry at offset " + offset, e);
        }
    }

    long firstIndex() {
        return firstIndex;
    }

    long lastIndex() {
        return lastIndex;
    }

    long segmentId() {
        return segmentId;
    }

    Path path() {
        return path;
    }

    long size() {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return 0;
        }
    }

    void sync() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to sync segment", e);
        }
    }

    void seal() {
        if (mappedSegment != null) {
            return;
        }

        try {
            long fileSize = channel.size();
            arena = Arena.ofShared();
            mappedSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to seal segment: " + path, e);
        }
    }

    @Override
    public void close() {
        try {
            writeArena.close();
            if (arena != null) {
                arena.close();
            }
            channel.close();
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to close segment", e);
        }
    }

    private MemorySegment serializeEntryOffHeap(LogEntry entry, Arena arena) {
        Operation op = entry.command();

        return switch (op) {
            case Put put -> {
                long size = 8 + 1 + 4 + put.key().size() + 4 + put.value().size() + 8;
                MemorySegment seg = arena.allocate(size);
                long offset = 0;

                seg.set(ValueLayout.JAVA_LONG, offset, entry.term());
                offset += 8;
                seg.set(ValueLayout.JAVA_BYTE, offset, (byte) 1);
                offset += 1;
                seg.set(ValueLayout.JAVA_INT, offset, put.key().size());
                offset += 4;
                MemorySegment.copy(put.key().toByteArray(), 0, seg, ValueLayout.JAVA_BYTE, offset, put.key().size());
                offset += put.key().size();
                seg.set(ValueLayout.JAVA_INT, offset, put.value().size());
                offset += 4;
                MemorySegment.copy(put.value().toByteArray(), 0, seg, ValueLayout.JAVA_BYTE, offset, put.value().size());
                offset += put.value().size();
                seg.set(ValueLayout.JAVA_LONG, offset, put.leaseId());

                yield seg;
            }
            case Delete delete -> {
                long size = 8 + 1 + 4 + delete.key().size();
                MemorySegment seg = arena.allocate(size);
                long offset = 0;

                seg.set(ValueLayout.JAVA_LONG, offset, entry.term());
                offset += 8;
                seg.set(ValueLayout.JAVA_BYTE, offset, (byte) 2);
                offset += 1;
                seg.set(ValueLayout.JAVA_INT, offset, delete.key().size());
                offset += 4;
                MemorySegment.copy(delete.key().toByteArray(), 0, seg, ValueLayout.JAVA_BYTE, offset, delete.key().size());

                yield seg;
            }
            case GrantLease grantLease -> {
                long size = 8 + 1 + 8 + 8 + 8;
                MemorySegment seg = arena.allocate(size);
                long offset = 0;

                seg.set(ValueLayout.JAVA_LONG, offset, entry.term());
                offset += 8;
                seg.set(ValueLayout.JAVA_BYTE, offset, (byte) 3);
                offset += 1;
                seg.set(ValueLayout.JAVA_LONG, offset, grantLease.leaseId());
                offset += 8;
                seg.set(ValueLayout.JAVA_LONG, offset, grantLease.ttlMillis());
                offset += 8;
                seg.set(ValueLayout.JAVA_LONG, offset, grantLease.grantedAtMillis());

                yield seg;
            }
            case RevokeLease revokeLease -> {
                long size = 8 + 1 + 8;
                MemorySegment seg = arena.allocate(size);
                long offset = 0;

                seg.set(ValueLayout.JAVA_LONG, offset, entry.term());
                offset += 8;
                seg.set(ValueLayout.JAVA_BYTE, offset, (byte) 4);
                offset += 1;
                seg.set(ValueLayout.JAVA_LONG, offset, revokeLease.leaseId());

                yield seg;
            }
            case KeepAliveLease keepAliveLease -> {
                long size = 8 + 1 + 8;
                MemorySegment seg = arena.allocate(size);
                long offset = 0;

                seg.set(ValueLayout.JAVA_LONG, offset, entry.term());
                offset += 8;
                seg.set(ValueLayout.JAVA_BYTE, offset, (byte) 5);
                offset += 1;
                seg.set(ValueLayout.JAVA_LONG, offset, keepAliveLease.leaseId());

                yield seg;
            }
        };
    }

    private LogEntry deserializeEntry(long index, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        long term = buffer.getLong();
        byte opType = buffer.get();

        Operation operation = switch (opType) {
            case 1 -> {
                int keySize = buffer.getInt();
                byte[] keyBytes = new byte[keySize];
                buffer.get(keyBytes);
                ByteArray key = ByteArray.wrap(keyBytes);

                int valueSize = buffer.getInt();
                byte[] valueBytes = new byte[valueSize];
                buffer.get(valueBytes);
                ByteArray value = ByteArray.wrap(valueBytes);

                long leaseId = buffer.getLong();

                yield new Put(key, value, leaseId);
            }
            case 2 -> {
                int keySize = buffer.getInt();
                byte[] keyBytes = new byte[keySize];
                buffer.get(keyBytes);
                ByteArray key = ByteArray.wrap(keyBytes);

                yield new Delete(key);
            }
            case 3 -> {
                long leaseId = buffer.getLong();
                long ttlMillis = buffer.getLong();
                long grantedAtMillis = buffer.getLong();

                yield new GrantLease(leaseId, ttlMillis, grantedAtMillis);
            }
            case 4 -> {
                long leaseId = buffer.getLong();

                yield new RevokeLease(leaseId);
            }
            case 5 -> {
                long leaseId = buffer.getLong();

                yield new KeepAliveLease(leaseId);
            }
            default -> throw new RaftException.LogException("Unknown operation type: " + opType);
        };

        return new LogEntry(index, term, operation);
    }
}
