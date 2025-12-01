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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

final class RaftLogSegment implements AutoCloseable {

    private static final int MAGIC_NUMBER = 0x5241464C;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 16;
    private static final int ENTRY_HEADER_SIZE = 16;

    private final Path path;
    private final long segmentId;
    private final FileChannel channel;
    private final ReentrantLock channelLock = new ReentrantLock();
    private final Map<Long, Long> indexToOffset;

    private long firstIndex;
    private long lastIndex;
    private Arena arena;
    private MemorySegment mappedSegment;

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
    }

    void append(LogEntry entry) {
        try {
            long entryOffset = channel.size();
            byte[] serialized = serializeEntry(entry);

            CRC32C crc = new CRC32C();
            crc.update(serialized);

            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_HEADER_SIZE + serialized.length);
            buffer.putInt((int) crc.getValue());
            buffer.putInt(serialized.length);
            buffer.putLong(entry.index());
            buffer.put(serialized);
            buffer.flip();

            channel.position(entryOffset);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
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

    void sync() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to sync segment", e);
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

    @Override
    public void close() {
        try {
            if (arena != null) {
                arena.close();
            }
            channel.close();
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to close segment", e);
        }
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
        channelLock.lock();
        try {
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
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to read entry at offset " + offset, e);
        } finally {
            channelLock.unlock();
        }
    }

    private byte[] serializeEntry(LogEntry entry) {
        Operation op = entry.command();

        return switch (op) {
            case Put put -> {
                int size = 8 + 1 + 4 + put.key().size() + 4 + put.value().size() + 8;
                ByteBuffer buf = ByteBuffer.allocate(size);
                buf.putLong(entry.term());
                buf.put((byte) 1);
                buf.putInt(put.key().size());
                buf.put(put.key().toByteArray());
                buf.putInt(put.value().size());
                buf.put(put.value().toByteArray());
                buf.putLong(put.leaseId());
                yield buf.array();
            }
            case Delete delete -> {
                int size = 8 + 1 + 4 + delete.key().size();
                ByteBuffer buf = ByteBuffer.allocate(size);
                buf.putLong(entry.term());
                buf.put((byte) 2);
                buf.putInt(delete.key().size());
                buf.put(delete.key().toByteArray());
                yield buf.array();
            }
            case GrantLease grantLease -> {
                ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8 + 8 + 8);
                buf.putLong(entry.term());
                buf.put((byte) 3);
                buf.putLong(grantLease.leaseId());
                buf.putLong(grantLease.ttlMillis());
                buf.putLong(grantLease.grantedAtMillis());
                yield buf.array();
            }
            case RevokeLease revokeLease -> {
                ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8);
                buf.putLong(entry.term());
                buf.put((byte) 4);
                buf.putLong(revokeLease.leaseId());
                yield buf.array();
            }
            case KeepAliveLease keepAliveLease -> {
                ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8);
                buf.putLong(entry.term());
                buf.put((byte) 5);
                buf.putLong(keepAliveLease.leaseId());
                yield buf.array();
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
