package io.partdb.raft;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.zip.CRC32C;

final class SealedSegment implements LogSegment {

    static final int MAGIC_NUMBER = 0x5241464C;
    static final int VERSION = 1;
    static final int HEADER_SIZE = 16;
    static final int ENTRY_HEADER_SIZE = 16;

    private final Path path;
    private final long segmentId;
    private final FileChannel channel;
    private final Arena arena;
    private final MemorySegment mapped;
    private final Map<Long, Long> indexToOffset;
    private final long firstIndex;
    private final long lastIndex;

    static SealedSegment open(Path path, long segmentId) {
        FileChannel channel = null;
        Arena arena = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ);
            long fileSize = channel.size();
            arena = Arena.ofShared();
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);

            int magic = mapped.get(ValueLayout.JAVA_INT, 0);
            if (magic != MAGIC_NUMBER) {
                throw new RaftException.LogException("Invalid magic number in segment: " + path);
            }

            int version = mapped.get(ValueLayout.JAVA_INT, 4);
            if (version != VERSION) {
                throw new RaftException.LogException("Unsupported version in segment: " + path);
            }

            long firstIndex = mapped.get(ValueLayout.JAVA_LONG, 8);
            Map<Long, Long> indexToOffset = new HashMap<>();
            long lastIndex = firstIndex - 1;

            long position = HEADER_SIZE;
            while (position < fileSize) {
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

                indexToOffset.put(index, position);
                lastIndex = index;
                position += ENTRY_HEADER_SIZE + length;
            }

            var segment = new SealedSegment(path, segmentId, channel, arena, mapped,
                Map.copyOf(indexToOffset), firstIndex, lastIndex);

            channel = null;
            arena = null;

            return segment;
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to open segment: " + path, e);
        } finally {
            if (arena != null) {
                arena.close();
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {}
            }
        }
    }

    private SealedSegment(
        Path path,
        long segmentId,
        FileChannel channel,
        Arena arena,
        MemorySegment mapped,
        Map<Long, Long> indexToOffset,
        long firstIndex,
        long lastIndex
    ) {
        this.path = path;
        this.segmentId = segmentId;
        this.channel = channel;
        this.arena = arena;
        this.mapped = mapped;
        this.indexToOffset = indexToOffset;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
    }

    @Override
    public Optional<LogEntry> get(long index) {
        Long offset = indexToOffset.get(index);
        if (offset == null) {
            return Optional.empty();
        }
        return Optional.of(readEntryAtOffset(offset));
    }

    @Override
    public List<LogEntry> getRange(long startIndex, long endIndex) {
        return indexToOffset.keySet().stream()
            .filter(idx -> idx >= startIndex && idx < endIndex)
            .sorted()
            .map(this::get)
            .flatMap(Optional::stream)
            .toList();
    }

    @Override
    public List<LogEntry> readAll() {
        return indexToOffset.keySet().stream()
            .sorted()
            .map(this::get)
            .flatMap(Optional::stream)
            .toList();
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
    public long segmentId() {
        return segmentId;
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public long size() {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() {
        try {
            arena.close();
            channel.close();
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to close segment", e);
        }
    }

    private LogEntry readEntryAtOffset(long offset) {
        int length = mapped.get(ValueLayout.JAVA_INT, offset + 4);
        long index = mapped.get(ValueLayout.JAVA_LONG, offset + 8);

        byte[] entryData = new byte[length];
        MemorySegment.copy(mapped, ValueLayout.JAVA_BYTE, offset + ENTRY_HEADER_SIZE,
            entryData, 0, length);

        return LogEntryCodec.deserialize(index, entryData);
    }
}
