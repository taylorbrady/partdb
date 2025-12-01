package io.partdb.raft;

import java.io.IOException;
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

final class ActiveSegment implements LogSegment {

    private final Path path;
    private final long segmentId;
    private final FileChannel channel;
    private final ReentrantLock channelLock = new ReentrantLock();
    private final Map<Long, Long> indexToOffset;

    private long firstIndex;
    private long lastIndex;
    private boolean sealed;

    static ActiveSegment create(Path path, long segmentId, long firstIndex) {
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

            ByteBuffer header = ByteBuffer.allocate(SealedSegment.HEADER_SIZE);
            header.putInt(SealedSegment.MAGIC_NUMBER);
            header.putInt(SealedSegment.VERSION);
            header.putLong(firstIndex);
            header.flip();

            while (header.hasRemaining()) {
                channel.write(header);
            }
            channel.force(true);

            var segment = new ActiveSegment(path, segmentId, channel, new HashMap<>(), firstIndex, firstIndex - 1);

            channel = null;

            return segment;
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to create segment: " + path, e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {}
            }
        }
    }

    static ActiveSegment open(Path path, long segmentId) {
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

            long fileSize = channel.size();

            ByteBuffer headerBuf = ByteBuffer.allocate(SealedSegment.HEADER_SIZE);
            channel.read(headerBuf);
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != SealedSegment.MAGIC_NUMBER) {
                throw new RaftException.LogException("Invalid magic number in segment: " + path);
            }

            int version = headerBuf.getInt();
            if (version != SealedSegment.VERSION) {
                throw new RaftException.LogException("Unsupported version in segment: " + path);
            }

            long firstIndex = headerBuf.getLong();
            Map<Long, Long> indexToOffset = new HashMap<>();
            long lastIndex = firstIndex - 1;

            long position = SealedSegment.HEADER_SIZE;
            while (position < fileSize) {
                if (position + SealedSegment.ENTRY_HEADER_SIZE > fileSize) {
                    break;
                }

                channel.position(position);
                ByteBuffer entryHeader = ByteBuffer.allocate(SealedSegment.ENTRY_HEADER_SIZE);
                if (channel.read(entryHeader) < SealedSegment.ENTRY_HEADER_SIZE) {
                    break;
                }
                entryHeader.flip();

                int crc = entryHeader.getInt();
                int length = entryHeader.getInt();
                long index = entryHeader.getLong();

                if (position + SealedSegment.ENTRY_HEADER_SIZE + length > fileSize) {
                    break;
                }

                ByteBuffer entryData = ByteBuffer.allocate(length);
                int bytesRead = 0;
                while (bytesRead < length) {
                    int read = channel.read(entryData);
                    if (read == -1) {
                        break;
                    }
                    bytesRead += read;
                }

                if (bytesRead < length) {
                    break;
                }

                entryData.flip();
                CRC32C crc32c = new CRC32C();
                crc32c.update(entryData.array());
                if ((int) crc32c.getValue() != crc) {
                    throw new RaftException.LogException("CRC mismatch in segment: " + path);
                }

                indexToOffset.put(index, position);
                lastIndex = index;
                position += SealedSegment.ENTRY_HEADER_SIZE + length;
            }

            var segment = new ActiveSegment(path, segmentId, channel, indexToOffset, firstIndex, lastIndex);

            channel = null;

            return segment;
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to open segment: " + path, e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {}
            }
        }
    }

    private ActiveSegment(
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
        this.sealed = false;
    }

    void append(LogEntry entry) {
        checkNotSealed();
        try {
            long entryOffset = channel.size();
            byte[] serialized = LogEntryCodec.serialize(entry);

            CRC32C crc = new CRC32C();
            crc.update(serialized);

            ByteBuffer buffer = ByteBuffer.allocate(SealedSegment.ENTRY_HEADER_SIZE + serialized.length);
            buffer.putInt((int) crc.getValue());
            buffer.putInt(serialized.length);
            buffer.putLong(entry.index());
            buffer.put(serialized);
            buffer.flip();

            channel.position(entryOffset);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            indexToOffset.put(entry.index(), entryOffset);
            lastIndex = entry.index();
            if (firstIndex < 0 || indexToOffset.size() == 1) {
                firstIndex = entry.index();
            }
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to append entry", e);
        }
    }

    void sync() {
        checkNotSealed();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to sync segment", e);
        }
    }

    SealedSegment seal() {
        checkNotSealed();
        sync();
        try {
            channel.close();
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to close segment before sealing", e);
        }
        sealed = true;
        return SealedSegment.open(path, segmentId);
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
        if (!sealed) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new RaftException.LogException("Failed to close segment", e);
            }
        }
    }

    private void checkNotSealed() {
        if (sealed) {
            throw new IllegalStateException("Segment has been sealed");
        }
    }

    private LogEntry readEntryAtOffset(long offset) {
        channelLock.lock();
        try {
            channel.position(offset);

            ByteBuffer entryHeader = ByteBuffer.allocate(SealedSegment.ENTRY_HEADER_SIZE);
            if (channel.read(entryHeader) < SealedSegment.ENTRY_HEADER_SIZE) {
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

            return LogEntryCodec.deserialize(index, entryData.array());
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to read entry at offset " + offset, e);
        } finally {
            channelLock.unlock();
        }
    }
}
