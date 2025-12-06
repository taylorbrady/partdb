package io.partdb.server.storage;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

import static io.partdb.server.storage.LogCodec.BYTE_ORDER;

public final class ActiveSegment implements LogSegment {

    private static final int WRITE_BUFFER_SIZE = 64 * 1024;

    private final Path path;
    private final int sequence;
    private final long firstIndex;
    private final FileChannel channel;
    private final ByteBuffer writeBuffer;
    private final CRC32C crc;

    private long lastIndex;
    private long fileSize;

    private ActiveSegment(Path path, int sequence, long firstIndex, FileChannel channel) {
        this.path = path;
        this.sequence = sequence;
        this.firstIndex = firstIndex;
        this.channel = channel;
        this.writeBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE).order(BYTE_ORDER);
        this.crc = new CRC32C();
        this.lastIndex = firstIndex - 1;
        this.fileSize = 0;
    }

    public static ActiveSegment create(Path directory, int sequence, long firstIndex) {
        Path path = directory.resolve(LogSegment.formatFileName(sequence, firstIndex));
        try {
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);
            return new ActiveSegment(path, sequence, firstIndex, channel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ActiveSegment openForAppend(Path path, int sequence, long firstIndex, long lastIndex) {
        try {
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);
            var segment = new ActiveSegment(path, sequence, firstIndex, channel);
            segment.lastIndex = lastIndex;
            segment.fileSize = channel.size();
            channel.position(segment.fileSize);
            return segment;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
        return fileSize;
    }

    @Override
    public void close() {
        flush();
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void append(LogRecord record) {
        byte[] payload = encodePayload(record);
        int recordSize = RECORD_HEADER_SIZE + payload.length;

        if (writeBuffer.remaining() < recordSize) {
            flush();
        }

        if (recordSize > WRITE_BUFFER_SIZE) {
            flush();
            writeRecordDirect(record, payload);
        } else {
            writeRecordToBuffer(record, payload);
        }

        switch (record) {
            case LogRecord.Entry(LogEntry entry) -> lastIndex = entry.index();
            case LogRecord.State _, LogRecord.SnapshotMarker _ -> {}
        }
    }

    private void writeRecordToBuffer(LogRecord record, byte[] payload) {
        int crcValue = computeCrc(recordType(record), payload);

        writeBuffer.putInt(payload.length);
        writeBuffer.putInt(crcValue);
        writeBuffer.put(recordType(record));
        writeBuffer.put(payload);

        fileSize += RECORD_HEADER_SIZE + payload.length;
    }

    private void writeRecordDirect(LogRecord record, byte[] payload) {
        int crcValue = computeCrc(recordType(record), payload);

        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE).order(BYTE_ORDER);
        header.putInt(payload.length);
        header.putInt(crcValue);
        header.put(recordType(record));
        header.flip();

        try {
            channel.write(header);
            channel.write(ByteBuffer.wrap(payload));
            fileSize += RECORD_HEADER_SIZE + payload.length;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] encodePayload(LogRecord record) {
        return switch (record) {
            case LogRecord.Entry(LogEntry entry) -> {
                int size = LogCodec.entrySize(entry);
                ByteBuffer buf = ByteBuffer.allocate(size).order(BYTE_ORDER);
                LogCodec.writeEntry(buf, entry);
                yield buf.array();
            }
            case LogRecord.State(HardState state) -> {
                int size = LogCodec.hardStateSize(state);
                ByteBuffer buf = ByteBuffer.allocate(size).order(BYTE_ORDER);
                LogCodec.writeHardState(buf, state);
                yield buf.array();
            }
            case LogRecord.SnapshotMarker(long index, long term) -> {
                ByteBuffer buf = ByteBuffer.allocate(16).order(BYTE_ORDER);
                buf.putLong(index);
                buf.putLong(term);
                yield buf.array();
            }
        };
    }

    private byte recordType(LogRecord record) {
        return switch (record) {
            case LogRecord.Entry _ -> LogRecord.TYPE_ENTRY;
            case LogRecord.State _ -> LogRecord.TYPE_STATE;
            case LogRecord.SnapshotMarker _ -> LogRecord.TYPE_SNAPSHOT_MARKER;
        };
    }

    private int computeCrc(byte type, byte[] payload) {
        crc.reset();
        crc.update(type);
        crc.update(payload);
        return (int) crc.getValue();
    }

    public void flush() {
        if (writeBuffer.position() == 0) {
            return;
        }
        writeBuffer.flip();
        try {
            while (writeBuffer.hasRemaining()) {
                channel.write(writeBuffer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writeBuffer.clear();
    }

    public void sync() {
        flush();
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SealedSegment seal() {
        sync();
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return SealedSegment.open(path, sequence, firstIndex, lastIndex);
    }
}
