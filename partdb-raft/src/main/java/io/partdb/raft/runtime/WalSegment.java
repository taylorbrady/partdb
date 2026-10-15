package io.partdb.raft.runtime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

final class WalSegment implements AutoCloseable {

    static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES + Byte.BYTES;
    private static final int BUFFER_BYTES = 256 * 1024;

    record Scan(List<WalRecord> records, long validBytes, boolean complete) {}

    private final Path path;
    private final FileChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_BYTES).order(BinaryFormat.ORDER);
    private final CRC32C crc = new CRC32C();
    private long size;

    private WalSegment(Path path, FileChannel channel, long size) {
        this.path = path;
        this.channel = channel;
        this.size = size;
    }

    static WalSegment create(Path path) {
        try {
            return new WalSegment(path, FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), 0);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to create WAL segment " + path, e);
        }
    }

    static WalSegment reopen(Path path, long validBytes) {
        try {
            var channel = FileChannel.open(path, StandardOpenOption.WRITE);
            if (channel.size() > validBytes) {
                channel.truncate(validBytes);
                channel.force(true);
            }
            channel.position(validBytes);
            return new WalSegment(path, channel, validBytes);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open WAL segment " + path, e);
        }
    }

    static Scan scan(Path path) {
        try {
            var bytes = ByteBuffer.wrap(Files.readAllBytes(path)).order(BinaryFormat.ORDER);
            var records = new ArrayList<WalRecord>();
            var crc = new CRC32C();
            while (bytes.remaining() >= HEADER_BYTES) {
                int start = bytes.position();
                int length = bytes.getInt();
                int checksum = bytes.getInt();
                byte type = bytes.get();
                if (length < 0 || bytes.remaining() < length) {
                    return new Scan(records, start, false);
                }
                var payload = bytes.slice(bytes.position(), length).order(BinaryFormat.ORDER);
                crc.reset();
                crc.update(type);
                crc.update(payload.duplicate());
                if ((int) crc.getValue() != checksum) {
                    return new Scan(records, start, false);
                }
                records.add(WalRecord.decode(type, payload));
                bytes.position(bytes.position() + length);
            }
            return new Scan(records, bytes.position(), !bytes.hasRemaining());
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to read WAL segment " + path, e);
        }
    }

    Path path() {
        return path;
    }

    long size() {
        return size;
    }

    void append(WalRecord record) {
        var payload = record.encode();
        crc.reset();
        crc.update(record.type());
        crc.update(payload.duplicate());
        int length = payload.remaining();
        if (buffer.remaining() < HEADER_BYTES + length) {
            flush();
        }
        if (buffer.remaining() < HEADER_BYTES + length) {
            var header = ByteBuffer.allocate(HEADER_BYTES).order(BinaryFormat.ORDER);
            header.putInt(length).putInt((int) crc.getValue()).put(record.type()).flip();
            write(header);
            write(payload);
        } else {
            buffer.putInt(length).putInt((int) crc.getValue()).put(record.type()).put(payload);
        }
        size += HEADER_BYTES + length;
    }

    void sync() {
        flush();
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to sync WAL segment " + path, e);
        }
    }

    @Override
    public void close() {
        sync();
        try {
            channel.close();
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to close WAL segment " + path, e);
        }
    }

    private void flush() {
        buffer.flip();
        write(buffer);
        buffer.clear();
    }

    private void write(ByteBuffer source) {
        try {
            while (source.hasRemaining()) {
                channel.write(source);
            }
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to write WAL segment " + path, e);
        }
    }
}
