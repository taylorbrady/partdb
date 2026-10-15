package io.partdb.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

final class Checkpoint {

    private static final int MAGIC = 0x43424450;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    private static final int TABLE_HEADER_SIZE = 4 + 8;
    private static final int MAX_LEVEL = 64;
    private static final int CHUNK = 1 << 16;

    record Part(int level, MemorySegment contents) {}

    record Restored(long index, Version version) {}

    private Checkpoint() {
    }

    static void write(OutputStream out, long index, List<Part> parts) throws IOException {
        ByteBuffer header = Encoding.allocate(HEADER_SIZE);
        header.putInt(MAGIC).putInt(VERSION).putLong(index).putInt(parts.size());
        header.putInt(Encoding.crc(header.array(), 0, header.position()));
        out.write(header.array());

        byte[] chunk = new byte[CHUNK];
        for (Part part : parts) {
            MemorySegment contents = part.contents();
            out.write(Encoding.allocate(TABLE_HEADER_SIZE).putInt(part.level()).putLong(contents.byteSize()).array());
            for (long offset = 0; offset < contents.byteSize(); offset += CHUNK) {
                int length = (int) Math.min(CHUNK, contents.byteSize() - offset);
                MemorySegment.copy(contents, ValueLayout.JAVA_BYTE, offset, chunk, 0, length);
                out.write(chunk, 0, length);
            }
            out.write(Encoding.allocate(Integer.BYTES).putInt(Encoding.crc(contents)).array());
        }
        out.flush();
    }

    static Restored read(InputStream in, Generation staged, BlockCache cache, int levelCount) {
        ByteBuffer header = Encoding.wrap(readFully(in, HEADER_SIZE));
        Encoding.check(header.getInt() == MAGIC, "Invalid checkpoint magic");
        int version = header.getInt();
        Encoding.check(version == VERSION, "Unsupported checkpoint version: " + version);
        long index = header.getLong();
        int count = header.getInt();
        Encoding.check(header.getInt() == Encoding.crc(header.array(), 0, HEADER_SIZE - Integer.BYTES), "Checkpoint header checksum mismatch");
        Encoding.check(index >= 0 && count >= 0, "Invalid checkpoint header");

        List<TableReader> tables = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ByteBuffer tableHeader = Encoding.wrap(readFully(in, TABLE_HEADER_SIZE));
                int level = tableHeader.getInt();
                long size = tableHeader.getLong();
                Encoding.check(level >= 0 && level < MAX_LEVEL && size > 0, "Invalid checkpoint table header");
                long id = staged.allocateTableId();
                Path path = staged.tablePath(id);
                copy(in, path, size);
                tables.add(TableReader.open(path, id, level, cache));
            }
            Disk.syncDirectory(staged.directory());
            Version restored = Version.of(tables, levelCount);
            try {
                staged.commit(Manifest.of(index, staged.nextTableId(), restored));
            } catch (RuntimeException e) {
                restored.release();
                throw e;
            }
            return new Restored(index, restored);
        } finally {
            tables.forEach(TableReader::release);
        }
    }

    private static void copy(InputStream in, Path path, long size) {
        CRC32C crc = new CRC32C();
        byte[] chunk = new byte[CHUNK];
        try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            long remaining = size;
            while (remaining > 0) {
                int read = in.readNBytes(chunk, 0, (int) Math.min(CHUNK, remaining));
                Encoding.check(read > 0, "Truncated checkpoint table");
                crc.update(chunk, 0, read);
                ByteBuffer buffer = ByteBuffer.wrap(chunk, 0, read);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                remaining -= read;
            }
            int expected = Encoding.wrap(readFully(in, Integer.BYTES)).getInt();
            Encoding.check(expected == (int) crc.getValue(), "Checkpoint table checksum mismatch");
            channel.force(true);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to stage checkpoint table " + path, e);
        }
    }

    private static byte[] readFully(InputStream in, int length) {
        try {
            byte[] bytes = in.readNBytes(length);
            Encoding.check(bytes.length == length, "Truncated checkpoint");
            return bytes;
        } catch (IOException e) {
            throw new StoreException.IO("Failed to read checkpoint", e);
        }
    }
}
