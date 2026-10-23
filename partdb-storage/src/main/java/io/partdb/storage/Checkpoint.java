package io.partdb.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
    private static final int CHUNK = 1 << 16;

    record Part(int level, MemorySegment contents) {}

    record Restored(long revision, Version version) {}

    private Checkpoint() {
    }

    static void write(OutputStream out, long revision, List<Part> parts) throws IOException {
        ByteBuffer header = Encoding.allocate(HEADER_SIZE);
        header.putInt(MAGIC).putInt(VERSION).putLong(revision).putInt(parts.size());
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

    static Restored read(InputStream in, StoreDirectory directory, BlockCache cache) {
        ByteBuffer header = Encoding.wrap(readFully(in, HEADER_SIZE));
        Encoding.check(header.getInt() == MAGIC, "invalid checkpoint magic");
        int version = header.getInt();
        Encoding.check(version == VERSION, "unsupported checkpoint version: " + version);
        long revision = header.getLong();
        int count = header.getInt();
        Encoding.check(header.getInt() == Encoding.crc(header.array(), 0, HEADER_SIZE - Integer.BYTES), "checkpoint header checksum mismatch");
        Encoding.check(revision >= 0 && count >= 0, "invalid checkpoint header");

        List<List<TableReader>> levels = Version.emptyLevels();
        List<TableReader> opened = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ByteBuffer tableHeader = Encoding.wrap(readFully(in, TABLE_HEADER_SIZE));
                int level = tableHeader.getInt();
                long size = tableHeader.getLong();
                Encoding.check(level >= 0 && level < Version.LEVELS && size > 0, "invalid checkpoint table header");
                long id = directory.allocateTableId();
                Path path = directory.tablePath(id);
                copy(in, path, size);
                TableReader table = TableReader.open(path, id, cache);
                opened.add(table);
                levels.get(level).add(table);
            }
            directory.sync();
            return new Restored(revision, Version.of(levels));
        } catch (RuntimeException e) {
            opened.forEach(TableReader::markObsolete);
            throw e;
        } finally {
            opened.forEach(TableReader::release);
        }
    }

    private static void copy(InputStream in, Path path, long size) {
        CRC32C crc = new CRC32C();
        byte[] chunk = new byte[CHUNK];
        try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            long remaining = size;
            while (remaining > 0) {
                int read = in.readNBytes(chunk, 0, (int) Math.min(CHUNK, remaining));
                Encoding.check(read > 0, "truncated checkpoint table");
                crc.update(chunk, 0, read);
                ByteBuffer buffer = ByteBuffer.wrap(chunk, 0, read);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                remaining -= read;
            }
            int expected = Encoding.wrap(readFully(in, Integer.BYTES)).getInt();
            Encoding.check(expected == (int) crc.getValue(), "checkpoint table checksum mismatch");
            channel.force(true);
        } catch (IOException e) {
            discard(path, e);
            throw new StoreException.IO("failed to stage checkpoint table " + path, e);
        } catch (RuntimeException e) {
            discard(path, e);
            throw e;
        }
    }

    private static void discard(Path path, Exception failure) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            failure.addSuppressed(e);
        }
    }

    private static byte[] readFully(InputStream in, int length) {
        try {
            byte[] bytes = in.readNBytes(length);
            Encoding.check(bytes.length == length, "truncated checkpoint");
            return bytes;
        } catch (IOException e) {
            throw new StoreException.IO("failed to read checkpoint", e);
        }
    }
}
