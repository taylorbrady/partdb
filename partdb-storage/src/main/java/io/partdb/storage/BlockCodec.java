package io.partdb.storage;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

final class BlockCodec {

    private static final int HEADER = 1 + Integer.BYTES + Integer.BYTES;
    private static final int CHECKSUM = Integer.BYTES;

    private BlockCodec() {
    }

    static byte[] encode(StoreOptions.Compression compression, byte[] raw) {
        byte[] data = compress(compression, raw);
        var buffer = Encoding.allocate(HEADER + data.length + CHECKSUM);
        buffer.put(id(compression));
        buffer.putInt(raw.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.putInt(Encoding.crc(buffer.array(), 0, buffer.position()));
        return buffer.array();
    }

    static byte[] decode(MemorySegment frame) {
        long size = frame.byteSize();
        Encoding.check(size >= HEADER + CHECKSUM, "Block frame too small: " + size);
        int stored = frame.get(Encoding.INT, size - CHECKSUM);
        Encoding.check(stored == Encoding.crc(frame.asSlice(0, size - CHECKSUM)), "Block checksum mismatch");

        StoreOptions.Compression compression = compression(frame.get(ValueLayout.JAVA_BYTE, 0));
        int rawSize = frame.get(Encoding.INT, 1);
        int dataSize = frame.get(Encoding.INT, 1 + Integer.BYTES);
        Encoding.check(rawSize >= 0 && dataSize >= 0, "Negative block frame size");
        Encoding.check((long) HEADER + dataSize + CHECKSUM == size, "Block frame size mismatch");
        byte[] data = frame.asSlice(HEADER, dataSize).toArray(ValueLayout.JAVA_BYTE);
        return decompress(compression, data, rawSize);
    }

    static byte[] compress(StoreOptions.Compression compression, byte[] raw) {
        return switch (compression) {
            case NONE -> raw;
            case DEFLATE -> deflate(raw);
        };
    }

    static byte[] decompress(StoreOptions.Compression compression, byte[] data, int rawSize) {
        byte[] raw = switch (compression) {
            case NONE -> data;
            case DEFLATE -> inflate(data, rawSize);
        };
        Encoding.check(raw.length == rawSize, "Block size mismatch: expected " + rawSize + ", got " + raw.length);
        return raw;
    }

    private static byte id(StoreOptions.Compression compression) {
        return switch (compression) {
            case NONE -> 0;
            case DEFLATE -> 1;
        };
    }

    private static StoreOptions.Compression compression(byte id) {
        return switch (id) {
            case 0 -> StoreOptions.Compression.NONE;
            case 1 -> StoreOptions.Compression.DEFLATE;
            default -> throw new StoreException.Corruption("Unknown block compression: " + id);
        };
    }

    private static byte[] deflate(byte[] raw) {
        Deflater deflater = new Deflater(Deflater.BEST_SPEED);
        try {
            deflater.setInput(raw);
            deflater.finish();
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, raw.length / 2));
            byte[] chunk = new byte[Math.max(256, raw.length)];
            while (!deflater.finished()) {
                out.write(chunk, 0, deflater.deflate(chunk));
            }
            return out.toByteArray();
        } finally {
            deflater.end();
        }
    }

    private static byte[] inflate(byte[] data, int rawSize) {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(data);
            byte[] raw = new byte[rawSize];
            int filled = 0;
            while (!inflater.finished()) {
                int read = inflater.inflate(raw, filled, raw.length - filled);
                if (read == 0 && (inflater.needsInput() || inflater.needsDictionary() || filled == raw.length)) {
                    break;
                }
                filled += read;
            }
            Encoding.check(inflater.finished() && filled == rawSize, "Block decompression size mismatch");
            return raw;
        } catch (DataFormatException e) {
            throw new StoreException.Corruption("Block decompression failed", e);
        } finally {
            inflater.end();
        }
    }
}
