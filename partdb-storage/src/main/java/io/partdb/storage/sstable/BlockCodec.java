package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

public sealed interface BlockCodec permits BlockCodec.None, BlockCodec.Lz4, BlockCodec.Zstd {

    None NONE = new None();
    Lz4 LZ4 = new Lz4();
    Zstd ZSTD = new Zstd();

    byte id();

    byte[] compress(byte[] input);

    byte[] decompress(byte[] input, int uncompressedSize);

    String name();

    record None() implements BlockCodec {
        public static final byte ID = 0;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public byte[] compress(byte[] input) {
            return input;
        }

        @Override
        public byte[] decompress(byte[] input, int uncompressedSize) {
            return input;
        }

        @Override
        public String name() {
            return "none";
        }
    }

    record Lz4() implements BlockCodec {
        public static final byte ID = 1;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public byte[] compress(byte[] input) {
            return Lz4Native.compress(input);
        }

        @Override
        public byte[] decompress(byte[] input, int uncompressedSize) {
            return Lz4Native.decompress(input, uncompressedSize);
        }

        @Override
        public String name() {
            return "lz4";
        }
    }

    record Zstd() implements BlockCodec {
        public static final byte ID = 2;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public byte[] compress(byte[] input) {
            return ZstdNative.compress(input);
        }

        @Override
        public byte[] decompress(byte[] input, int uncompressedSize) {
            return ZstdNative.decompress(input, uncompressedSize);
        }

        @Override
        public String name() {
            return "zstd";
        }
    }

    static BlockCodec fromId(byte id) {
        return switch (id) {
            case None.ID -> NONE;
            case Lz4.ID -> LZ4;
            case Zstd.ID -> ZSTD;
            default -> throw new StorageException.Corruption("Unknown codec id: " + id);
        };
    }
}
