package io.partdb.storage;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

enum BlockCodec {
    NONE((byte) 0) {
        @Override
        public byte[] compress(byte[] input) {
            return input;
        }

        @Override
        public byte[] decompress(byte[] input, int uncompressedSize) {
            return input;
        }
    },
    DEFLATE((byte) 1) {
        @Override
        public byte[] compress(byte[] input) {
            return deflate(input);
        }

        @Override
        public byte[] decompress(byte[] input, int uncompressedSize) {
            return inflate(input, uncompressedSize);
        }
    };

    private static final int DEFLATE_LEVEL = Deflater.BEST_SPEED;

    private final byte id;

    BlockCodec(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public abstract byte[] compress(byte[] input);

    public abstract byte[] decompress(byte[] input, int uncompressedSize);

    public static BlockCodec fromId(byte id) {
        for (BlockCodec codec : values()) {
            if (codec.id == id) {
                return codec;
            }
        }
        throw new StorageException.Corruption("Unknown codec id: " + id);
    }

    private static byte[] deflate(byte[] input) {
        Deflater deflater = new Deflater(DEFLATE_LEVEL);
        try {
            deflater.setInput(input);
            deflater.finish();

            ByteArrayOutputStream output = new ByteArrayOutputStream(input.length);
            byte[] buffer = new byte[Math.max(256, input.length)];
            while (!deflater.finished()) {
                int written = deflater.deflate(buffer);
                if (written <= 0 && deflater.needsInput()) {
                    break;
                }
                output.write(buffer, 0, written);
            }

            if (!deflater.finished()) {
                throw new StorageException.Corruption("Deflate compression failed");
            }

            return output.toByteArray();
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException.Corruption("Deflate compression error", e);
        } finally {
            deflater.end();
        }
    }

    private static byte[] inflate(byte[] input, int uncompressedSize) {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(input);

            ByteArrayOutputStream output = new ByteArrayOutputStream(uncompressedSize);
            byte[] buffer = new byte[Math.max(256, uncompressedSize)];
            while (!inflater.finished()) {
                int read = inflater.inflate(buffer);
                if (read == 0) {
                    if (inflater.needsInput()) {
                        throw new StorageException.Corruption("Deflate decompression ended early");
                    }
                    if (inflater.needsDictionary()) {
                        throw new StorageException.Corruption("Deflate decompression requires a dictionary");
                    }
                } else {
                    output.write(buffer, 0, read);
                }
            }

            byte[] result = output.toByteArray();
            if (result.length != uncompressedSize) {
                throw new StorageException.Corruption(
                    "Deflate decompression size mismatch: expected " + uncompressedSize + ", got " + result.length
                );
            }
            return result;
        } catch (StorageException e) {
            throw e;
        } catch (DataFormatException e) {
            throw new StorageException.Corruption("Deflate decompression error", e);
        } finally {
            inflater.end();
        }
    }
}
