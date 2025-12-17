package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

final class ZstdNative {

    private static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private static final SymbolLookup ZSTD = NativeLibrary.load("zstd");
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle ZSTD_COMPRESS_BOUND = LINKER.downcallHandle(
        ZSTD.find("ZSTD_compressBound").orElseThrow(),
        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
    );

    private static final MethodHandle ZSTD_COMPRESS = LINKER.downcallHandle(
        ZSTD.find("ZSTD_compress").orElseThrow(),
        FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_INT
        )
    );

    private static final MethodHandle ZSTD_DECOMPRESS = LINKER.downcallHandle(
        ZSTD.find("ZSTD_decompress").orElseThrow(),
        FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG
        )
    );

    private static final MethodHandle ZSTD_IS_ERROR = LINKER.downcallHandle(
        ZSTD.find("ZSTD_isError").orElseThrow(),
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG)
    );

    static byte[] compress(byte[] input) {
        try (Arena arena = Arena.ofConfined()) {
            long maxCompressedSize = (long) ZSTD_COMPRESS_BOUND.invokeExact((long) input.length);

            MemorySegment src = arena.allocate(input.length);
            src.copyFrom(MemorySegment.ofArray(input));

            MemorySegment dst = arena.allocate(maxCompressedSize);

            long compressedSize = (long) ZSTD_COMPRESS.invokeExact(
                dst,
                maxCompressedSize,
                src,
                (long) input.length,
                DEFAULT_COMPRESSION_LEVEL
            );

            int isError = (int) ZSTD_IS_ERROR.invokeExact(compressedSize);
            if (isError != 0) {
                throw new StorageException.Corruption("Zstd compression failed");
            }

            return dst.asSlice(0, compressedSize).toArray(ValueLayout.JAVA_BYTE);
        } catch (StorageException e) {
            throw e;
        } catch (Throwable t) {
            throw new StorageException.Corruption("Zstd compression error", t);
        }
    }

    static byte[] decompress(byte[] input, int uncompressedSize) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(input.length);
            src.copyFrom(MemorySegment.ofArray(input));

            MemorySegment dst = arena.allocate(uncompressedSize);

            long result = (long) ZSTD_DECOMPRESS.invokeExact(
                dst,
                (long) uncompressedSize,
                src,
                (long) input.length
            );

            int isError = (int) ZSTD_IS_ERROR.invokeExact(result);
            if (isError != 0) {
                throw new StorageException.Corruption("Zstd decompression failed");
            }

            if (result != uncompressedSize) {
                throw new StorageException.Corruption(
                    "Zstd decompression size mismatch: expected " + uncompressedSize + ", got " + result
                );
            }

            return dst.toArray(ValueLayout.JAVA_BYTE);
        } catch (StorageException e) {
            throw e;
        } catch (Throwable t) {
            throw new StorageException.Corruption("Zstd decompression error", t);
        }
    }
}
