package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

final class Lz4Native {

    private static final SymbolLookup LZ4 = NativeLibrary.load("lz4");
    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle LZ4_COMPRESS_BOUND = LINKER.downcallHandle(
        LZ4.find("LZ4_compressBound").orElseThrow(),
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
    );

    private static final MethodHandle LZ4_COMPRESS_DEFAULT = LINKER.downcallHandle(
        LZ4.find("LZ4_compress_default").orElseThrow(),
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT
        )
    );

    private static final MethodHandle LZ4_DECOMPRESS_SAFE = LINKER.downcallHandle(
        LZ4.find("LZ4_decompress_safe").orElseThrow(),
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT
        )
    );

    static byte[] compress(byte[] input) {
        try (Arena arena = Arena.ofConfined()) {
            int maxCompressedSize = (int) LZ4_COMPRESS_BOUND.invokeExact(input.length);

            MemorySegment src = arena.allocate(input.length);
            src.copyFrom(MemorySegment.ofArray(input));

            MemorySegment dst = arena.allocate(maxCompressedSize);

            int compressedSize = (int) LZ4_COMPRESS_DEFAULT.invokeExact(
                src,
                dst,
                input.length,
                maxCompressedSize
            );

            if (compressedSize <= 0) {
                throw new StorageException.Corruption("LZ4 compression failed");
            }

            return dst.asSlice(0, compressedSize).toArray(ValueLayout.JAVA_BYTE);
        } catch (StorageException e) {
            throw e;
        } catch (Throwable t) {
            throw new StorageException.Corruption("LZ4 compression error", t);
        }
    }

    static byte[] decompress(byte[] input, int uncompressedSize) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(input.length);
            src.copyFrom(MemorySegment.ofArray(input));

            MemorySegment dst = arena.allocate(uncompressedSize);

            int result = (int) LZ4_DECOMPRESS_SAFE.invokeExact(
                src,
                dst,
                input.length,
                uncompressedSize
            );

            if (result != uncompressedSize) {
                throw new StorageException.Corruption(
                    "LZ4 decompression failed: expected " + uncompressedSize + ", got " + result
                );
            }

            return dst.toArray(ValueLayout.JAVA_BYTE);
        } catch (StorageException e) {
            throw e;
        } catch (Throwable t) {
            throw new StorageException.Corruption("LZ4 decompression error", t);
        }
    }
}
