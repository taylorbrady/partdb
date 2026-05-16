package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class SSTableReader implements AutoCloseable {

    private static final AtomicLong NEXT_CACHE_ID = new AtomicLong();

    private final long id;
    private final long cacheId;
    private final int level;
    private final Path path;
    private final Arena arena;
    private final MemorySegment segment;
    private final BloomFilter bloomFilter;
    private final DataBlockIndex index;
    private final SSTableFooter footer;
    private final BlockCache cache;
    private final BlockCodec codec;
    private final long fileSizeBytes;
    private final AtomicInteger references;

    private SSTableReader(
        long id,
        long cacheId,
        int level,
        Path path,
        Arena arena,
        MemorySegment segment,
        BloomFilter bloomFilter,
        DataBlockIndex index,
        SSTableFooter footer,
        BlockCache cache,
        BlockCodec codec,
        long fileSizeBytes,
        AtomicInteger references
    ) {
        this.id = id;
        this.cacheId = cacheId;
        this.level = level;
        this.path = path;
        this.arena = arena;
        this.segment = segment;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.footer = footer;
        this.cache = cache;
        this.codec = codec;
        this.fileSizeBytes = fileSizeBytes;
        this.references = references;
    }

    static SSTableReader open(long id, int level, Path path, BlockCache cache) {
        Arena arena = Arena.ofShared();
        try {
            long fileSize;
            MemorySegment segment;

            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                fileSize = channel.size();
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            }

            SSTableHeader header = readHeader(segment);
            BlockCodec codec = BlockCodec.fromId(header.codecId());
            SSTableFooter footer = readFooter(segment, fileSize);
            BloomFilter bloomFilter = readBloomFilter(segment, footer);
            DataBlockIndex index = readIndex(segment, footer, fileSize);

            return new SSTableReader(
                id,
                NEXT_CACHE_ID.incrementAndGet(),
                level,
                path,
                arena,
                segment,
                bloomFilter,
                index,
                footer,
                cache,
                codec,
                fileSize,
                new AtomicInteger(1)
            );
        } catch (RuntimeException e) {
            arena.close();
            throw e;
        } catch (IOException e) {
            arena.close();
            throw new StorageException.IO("Failed to open SSTable: " + path, e);
        }
    }

    long id() {
        return id;
    }

    int level() {
        return level;
    }

    Optional<StoredEntry> get(Slice key, long snapshotRevision) {
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        Optional<DataBlockIndex.Entry> blockEntry = index.find(key);
        if (blockEntry.isEmpty()) {
            return Optional.empty();
        }

        DataBlockReader block = loadBlock(blockEntry.get().handle());
        return block.findVisible(key, snapshotRevision);
    }

    Iterator<InternalEntry> scan(ScanBounds bounds) {
        return new ScanIterator(bounds);
    }

    Path path() {
        return path;
    }

    byte[] fileBytes() {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    Slice smallestKey() {
        return footer.smallestKey();
    }

    Slice largestKey() {
        return footer.largestKey();
    }

    long entryCount() {
        return footer.entryCount();
    }

    long smallestRevision() {
        return footer.smallestRevision();
    }

    long largestRevision() {
        return footer.largestRevision();
    }

    long fileSizeBytes() {
        return fileSizeBytes;
    }

    SSTableMetadata metadata() {
        return new SSTableMetadata(
            id,
            level,
            smallestKey(),
            largestKey(),
            smallestRevision(),
            largestRevision(),
            fileSizeBytes,
            entryCount()
        );
    }

    boolean mightContain(Slice key) {
        return key.compareTo(smallestKey()) >= 0 && key.compareTo(largestKey()) <= 0;
    }

    SSTableReader retain() {
        while (true) {
            int current = references.get();
            if (current <= 0) {
                throw new StorageException.Closed("SSTable reader is closed");
            }
            if (references.compareAndSet(current, current + 1)) {
                return this;
            }
        }
    }

    @Override
    public void close() {
        int remaining = references.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("SSTable reader over-closed");
        }
        if (remaining == 0) {
            cache.invalidate(cacheId);
            arena.close();
        }
    }

    private DataBlockReader loadBlock(BlockHandle handle) {
        DataBlockReader cached = cache.get(cacheId, handle.offset());
        if (cached != null) {
            return cached;
        }

        MemorySegment blockSegment = segment.asSlice(handle.offset(), handle.size());
        CompressedBlock compressed = CompressedBlock.deserialize(blockSegment);
        byte[] decompressed = codec.decompress(compressed.data(), compressed.uncompressedSize());
        DataBlockReader block = DataBlockReader.from(MemorySegment.ofArray(decompressed));

        cache.put(cacheId, handle.offset(), block);

        return block;
    }

    private static SSTableHeader readHeader(MemorySegment segment) {
        if (segment.byteSize() < SSTableHeader.HEADER_SIZE) {
            throw new StorageException.Corruption("SSTable too small for header");
        }
        int magic = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int version = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        byte codecId = segment.get(ValueLayout.JAVA_BYTE, 8);
        return new SSTableHeader(magic, version, codecId);
    }

    private static SSTableFooter readFooter(MemorySegment segment, long fileSize) {
        if (fileSize < SSTableHeader.HEADER_SIZE + 8) {
            throw new StorageException.Corruption("SSTable too small for footer");
        }
        int footerSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, fileSize - 8);
        if (footerSize <= 0 || footerSize > fileSize - SSTableHeader.HEADER_SIZE) {
            throw new StorageException.Corruption("Invalid SSTable footer size: " + footerSize);
        }
        long footerOffset = fileSize - footerSize;
        MemorySegment footerSegment = segment.asSlice(footerOffset, footerSize);
        return SSTableFooter.deserialize(footerSegment);
    }

    private static BloomFilter readBloomFilter(MemorySegment segment, SSTableFooter footer) {
        validateRange(segment.byteSize(), footer.bloomFilterOffset(), footer.bloomFilterSize(), "bloom filter");
        MemorySegment bloomSegment = segment.asSlice(footer.bloomFilterOffset(), footer.bloomFilterSize());
        return BloomFilter.from(bloomSegment);
    }

    private static DataBlockIndex readIndex(MemorySegment segment, SSTableFooter footer, long fileSize) {
        long indexOffset = footer.indexOffset();
        int footerSize = SSTableFooter.calculateFooterSize(footer.smallestKey(), footer.largestKey());
        long indexSize = fileSize - footerSize - indexOffset;
        if (indexSize < 0 || indexSize > Integer.MAX_VALUE) {
            throw new StorageException.Corruption("Invalid SSTable index size: " + indexSize);
        }
        validateRange(segment.byteSize(), indexOffset, (int) indexSize, "index");
        MemorySegment indexSegment = segment.asSlice(indexOffset, indexSize);
        return DataBlockIndex.deserialize(indexSegment, footer.blockCount());
    }

    private static void validateRange(long totalSize, long offset, int size, String label) {
        if (offset < 0) {
            throw new StorageException.Corruption("Negative SSTable " + label + " offset");
        }
        if (size < 0) {
            throw new StorageException.Corruption("Negative SSTable " + label + " size");
        }
        if (offset + size > totalSize) {
            throw new StorageException.Corruption(
                "SSTable " + label + " exceeds file bounds: offset=%d size=%d total=%d"
                    .formatted(offset, size, totalSize)
            );
        }
    }

    private final class ScanIterator implements Iterator<InternalEntry> {

        private final ScanBounds bounds;
        private final List<DataBlockIndex.Entry> blocks;
        private int currentBlockIndex;
        private DataBlockCursor currentBlockCursor;
        private InternalEntry nextEntry;

        private ScanIterator(ScanBounds bounds) {
            this.bounds = bounds;
            this.blocks = bounds.isAll()
                ? index.entries()
                : index.findInRange(bounds);
            this.currentBlockIndex = 0;
            this.currentBlockCursor = null;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public InternalEntry next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            InternalEntry result = nextEntry;
            advance();
            return result;
        }

        private void advance() {
            while (true) {
                if (currentBlockCursor != null && currentBlockCursor.hasNext()) {
                    InternalEntry entry = currentBlockCursor.next();

                    if (!bounds.includes(entry.userKey())) {
                        Slice endExclusive = bounds.endExclusive();
                        if (endExclusive != null && entry.userKey().compareTo(endExclusive) >= 0) {
                            nextEntry = null;
                            return;
                        }
                        continue;
                    }

                    nextEntry = entry;
                    return;
                }

                if (currentBlockIndex >= blocks.size()) {
                    nextEntry = null;
                    return;
                }

                DataBlockIndex.Entry blockEntry = blocks.get(currentBlockIndex);
                DataBlockReader block = loadBlock(blockEntry.handle());
                currentBlockCursor = currentBlockIndex == 0 && bounds.startInclusive() != null
                    ? block.cursorAtOrAfter(bounds.startInclusive())
                    : block.cursor();
                currentBlockIndex++;
            }
        }
    }
}
