package io.partdb.storage;

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

final class SSTableReader implements AutoCloseable {

    private final long id;
    private final int level;
    private final Path path;
    private final Arena arena;
    private final MemorySegment segment;
    private final BloomFilter bloomFilter;
    private final BlockIndex index;
    private final SSTableFooter footer;
    private final BlockCache cache;
    private final BlockCodec codec;
    private final long fileSizeBytes;

    private SSTableReader(
        long id,
        int level,
        Path path,
        Arena arena,
        MemorySegment segment,
        BloomFilter bloomFilter,
        BlockIndex index,
        SSTableFooter footer,
        BlockCache cache,
        BlockCodec codec,
        long fileSizeBytes
    ) {
        this.id = id;
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
            BlockIndex index = readIndex(segment, footer, fileSize);

            return new SSTableReader(
                id,
                level,
                path,
                arena,
                segment,
                bloomFilter,
                index,
                footer,
                cache,
                codec,
                fileSize
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

    Optional<StoredEntry> get(Slice key) {
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        Optional<BlockIndex.Entry> blockEntry = index.find(key);
        if (blockEntry.isEmpty()) {
            return Optional.empty();
        }

        Block block = loadBlock(blockEntry.get().handle());
        return block.find(key);
    }

    Iterator<StoredEntry> scan(ScanBounds bounds) {
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

    @Override
    public void close() {
        cache.invalidate(id);
        arena.close();
    }

    private Block loadBlock(BlockHandle handle) {
        Block cached = cache.get(id, handle.offset());
        if (cached != null) {
            return cached;
        }

        MemorySegment blockSegment = segment.asSlice(handle.offset(), handle.size());
        CompressedBlock compressed = CompressedBlock.deserialize(blockSegment);
        byte[] decompressed = codec.decompress(compressed.data(), compressed.uncompressedSize());
        Block block = Block.from(MemorySegment.ofArray(decompressed));

        cache.put(id, handle.offset(), block);

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

    private static BlockIndex readIndex(MemorySegment segment, SSTableFooter footer, long fileSize) {
        long indexOffset = footer.indexOffset();
        int footerSize = SSTableFooter.calculateFooterSize(footer.smallestKey(), footer.largestKey());
        long indexSize = fileSize - footerSize - indexOffset;
        if (indexSize < 0 || indexSize > Integer.MAX_VALUE) {
            throw new StorageException.Corruption("Invalid SSTable index size: " + indexSize);
        }
        validateRange(segment.byteSize(), indexOffset, (int) indexSize, "index");
        MemorySegment indexSegment = segment.asSlice(indexOffset, indexSize);
        return BlockIndex.deserialize(indexSegment, footer.blockCount());
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

    private final class ScanIterator implements Iterator<StoredEntry> {

        private final ScanBounds bounds;
        private final List<BlockIndex.Entry> blocks;
        private int currentBlockIndex;
        private Iterator<StoredEntry> currentBlockIterator;
        private StoredEntry nextEntry;

        private ScanIterator(ScanBounds bounds) {
            this.bounds = bounds;
            this.blocks = bounds.isAll()
                ? index.entries()
                : index.findInRange(bounds);
            this.currentBlockIndex = 0;
            this.currentBlockIterator = null;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public StoredEntry next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            StoredEntry result = nextEntry;
            advance();
            return result;
        }

        private void advance() {
            while (true) {
                if (currentBlockIterator != null && currentBlockIterator.hasNext()) {
                    StoredEntry entry = currentBlockIterator.next();

                    if (!bounds.includes(entry.key())) {
                        Slice endExclusive = bounds.endExclusive();
                        if (endExclusive != null && entry.key().compareTo(endExclusive) >= 0) {
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

                BlockIndex.Entry blockEntry = blocks.get(currentBlockIndex);
                Block block = loadBlock(blockEntry.handle());
                currentBlockIterator = block.iterator();
                currentBlockIndex++;
            }
        }
    }
}
