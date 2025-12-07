package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.storage.Entry;

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

public final class SSTableReader implements AutoCloseable {

    private final Path path;
    private final Arena arena;
    private final MemorySegment segment;
    private final BloomFilter bloomFilter;
    private final BlockIndex index;
    private final SSTableFooter footer;

    private SSTableReader(
        Path path,
        Arena arena,
        MemorySegment segment,
        BloomFilter bloomFilter,
        BlockIndex index,
        SSTableFooter footer
    ) {
        this.path = path;
        this.arena = arena;
        this.segment = segment;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.footer = footer;
    }

    public static SSTableReader open(Path path) {
        Arena arena = Arena.ofShared();
        try {
            long fileSize;
            MemorySegment segment;

            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                fileSize = channel.size();
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            }

            validateHeader(segment);
            SSTableFooter footer = readFooter(segment, fileSize);
            BloomFilter bloomFilter = readBloomFilter(segment, footer);
            BlockIndex index = readIndex(segment, footer, fileSize);

            return new SSTableReader(path, arena, segment, bloomFilter, index, footer);
        } catch (IOException e) {
            arena.close();
            throw new SSTableException("Failed to open SSTable: " + path, e);
        }
    }

    private static void validateHeader(MemorySegment segment) {
        int magic = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int version = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        new SSTableHeader(magic, version);
    }

    private static SSTableFooter readFooter(MemorySegment segment, long fileSize) {
        int footerSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, fileSize - 8);
        long footerOffset = fileSize - footerSize;
        MemorySegment footerSegment = segment.asSlice(footerOffset, footerSize);
        return SSTableFooter.deserialize(footerSegment);
    }

    private static BloomFilter readBloomFilter(MemorySegment segment, SSTableFooter footer) {
        MemorySegment bloomSegment = segment.asSlice(footer.bloomFilterOffset(), footer.bloomFilterSize());
        return BloomFilter.deserialize(bloomSegment);
    }

    private static BlockIndex readIndex(MemorySegment segment, SSTableFooter footer, long fileSize) {
        long indexOffset = footer.indexOffset();
        int footerSize = SSTableFooter.calculateFooterSize(footer.largestKey());
        long indexSize = fileSize - footerSize - indexOffset;
        MemorySegment indexSegment = segment.asSlice(indexOffset, indexSize);
        return BlockIndex.deserialize(indexSegment, footer.blockCount());
    }

    public Optional<Entry> get(ByteArray key) {
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        Optional<BlockIndex.IndexEntry> blockEntry = index.findBlock(key);
        if (blockEntry.isEmpty()) {
            return Optional.empty();
        }

        List<Entry> entries = readBlock(blockEntry.get());
        for (Entry entry : entries) {
            if (entry.key().equals(key)) {
                return Optional.of(entry);
            }
        }

        return Optional.empty();
    }

    public CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        return new ScanIterator(startKey, endKey);
    }

    private List<Entry> readBlock(BlockIndex.IndexEntry blockEntry) {
        MemorySegment blockSegment = segment.asSlice(blockEntry.offset(), blockEntry.size());
        return DataBlock.deserialize(blockSegment);
    }

    public Path path() {
        return path;
    }

    public ByteArray largestKey() {
        return footer.largestKey();
    }

    public long entryCount() {
        return footer.entryCount();
    }

    public BlockIndex index() {
        return index;
    }

    @Override
    public void close() {
        arena.close();
    }

    private class ScanIterator implements CloseableIterator<Entry> {

        private final ByteArray startKey;
        private final ByteArray endKey;
        private final List<BlockIndex.IndexEntry> blocks;
        private int currentBlockIndex;
        private Iterator<Entry> currentBlockIterator;
        private Entry nextEntry;

        ScanIterator(ByteArray startKey, ByteArray endKey) {
            this.startKey = startKey;
            this.endKey = endKey;

            if (startKey == null && endKey == null) {
                this.blocks = index.entries();
            } else {
                this.blocks = index.findBlocksInRange(startKey, endKey);
            }

            this.currentBlockIndex = 0;
            this.currentBlockIterator = null;
            advance();
        }

        private void advance() {
            while (true) {
                if (currentBlockIterator != null && currentBlockIterator.hasNext()) {
                    Entry entry = currentBlockIterator.next();

                    boolean afterStart = startKey == null || entry.key().compareTo(startKey) >= 0;
                    boolean beforeEnd = endKey == null || entry.key().compareTo(endKey) < 0;

                    if (afterStart && beforeEnd) {
                        nextEntry = entry;
                        return;
                    } else if (!beforeEnd) {
                        nextEntry = null;
                        return;
                    }
                } else if (currentBlockIndex < blocks.size()) {
                    BlockIndex.IndexEntry blockEntry = blocks.get(currentBlockIndex);
                    List<Entry> entries = readBlock(blockEntry);
                    currentBlockIterator = entries.iterator();
                    currentBlockIndex++;
                } else {
                    nextEntry = null;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Entry next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            Entry result = nextEntry;
            advance();
            return result;
        }

        @Override
        public void close() {}
    }
}
