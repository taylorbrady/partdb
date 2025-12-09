package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;
import io.partdb.storage.VersionedKey;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class SSTable implements AutoCloseable {

    private final Path path;
    private final Arena arena;
    private final MemorySegment segment;
    private final BloomFilter bloomFilter;
    private final BlockIndex index;
    private final SSTableFooter footer;
    private final BlockCache cache;

    private SSTable(
        Path path,
        Arena arena,
        MemorySegment segment,
        BloomFilter bloomFilter,
        BlockIndex index,
        SSTableFooter footer,
        BlockCache cache
    ) {
        this.path = path;
        this.arena = arena;
        this.segment = segment;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.footer = footer;
        this.cache = cache;
    }

    public static SSTable open(Path path) {
        return open(path, null);
    }

    public static SSTable open(Path path, BlockCache cache) {
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

            return new SSTable(path, arena, segment, bloomFilter, index, footer, cache);
        } catch (IOException e) {
            arena.close();
            throw new SSTableException("Failed to open SSTable: " + path, e);
        }
    }

    public Optional<Entry> get(ByteArray key, Timestamp readTimestamp) {
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        Optional<BlockIndex.Entry> blockEntry = index.find(key);
        if (blockEntry.isEmpty()) {
            return Optional.empty();
        }

        Block block = loadBlock(blockEntry.get().handle());
        return block.find(key, readTimestamp);
    }

    public Scan scan() {
        return new Scan();
    }

    public Path path() {
        return path;
    }

    public ByteArray smallestKey() {
        return footer.smallestKey();
    }

    public ByteArray largestKey() {
        return footer.largestKey();
    }

    public long entryCount() {
        return footer.entryCount();
    }

    public Timestamp smallestTimestamp() {
        return footer.smallestTimestamp();
    }

    public Timestamp largestTimestamp() {
        return footer.largestTimestamp();
    }

    @Override
    public void close() {
        if (cache != null) {
            cache.invalidate(path);
        }
        arena.close();
    }

    private Block loadBlock(BlockHandle handle) {
        if (cache != null) {
            Optional<Block> cached = cache.get(path, handle);
            if (cached.isPresent()) {
                return cached.get();
            }
        }

        MemorySegment blockSegment = segment.asSlice(handle.offset(), handle.size());
        Block block = Block.from(blockSegment);

        if (cache != null) {
            cache.put(path, handle, block);
        }

        return block;
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
        return BloomFilter.from(bloomSegment);
    }

    private static BlockIndex readIndex(MemorySegment segment, SSTableFooter footer, long fileSize) {
        long indexOffset = footer.indexOffset();
        int footerSize = SSTableFooter.calculateFooterSize(footer.smallestKey(), footer.largestKey());
        long indexSize = fileSize - footerSize - indexOffset;
        MemorySegment indexSegment = segment.asSlice(indexOffset, indexSize);
        return BlockIndex.deserialize(indexSegment, footer.blockCount());
    }

    public final class Scan implements Iterable<Entry> {

        private ByteArray startKey;
        private ByteArray endKey;
        private ScanMode mode = new ScanMode.AllVersions();

        public Scan from(ByteArray startKey) {
            this.startKey = startKey;
            return this;
        }

        public Scan until(ByteArray endKey) {
            this.endKey = endKey;
            return this;
        }

        public Scan asOf(Timestamp timestamp) {
            this.mode = new ScanMode.Snapshot(timestamp);
            return this;
        }

        public Scan allVersions() {
            this.mode = new ScanMode.AllVersions();
            return this;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new ScanIterator(mode, startKey, endKey);
        }

        public Stream<Entry> stream() {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    private class ScanIterator implements Iterator<Entry> {

        private final ByteArray startKey;
        private final ByteArray endKey;
        private final ScanMode mode;
        private final List<BlockIndex.Entry> blocks;
        private int currentBlockIndex;
        private Iterator<Entry> currentBlockIterator;
        private ByteArray lastKey;
        private Entry nextEntry;

        ScanIterator(ScanMode mode, ByteArray startKey, ByteArray endKey) {
            this.mode = mode;
            this.startKey = startKey;
            this.endKey = endKey;

            if (startKey == null && endKey == null) {
                this.blocks = index.entries();
            } else {
                this.blocks = index.findInRange(startKey, endKey);
            }

            this.currentBlockIndex = 0;
            this.currentBlockIterator = null;
            this.lastKey = null;
            advance();
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

        private void advance() {
            while (true) {
                if (currentBlockIterator != null && currentBlockIterator.hasNext()) {
                    Entry entry = currentBlockIterator.next();

                    boolean afterStart = startKey == null || entry.key().compareTo(startKey) >= 0;
                    boolean beforeEnd = endKey == null || entry.key().compareTo(endKey) < 0;

                    if (!afterStart) {
                        continue;
                    }
                    if (!beforeEnd) {
                        nextEntry = null;
                        return;
                    }

                    switch (mode) {
                        case ScanMode.Snapshot(var readTimestamp) -> {
                            if (entry.timestamp().compareTo(readTimestamp) > 0) {
                                continue;
                            }
                            if (lastKey != null && entry.key().equals(lastKey)) {
                                continue;
                            }
                            lastKey = entry.key();
                        }
                        case ScanMode.AllVersions() -> {}
                    }

                    nextEntry = entry;
                    return;
                } else if (currentBlockIndex < blocks.size()) {
                    BlockIndex.Entry blockEntry = blocks.get(currentBlockIndex);
                    Block block = loadBlock(blockEntry.handle());
                    currentBlockIterator = block.iterator();
                    currentBlockIndex++;
                } else {
                    nextEntry = null;
                    return;
                }
            }
        }
    }

    public static final class Writer implements AutoCloseable {

        private final Path path;
        private final SSTableConfig config;
        private final FileChannel channel;
        private final List<BlockIndex.Entry> indexEntries;
        private final Block.Builder currentBlock;
        private final List<ByteArray> keys;

        private VersionedKey lastVersionedKey;
        private ByteArray firstKey;
        private ByteArray lastKey;
        private Timestamp smallestTimestamp;
        private Timestamp largestTimestamp;
        private long filePosition;
        private boolean closed;
        private long totalEntryCount;

        private Writer(Path path, SSTableConfig config, FileChannel channel) {
            this.path = path;
            this.config = config;
            this.channel = channel;
            this.indexEntries = new ArrayList<>();
            this.currentBlock = new Block.Builder();
            this.keys = new ArrayList<>();
            this.lastVersionedKey = null;
            this.firstKey = null;
            this.lastKey = null;
            this.smallestTimestamp = null;
            this.largestTimestamp = null;
            this.filePosition = SSTableHeader.HEADER_SIZE;
            this.closed = false;
            this.totalEntryCount = 0;
        }

        public static Writer create(Path path, SSTableConfig config) {
            try {
                FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);

                Writer writer = new Writer(path, config, channel);
                writer.writeHeader();
                return writer;
            } catch (IOException e) {
                throw new SSTableException("Failed to create SSTable: " + path, e);
            }
        }

        public static Writer create(Path path) {
            return create(path, SSTableConfig.defaults());
        }

        public void add(Entry entry) {
            if (closed) {
                throw new SSTableException("Cannot add to closed writer");
            }

            VersionedKey currentVersionedKey = new VersionedKey(entry.key(), entry.timestamp());

            if (lastVersionedKey != null && currentVersionedKey.compareTo(lastVersionedKey) <= 0) {
                throw new SSTableException(
                    "Entries must be added in strictly ascending order: " +
                    "last=(%s, %s), current=(%s, %s)".formatted(
                        lastVersionedKey.key(), lastVersionedKey.timestamp(),
                        entry.key(), entry.timestamp())
                );
            }

            lastVersionedKey = currentVersionedKey;
            if (firstKey == null) {
                firstKey = entry.key();
            }
            lastKey = entry.key();

            if (smallestTimestamp == null || entry.timestamp().compareTo(smallestTimestamp) < 0) {
                smallestTimestamp = entry.timestamp();
            }
            if (largestTimestamp == null || entry.timestamp().compareTo(largestTimestamp) > 0) {
                largestTimestamp = entry.timestamp();
            }

            keys.add(entry.key());
            totalEntryCount++;

            if (currentBlock.estimatedSize() > config.blockSize() && !currentBlock.isEmpty()) {
                flushCurrentBlock();
            }

            currentBlock.add(entry);
        }

        public Path path() {
            return path;
        }

        public Timestamp smallestTimestamp() {
            return smallestTimestamp;
        }

        public Timestamp largestTimestamp() {
            return largestTimestamp;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }

            try {
                if (!currentBlock.isEmpty()) {
                    flushCurrentBlock();
                }

                BloomFilter bloomFilter = BloomFilter.build(keys, config.bloomFilterFalsePositiveRate());
                long bloomFilterOffset = filePosition;
                byte[] bloomFilterData = bloomFilter.serialize();

                ByteBuffer buffer = ByteBuffer.wrap(bloomFilterData);
                channel.position(filePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                filePosition += bloomFilterData.length;

                long indexOffset = filePosition;
                BlockIndex blockIndex = new BlockIndex(indexEntries);
                byte[] indexData = blockIndex.serialize();

                buffer = ByteBuffer.wrap(indexData);
                channel.position(filePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                filePosition += indexData.length;

                SSTableFooter footer = new SSTableFooter(
                    bloomFilterOffset,
                    bloomFilterData.length,
                    indexOffset,
                    indexEntries.size(),
                    firstKey != null ? firstKey : ByteArray.empty(),
                    lastKey != null ? lastKey : ByteArray.empty(),
                    smallestTimestamp != null ? smallestTimestamp : Timestamp.ZERO,
                    largestTimestamp != null ? largestTimestamp : Timestamp.ZERO,
                    totalEntryCount,
                    0
                );
                byte[] footerData = footer.serialize();

                buffer = ByteBuffer.wrap(footerData);
                channel.position(filePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }

                channel.force(true);
                closed = true;
            } catch (IOException e) {
                throw new SSTableException("Failed to close SSTable writer", e);
            } finally {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new SSTableException("Failed to close file channel", e);
                }
            }
        }

        private void writeHeader() {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.nativeOrder());
                buffer.putInt(SSTableHeader.MAGIC_NUMBER);
                buffer.putInt(SSTableHeader.CURRENT_VERSION);
                buffer.putInt(0);
                buffer.flip();

                channel.position(0);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            } catch (IOException e) {
                throw new SSTableException("Failed to write header", e);
            }
        }

        private void flushCurrentBlock() {
            if (currentBlock.isEmpty()) {
                return;
            }

            ByteArray blockFirstKey = currentBlock.firstKey();
            byte[] blockData = currentBlock.build();

            try {
                ByteBuffer buffer = ByteBuffer.wrap(blockData);
                long blockOffset = filePosition;

                channel.position(filePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }

                filePosition += blockData.length;
                indexEntries.add(new BlockIndex.Entry(blockFirstKey, new BlockHandle(blockOffset, blockData.length)));

                currentBlock.reset();
            } catch (IOException e) {
                throw new SSTableException("Failed to write data block", e);
            }
        }
    }
}
