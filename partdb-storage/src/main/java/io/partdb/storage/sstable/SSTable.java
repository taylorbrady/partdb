package io.partdb.storage.sstable;

import io.partdb.common.Slice;
import io.partdb.storage.Mutation;
import io.partdb.storage.StorageException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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

    private SSTable(
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

    static SSTable open(long id, int level, Path path, BlockCache cache) {
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

            return new SSTable(id, level, path, arena, segment, bloomFilter, index, footer, cache, codec, fileSize);
        } catch (IOException e) {
            arena.close();
            throw new StorageException.IO("Failed to open SSTable: " + path, e);
        }
    }

    static Builder builder(long id, int level, Path path, SSTableConfig config) {
        try {
            FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

            Builder builder = new Builder(id, level, path, config, channel);
            builder.writeHeader();
            return builder;
        } catch (IOException e) {
            throw new StorageException.IO("Failed to create SSTable: " + path, e);
        }
    }

    public long id() {
        return id;
    }

    public int level() {
        return level;
    }

    public Optional<Mutation> get(Slice key) {
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

    public Scan scan() {
        return new Scan();
    }

    public Path path() {
        return path;
    }

    public Slice smallestKey() {
        return footer.smallestKey();
    }

    public Slice largestKey() {
        return footer.largestKey();
    }

    public long entryCount() {
        return footer.entryCount();
    }

    public long smallestRevision() {
        return footer.smallestRevision();
    }

    public long largestRevision() {
        return footer.largestRevision();
    }

    public long fileSizeBytes() {
        return fileSizeBytes;
    }

    public SSTableDescriptor descriptor() {
        return new SSTableDescriptor(
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

    public boolean mightContain(Slice key) {
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
        int magic = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int version = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        byte codecId = segment.get(ValueLayout.JAVA_BYTE, 8);
        return new SSTableHeader(magic, version, codecId);
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

    public final class Scan implements Iterable<Mutation> {

        private Slice startKey;
        private Slice endKey;

        public Scan from(Slice startKey) {
            this.startKey = startKey;
            return this;
        }

        public Scan until(Slice endKey) {
            this.endKey = endKey;
            return this;
        }

        @Override
        public Iterator<Mutation> iterator() {
            return new ScanIterator(startKey, endKey);
        }

        public Stream<Mutation> stream() {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    private class ScanIterator implements Iterator<Mutation> {

        private final Slice startKey;
        private final Slice endKey;
        private final List<BlockIndex.Entry> blocks;
        private int currentBlockIndex;
        private Iterator<Mutation> currentBlockIterator;
        private Mutation nextMutation;

        ScanIterator(Slice startKey, Slice endKey) {
            this.startKey = startKey;
            this.endKey = endKey;

            if (startKey == null && endKey == null) {
                this.blocks = index.entries();
            } else {
                this.blocks = index.findInRange(startKey, endKey);
            }

            this.currentBlockIndex = 0;
            this.currentBlockIterator = null;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextMutation != null;
        }

        @Override
        public Mutation next() {
            if (nextMutation == null) {
                throw new NoSuchElementException();
            }
            Mutation result = nextMutation;
            advance();
            return result;
        }

        private void advance() {
            while (true) {
                if (currentBlockIterator != null && currentBlockIterator.hasNext()) {
                    Mutation mutation = currentBlockIterator.next();

                    boolean afterStart = startKey == null || mutation.key().compareTo(startKey) >= 0;
                    boolean beforeEnd = endKey == null || mutation.key().compareTo(endKey) < 0;

                    if (!afterStart) {
                        continue;
                    }
                    if (!beforeEnd) {
                        nextMutation = null;
                        return;
                    }

                    nextMutation = mutation;
                    return;
                } else if (currentBlockIndex < blocks.size()) {
                    BlockIndex.Entry blockEntry = blocks.get(currentBlockIndex);
                    Block block = loadBlock(blockEntry.handle());
                    currentBlockIterator = block.iterator();
                    currentBlockIndex++;
                } else {
                    nextMutation = null;
                    return;
                }
            }
        }
    }

    public static final class Builder implements AutoCloseable {

        private final long id;
        private final int level;
        private final Path path;
        private final SSTableConfig config;
        private final FileChannel channel;
        private final List<BlockIndex.Entry> indexEntries;
        private final Block.Builder currentBlock;
        private final List<Slice> keys;

        private Slice lastKey;
        private Slice firstKey;
        private long smallestRevision;
        private long largestRevision;
        private long filePosition;
        private long totalEntryCount;
        private long uncompressedBytes;
        private boolean hasRevision;
        private boolean finished;

        private Builder(long id, int level, Path path, SSTableConfig config, FileChannel channel) {
            this.id = id;
            this.level = level;
            this.path = path;
            this.config = config;
            this.channel = channel;
            this.indexEntries = new ArrayList<>();
            this.currentBlock = new Block.Builder();
            this.keys = new ArrayList<>();
            this.lastKey = null;
            this.firstKey = null;
            this.smallestRevision = 0;
            this.largestRevision = 0;
            this.filePosition = SSTableHeader.HEADER_SIZE;
            this.totalEntryCount = 0;
            this.uncompressedBytes = 0;
            this.hasRevision = false;
            this.finished = false;
        }

        public long id() {
            return id;
        }

        public int level() {
            return level;
        }

        public void add(Mutation mutation) {
            if (finished) {
                throw new IllegalStateException("Cannot add to finished builder");
            }

            if (lastKey != null && mutation.key().compareTo(lastKey) <= 0) {
                throw new IllegalArgumentException(
                    "Entries must be added in strictly ascending key order: last=%s, current=%s"
                        .formatted(lastKey, mutation.key())
                );
            }

            lastKey = mutation.key();
            if (firstKey == null) {
                firstKey = mutation.key();
            }

            if (!hasRevision || mutation.revision() < smallestRevision) {
                smallestRevision = mutation.revision();
            }
            if (!hasRevision || mutation.revision() > largestRevision) {
                largestRevision = mutation.revision();
            }
            hasRevision = true;

            keys.add(mutation.key());
            totalEntryCount++;
            uncompressedBytes += entrySize(mutation);

            if (currentBlock.estimatedSize() > config.blockSize() && !currentBlock.isEmpty()) {
                flushCurrentBlock();
            }

            currentBlock.add(mutation);
        }

        public long uncompressedBytes() {
            return uncompressedBytes;
        }

        public SSTableDescriptor finish() {
            if (finished) {
                throw new IllegalStateException("Builder already finished");
            }

            finishWriting();

            return new SSTableDescriptor(
                id,
                level,
                firstKey != null ? firstKey : Slice.empty(),
                lastKey != null ? lastKey : Slice.empty(),
                smallestRevision,
                largestRevision,
                filePosition,
                totalEntryCount
            );
        }

        private void finishWriting() {
            if (finished) {
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
                    firstKey != null ? firstKey : Slice.empty(),
                    lastKey != null ? lastKey : Slice.empty(),
                    smallestRevision,
                    largestRevision,
                    totalEntryCount,
                    0
                );
                byte[] footerData = footer.serialize();

                buffer = ByteBuffer.wrap(footerData);
                channel.position(filePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                filePosition += footerData.length;

                channel.force(true);
                finished = true;
            } catch (IOException e) {
                throw new StorageException.IO("Failed to finish SSTable builder", e);
            }
        }

        @Override
        public void close() {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
                if (!finished) {
                    Files.deleteIfExists(path);
                }
            } catch (IOException e) {
                throw new StorageException.IO("Failed to close file channel", e);
            }
        }

        private static int entrySize(Mutation mutation) {
            int base = 1 + 8 + 4 + mutation.key().length() + 4;
            return switch (mutation) {
                case Mutation.Put put -> base + put.value().length();
                case Mutation.Tombstone _ -> base;
            };
        }

        private void writeHeader() {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.nativeOrder());
                buffer.putInt(SSTableHeader.MAGIC_NUMBER);
                buffer.putInt(SSTableHeader.CURRENT_VERSION);
                buffer.put(config.codec().id());
                buffer.putInt(0);
                buffer.flip();

                channel.position(0);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            } catch (IOException e) {
                throw new StorageException.IO("Failed to write header", e);
            }
        }

        private void flushCurrentBlock() {
            if (currentBlock.isEmpty()) {
                return;
            }

            Slice blockFirstKey = currentBlock.firstKey();
            byte[] uncompressedData = currentBlock.build();

            byte[] compressedData = config.codec().compress(uncompressedData);
            CompressedBlock compressedBlock = new CompressedBlock(
                config.codec().id(),
                uncompressedData.length,
                compressedData
            );
            byte[] blockData = compressedBlock.serialize();

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
                throw new StorageException.IO("Failed to write data block", e);
            }
        }
    }
}
