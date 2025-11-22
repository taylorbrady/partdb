package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class SSTableReader implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mappedFile;
    private final BloomFilter bloomFilter;
    private final BlockIndex index;
    private final SSTableFooter footer;

    private SSTableReader(Path path, FileChannel channel, MappedByteBuffer mappedFile, BloomFilter bloomFilter, BlockIndex index, SSTableFooter footer) {
        this.path = path;
        this.channel = channel;
        this.mappedFile = mappedFile;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.footer = footer;
    }

    public static SSTableReader open(Path path) {
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            long fileSize = channel.size();

            MappedByteBuffer mappedFile = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            validateHeader(mappedFile);

            SSTableFooter footer = readFooter(mappedFile, fileSize);
            footer.validate();

            BloomFilter bloomFilter = readBloomFilter(mappedFile, footer);
            BlockIndex index = readIndex(mappedFile, footer, fileSize);

            return new SSTableReader(path, channel, mappedFile, bloomFilter, index, footer);
        } catch (IOException e) {
            throw new SSTableException("Failed to open SSTable: " + path, e);
        }
    }

    private static void validateHeader(MappedByteBuffer buffer) {
        buffer.position(0);
        int magic = buffer.getInt();
        int version = buffer.getInt();
        new SSTableHeader(magic, version);
    }

    private static SSTableFooter readFooter(MappedByteBuffer buffer, long fileSize) {
        int footerSizePosition = (int) (fileSize - 8);
        buffer.position(footerSizePosition);
        int footerSize = buffer.getInt();

        int footerOffset = (int) (fileSize - footerSize);
        buffer.position(footerOffset);

        ByteBuffer footerBuffer = buffer.slice(footerOffset, footerSize);
        return SSTableFooter.deserialize(footerBuffer);
    }

    private static BloomFilter readBloomFilter(MappedByteBuffer buffer, SSTableFooter footer) {
        int bloomFilterOffset = (int) footer.bloomFilterOffset();
        int bloomFilterSize = footer.bloomFilterSize();

        buffer.position(bloomFilterOffset);
        ByteBuffer bloomFilterBuffer = buffer.slice(bloomFilterOffset, bloomFilterSize);

        return BloomFilter.deserialize(bloomFilterBuffer);
    }

    private static BlockIndex readIndex(MappedByteBuffer buffer, SSTableFooter footer, long fileSize) {
        int indexOffset = (int) footer.indexOffset();
        int footerSize = SSTableFooter.calculateFooterSize(footer.largestKey());
        int indexSize = (int) (fileSize - footerSize - indexOffset);

        buffer.position(indexOffset);
        ByteBuffer indexBuffer = buffer.slice(indexOffset, indexSize);

        return BlockIndex.deserialize(indexBuffer, footer.blockCount());
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

    public Iterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        return new ScanIterator(startKey, endKey);
    }

    private List<Entry> readBlock(BlockIndex.IndexEntry blockEntry) {
        int offset = (int) blockEntry.offset();
        int size = blockEntry.size();

        ByteBuffer blockBuffer = mappedFile.slice(offset, size);
        return DataBlock.deserialize(blockBuffer);
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
        try {
            channel.close();
        } catch (IOException e) {
            throw new SSTableException("Failed to close SSTable reader", e);
        }
    }

    private class ScanIterator implements Iterator<Entry> {

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
    }
}
