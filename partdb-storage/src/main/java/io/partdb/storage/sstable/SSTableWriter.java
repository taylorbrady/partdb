package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class SSTableWriter implements AutoCloseable {

    private static final int ESTIMATED_ENTRIES = 10000;

    private final Path path;
    private final SSTableConfig config;
    private final FileChannel channel;
    private final List<BlockIndex.IndexEntry> indexEntries;
    private final List<Entry> currentBlock;
    private final BloomFilter bloomFilter;

    private ByteArray lastKey;
    private int currentBlockSize;
    private long filePosition;
    private boolean closed;
    private long totalEntryCount;

    private SSTableWriter(Path path, SSTableConfig config, FileChannel channel) {
        this.path = path;
        this.config = config;
        this.channel = channel;
        this.indexEntries = new ArrayList<>();
        this.currentBlock = new ArrayList<>();
        this.bloomFilter = BloomFilter.create(ESTIMATED_ENTRIES, config.bloomFilterFalsePositiveRate());
        this.lastKey = null;
        this.currentBlockSize = 0;
        this.filePosition = SSTableHeader.HEADER_SIZE;
        this.closed = false;
        this.totalEntryCount = 0;
    }

    public static SSTableWriter create(Path path, SSTableConfig config) {
        try {
            FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);

            SSTableWriter writer = new SSTableWriter(path, config, channel);
            writer.writeHeader();
            return writer;
        } catch (IOException e) {
            throw new SSTableException("Failed to create SSTable: " + path, e);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE);
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

    public void append(Entry entry) {
        if (closed) {
            throw new SSTableException("Cannot append to closed writer");
        }

        if (lastKey != null && entry.key().compareTo(lastKey) <= 0) {
            throw new SSTableException(
                "Entries must be appended in strictly ascending order: " +
                "last=%s, current=%s".formatted(lastKey, entry.key())
            );
        }

        lastKey = entry.key();
        bloomFilter.add(entry.key());
        totalEntryCount++;
        int entrySize = estimateEntrySize(entry);

        if (currentBlockSize + entrySize > config.blockSize() && !currentBlock.isEmpty()) {
            flushCurrentBlock();
        }

        currentBlock.add(entry);
        currentBlockSize += entrySize;
    }

    private void flushCurrentBlock() {
        if (currentBlock.isEmpty()) {
            return;
        }

        ByteArray firstKey = currentBlock.getFirst().key();
        byte[] blockData = DataBlock.serialize(currentBlock);

        try {
            ByteBuffer buffer = ByteBuffer.wrap(blockData);
            long blockOffset = filePosition;

            channel.position(filePosition);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            filePosition += blockData.length;
            indexEntries.add(new BlockIndex.IndexEntry(firstKey, blockOffset, blockData.length));

            currentBlock.clear();
            currentBlockSize = 0;
        } catch (IOException e) {
            throw new SSTableException("Failed to write data block", e);
        }
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

            long bloomFilterOffset = filePosition;
            byte[] bloomFilterData = bloomFilter.serialize();

            ByteBuffer buffer = ByteBuffer.wrap(bloomFilterData);
            channel.position(filePosition);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            filePosition += bloomFilterData.length;

            long indexOffset = filePosition;
            BlockIndex index = new BlockIndex(indexEntries);
            byte[] indexData = index.serialize();

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
                lastKey,
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

    private int estimateEntrySize(Entry entry) {
        int size = 8 + 1 + 8 + 4 + entry.key().size() + 4;
        if (entry.value() != null) {
            size += entry.value().size();
        }
        return size;
    }

    public Path path() {
        return path;
    }
}
