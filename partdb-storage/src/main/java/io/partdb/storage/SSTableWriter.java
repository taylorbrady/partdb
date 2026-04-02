package io.partdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

final class SSTableWriter implements AutoCloseable {

    private final long id;
    private final int level;
    private final Path path;
    private final LsmConfig config;
    private final FileChannel channel;
    private final List<DataBlockIndex.Entry> indexEntries;
    private final DataBlockWriter currentBlock;
    private final List<Slice> keys;

    private InternalKey lastInternalKey;
    private Slice lastKey;
    private Slice firstKey;
    private long smallestRevision;
    private long largestRevision;
    private long filePosition;
    private long totalEntryCount;
    private long uncompressedBytes;
    private boolean hasRevision;
    private boolean finished;

    private SSTableWriter(long id, int level, Path path, LsmConfig config, FileChannel channel) {
        this.id = id;
        this.level = level;
        this.path = path;
        this.config = config;
        this.channel = channel;
        this.indexEntries = new ArrayList<>();
        this.currentBlock = new DataBlockWriter(config.blockRestartInterval());
        this.keys = new ArrayList<>();
        this.lastInternalKey = null;
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

    static SSTableWriter create(long id, int level, Path path, LsmConfig config) {
        try {
            FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
            );

            SSTableWriter writer = new SSTableWriter(id, level, path, config, channel);
            writer.writeHeader();
            return writer;
        } catch (IOException e) {
            throw new StorageException.IO("Failed to create SSTable: " + path, e);
        }
    }

    long id() {
        return id;
    }

    int level() {
        return level;
    }

    void add(InternalEntry entry) {
        if (finished) {
            throw new IllegalStateException("Cannot add to finished writer");
        }

        if (lastInternalKey != null && entry.key().compareTo(lastInternalKey) <= 0) {
            throw new IllegalArgumentException(
                "Entries must be added in strictly ascending internal-key order: last=%s, current=%s"
                    .formatted(lastInternalKey, entry.key())
            );
        }

        lastInternalKey = entry.key();
        lastKey = entry.userKey();
        if (firstKey == null) {
            firstKey = entry.userKey();
        }

        if (!hasRevision || entry.revision() < smallestRevision) {
            smallestRevision = entry.revision();
        }
        if (!hasRevision || entry.revision() > largestRevision) {
            largestRevision = entry.revision();
        }
        hasRevision = true;

        keys.add(entry.userKey());
        totalEntryCount++;
        uncompressedBytes += entry.encodedSizeBytes();

        if (currentBlock.estimatedSizeBytes() > config.blockSize()
            && !currentBlock.isEmpty()
            && !currentBlock.lastKey().equals(entry.userKey())) {
            flushCurrentBlock();
        }

        currentBlock.append(entry);
    }

    void add(StoredEntry entry) {
        add(InternalEntry.from(entry));
    }

    long uncompressedBytes() {
        return uncompressedBytes;
    }

    SSTableMetadata finish() {
        if (finished) {
            throw new IllegalStateException("Writer already finished");
        }

        finishWriting();

        return new SSTableMetadata(
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
            DataBlockIndex blockIndex = new DataBlockIndex(indexEntries);
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
            throw new StorageException.IO("Failed to finish SSTable writer", e);
        }
    }

    private void writeHeader() {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.nativeOrder());
            buffer.putInt(SSTableHeader.MAGIC_NUMBER);
            buffer.putInt(SSTableHeader.CURRENT_VERSION);
            buffer.put(config.blockCodec().id());
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
        byte[] uncompressedData = currentBlock.finish();

        byte[] compressedData = config.blockCodec().compress(uncompressedData);
        CompressedBlock compressedBlock = new CompressedBlock(
            config.blockCodec().id(),
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
            indexEntries.add(new DataBlockIndex.Entry(blockFirstKey, new BlockHandle(blockOffset, blockData.length)));

            currentBlock.reset();
        } catch (IOException e) {
            throw new StorageException.IO("Failed to write data block", e);
        }
    }
}
