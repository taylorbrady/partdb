package io.partdb.storage;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

final class TableWriter implements AutoCloseable {

    static final int MAGIC = 0x54424450;
    static final int VERSION = 1;
    static final int HEADER_SIZE = 2 * Integer.BYTES;
    static final int FOOTER_FIXED_SIZE = 8 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + 4;

    private final Path path;
    private final long id;
    private final int level;
    private final StoreOptions.Table options;
    private final FileChannel channel;
    private final OutputStream out;
    private final BlockWriter block;
    private final List<BlockIndex.Handle> handles = new ArrayList<>();
    private final List<Slice> keys = new ArrayList<>();
    private final CRC32C metaCrc = new CRC32C();

    private Slice smallest;
    private Slice largest;
    private long position;
    private long entryCount;
    private long rawBytes;
    private boolean finished;

    private TableWriter(Path path, long id, int level, StoreOptions.Table options, FileChannel channel) {
        this.path = path;
        this.id = id;
        this.level = level;
        this.options = options;
        this.channel = channel;
        this.out = new BufferedOutputStream(Channels.newOutputStream(channel), 1 << 16);
        this.block = new BlockWriter(options.blockRestartInterval());
    }

    static TableWriter create(Path path, long id, int level, StoreOptions.Table options) {
        try {
            var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            var writer = new TableWriter(path, id, level, options, channel);
            writer.write(Encoding.allocate(HEADER_SIZE).putInt(MAGIC).putInt(VERSION).array());
            return writer;
        } catch (IOException e) {
            throw new StoreException.IO("Failed to create table " + path, e);
        }
    }

    void add(InternalEntry entry) {
        if (!block.isEmpty() && block.estimatedSize() >= options.blockSizeBytes() && !block.lastKey().equals(entry.userKey())) {
            flushBlock();
        }
        block.add(entry);
        if (largest == null || !largest.equals(entry.userKey())) {
            keys.add(entry.userKey());
        }
        if (smallest == null) {
            smallest = entry.userKey();
        }
        largest = entry.userKey();
        entryCount++;
        rawBytes += entry.encodedSize();
    }

    long id() {
        return id;
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    long rawBytes() {
        return rawBytes;
    }

    TableMetadata finish() {
        if (entryCount == 0) {
            throw new IllegalStateException("Cannot finish an empty table");
        }
        if (!block.isEmpty()) {
            flushBlock();
        }
        long bloomOffset = position;
        byte[] bloom = BloomFilter.build(keys, options.bloomFalsePositiveRate()).encode();
        writeMeta(bloom);
        long indexOffset = position;
        byte[] index = new BlockIndex(handles).encode();
        writeMeta(index);

        byte[] low = smallest.toByteArray();
        byte[] high = largest.toByteArray();
        int footerSize = FOOTER_FIXED_SIZE + low.length + high.length;
        var footer = Encoding.allocate(footerSize);
        footer.putLong(bloomOffset).putInt(bloom.length);
        footer.putLong(indexOffset).putInt(index.length);
        footer.putInt(handles.size()).putLong(entryCount);
        footer.putInt((int) metaCrc.getValue());
        footer.putInt(low.length).put(low);
        footer.putInt(high.length).put(high);
        footer.putInt(footerSize);
        footer.putInt(Encoding.crc(footer.array(), 0, footer.position()));
        write(footer.array());

        try {
            out.flush();
            channel.force(true);
            channel.close();
        } catch (IOException e) {
            throw new StoreException.IO("Failed to sync table " + path, e);
        }
        finished = true;
        return new TableMetadata(id, level, smallest.copy(), largest.copy(), position);
    }

    @Override
    public void close() {
        if (finished) {
            return;
        }
        try {
            channel.close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to discard table " + path, e);
        }
    }

    private void flushBlock() {
        Slice firstKey = block.firstKey();
        byte[] frame = BlockCodec.encode(options.compression(), block.finish());
        handles.add(new BlockIndex.Handle(firstKey, position, frame.length));
        write(frame);
    }

    private void writeMeta(byte[] bytes) {
        metaCrc.update(bytes);
        write(bytes);
    }

    private void write(byte[] bytes) {
        try {
            out.write(bytes);
            position += bytes.length;
        } catch (IOException e) {
            throw new StoreException.IO("Failed to write table " + path, e);
        }
    }
}
