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
    static final int FOOTER_FIXED_SIZE = 8 + 4 + 8 + 4 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4;

    private static final int RESTART_INTERVAL = 16;
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.01;

    private final Path path;
    private final long id;
    private final StoreOptions options;
    private final FileChannel channel;
    private final OutputStream out;
    private final BlockWriter block = new BlockWriter(RESTART_INTERVAL);
    private final List<BlockIndex.Handle> handles = new ArrayList<>();
    private final List<Slice> keys = new ArrayList<>();
    private final CRC32C metaCrc = new CRC32C();

    private Slice smallest;
    private Slice largest;
    private long position;
    private long tombstones;
    private long rawBytes;
    private boolean finished;

    private TableWriter(Path path, long id, StoreOptions options, FileChannel channel) {
        this.path = path;
        this.id = id;
        this.options = options;
        this.channel = channel;
        this.out = new BufferedOutputStream(Channels.newOutputStream(channel), 1 << 16);
    }

    static TableWriter create(Path path, long id, StoreOptions options) {
        try {
            var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            var writer = new TableWriter(path, id, options, channel);
            writer.write(Encoding.allocate(HEADER_SIZE).putInt(MAGIC).putInt(VERSION).array());
            return writer;
        } catch (IOException e) {
            throw new StoreException.IO("failed to create table " + path, e);
        }
    }

    void add(InternalEntry entry) {
        if (largest != null && entry.userKey().compareTo(largest) <= 0) {
            throw new IllegalArgumentException("table keys must be strictly increasing");
        }
        if (!block.isEmpty() && block.estimatedSize() >= options.blockBytes()) {
            flushBlock();
        }
        block.add(entry);
        keys.add(entry.userKey());
        if (smallest == null) {
            smallest = entry.userKey();
        }
        largest = entry.userKey();
        if (entry instanceof InternalEntry.Tombstone) {
            tombstones++;
        }
        rawBytes += entry.encodedSize();
    }

    long id() {
        return id;
    }

    long rawBytes() {
        return rawBytes;
    }

    TableMetadata finish() {
        if (keys.isEmpty()) {
            throw new IllegalStateException("cannot finish an empty table");
        }
        if (!block.isEmpty()) {
            flushBlock();
        }
        long bloomOffset = position;
        byte[] bloom = BloomFilter.build(keys, BLOOM_FALSE_POSITIVE_RATE).encode();
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
        footer.putInt(handles.size()).putLong(keys.size()).putLong(tombstones);
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
            throw new StoreException.IO("failed to sync table " + path, e);
        }
        finished = true;
        return new TableMetadata(id, smallest.copy(), largest.copy(), position, tombstones);
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
            throw new StoreException.IO("failed to discard table " + path, e);
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
            throw new StoreException.IO("failed to write table " + path, e);
        }
    }
}
