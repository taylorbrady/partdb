package io.partdb.storage;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Map.entry;

final class TableReader {

    private static final Logger log = System.getLogger(TableReader.class.getName());
    private static final AtomicLong CACHE_IDS = new AtomicLong();

    private final Path path;
    private final TableMetadata metadata;
    private final Arena arena;
    private final MemorySegment segment;
    private final BloomFilter bloom;
    private final BlockIndex index;
    private final BlockCache cache;
    private final long cacheId = CACHE_IDS.incrementAndGet();
    private final AtomicInteger refs = new AtomicInteger(1);
    private volatile boolean obsolete;

    private TableReader(
        Path path,
        TableMetadata metadata,
        Arena arena,
        MemorySegment segment,
        BloomFilter bloom,
        BlockIndex index,
        BlockCache cache
    ) {
        this.path = path;
        this.metadata = metadata;
        this.arena = arena;
        this.segment = segment;
        this.bloom = bloom;
        this.index = index;
        this.cache = cache;
    }

    static TableReader open(Path path, long id, BlockCache cache) {
        Arena arena = Arena.ofShared();
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            return parse(path, id, cache, arena, segment);
        } catch (IOException e) {
            arena.close();
            throw new StoreException.IO("Failed to open table " + path, e);
        } catch (RuntimeException e) {
            arena.close();
            throw e instanceof StoreException ? e : new StoreException.Corruption("Malformed table " + path, e);
        }
    }

    private static TableReader parse(Path path, long id, BlockCache cache, Arena arena, MemorySegment segment) {
        long size = segment.byteSize();
        Encoding.check(size >= TableWriter.HEADER_SIZE + TableWriter.FOOTER_FIXED_SIZE, "Table too small: " + path);
        Encoding.check(segment.get(Encoding.INT, 0) == TableWriter.MAGIC, "Invalid table magic: " + path);
        int version = segment.get(Encoding.INT, Integer.BYTES);
        Encoding.check(version == TableWriter.VERSION, "Unsupported table version " + version + ": " + path);

        int footerSize = segment.get(Encoding.INT, size - 2L * Integer.BYTES);
        Encoding.check(
            footerSize >= TableWriter.FOOTER_FIXED_SIZE && footerSize <= size - TableWriter.HEADER_SIZE,
            "Invalid table footer size: " + path
        );
        long footerOffset = size - footerSize;
        MemorySegment footer = segment.asSlice(footerOffset, footerSize);
        Encoding.check(
            footer.get(Encoding.INT, footerSize - Integer.BYTES) == Encoding.crc(footer.asSlice(0, footerSize - Integer.BYTES)),
            "Table footer checksum mismatch: " + path
        );

        long bloomOffset = footer.get(Encoding.LONG, 0);
        int bloomSize = footer.get(Encoding.INT, 8);
        long indexOffset = footer.get(Encoding.LONG, 12);
        int indexSize = footer.get(Encoding.INT, 20);
        int blockCount = footer.get(Encoding.INT, 24);
        long tombstones = footer.get(Encoding.LONG, 36);
        int metaCrc = footer.get(Encoding.INT, 44);
        int smallestLength = footer.get(Encoding.INT, 48);
        Encoding.check(smallestLength >= 0 && 52L + smallestLength <= footerSize, "Invalid table smallest key: " + path);
        Slice smallest = Slice.wrap(footer.asSlice(52, smallestLength)).copy();
        int largestLength = footer.get(Encoding.INT, 52L + smallestLength);
        Encoding.check(
            largestLength >= 0 && TableWriter.FOOTER_FIXED_SIZE + smallestLength + (long) largestLength == footerSize,
            "Invalid table largest key: " + path
        );
        Slice largest = Slice.wrap(footer.asSlice(56L + smallestLength, largestLength)).copy();

        Encoding.check(
            bloomOffset == indexOffset - bloomSize && bloomOffset >= TableWriter.HEADER_SIZE && indexOffset + indexSize == footerOffset,
            "Invalid table layout: " + path
        );
        Encoding.check(metaCrc == Encoding.crc(segment.asSlice(bloomOffset, (long) bloomSize + indexSize)), "Table metadata checksum mismatch: " + path);

        BloomFilter bloom = BloomFilter.decode(segment.asSlice(bloomOffset, bloomSize));
        BlockIndex index = BlockIndex.decode(segment.asSlice(indexOffset, indexSize), blockCount);
        for (int i = 0; i < index.size(); i++) {
            BlockIndex.Handle handle = index.get(i);
            Encoding.check(handle.offset() + handle.size() <= bloomOffset, "Table block exceeds data region: " + path);
        }
        var metadata = new TableMetadata(id, smallest, largest, size, tombstones);
        return new TableReader(path, metadata, arena, segment, bloom, index, cache);
    }

    TableMetadata metadata() {
        return metadata;
    }

    long id() {
        return metadata.id();
    }

    Path path() {
        return path;
    }

    MemorySegment contents() {
        return segment;
    }

    Optional<InternalEntry> get(Slice key) {
        if (!metadata.contains(key) || !bloom.mightContain(key)) {
            return Optional.empty();
        }
        int position = index.find(key);
        return position < 0 ? Optional.empty() : block(position).get(key);
    }

    Iterator<InternalEntry> scan(Bounds bounds) {
        return new Scan(bounds);
    }

    TableReader retain() {
        if (refs.getAndIncrement() <= 0) {
            throw new IllegalStateException("Table " + path + " was already released");
        }
        return this;
    }

    void release() {
        int remaining = refs.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Table " + path + " released too many times");
        }
        if (remaining > 0) {
            return;
        }
        cache.invalidate(cacheId);
        arena.close();
        if (obsolete) {
            try {
                if (Files.deleteIfExists(path)) {
                    Disk.syncDirectory(path.getParent());
                }
            } catch (IOException | StoreException e) {
                log.log(Level.WARNING, "Failed to delete obsolete table", entry("table", path), e);
            }
        }
    }

    void markObsolete() {
        obsolete = true;
    }

    private Block block(int position) {
        BlockIndex.Handle handle = index.get(position);
        Block cached = cache.get(cacheId, handle.offset());
        if (cached != null) {
            return cached;
        }
        Block block = Block.from(MemorySegment.ofArray(BlockCodec.decode(segment.asSlice(handle.offset(), handle.size()))));
        cache.put(cacheId, handle.offset(), block);
        return block;
    }

    private final class Scan implements Iterator<InternalEntry> {
        private final Bounds bounds;
        private int position;
        private Iterator<InternalEntry> current;
        private InternalEntry next;

        private Scan(Bounds bounds) {
            this.bounds = bounds;
            if (bounds.start() != null) {
                position = Math.max(0, index.find(bounds.start()));
                if (position < index.size()) {
                    current = block(position++).seek(bounds.start());
                }
            }
            advance();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public InternalEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            InternalEntry result = next;
            advance();
            return result;
        }

        private void advance() {
            while (current == null || !current.hasNext()) {
                if (position >= index.size() || !bounds.beforeEnd(index.get(position).firstKey())) {
                    next = null;
                    return;
                }
                current = block(position++).iterator();
            }
            InternalEntry candidate = current.next();
            next = bounds.beforeEnd(candidate.userKey()) ? candidate : null;
        }
    }
}
