package io.partdb.storage;

import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class StoreDirectory implements AutoCloseable {

    private static final Pattern TABLE = Pattern.compile("(\\d+)\\.sst");

    private final Path root;
    private final FileLock lock;
    private final AtomicLong nextTableId;

    record Opened(StoreDirectory directory, Manifest manifest) {}

    private StoreDirectory(Path root, FileLock lock, long nextTableId) {
        this.root = root;
        this.lock = lock;
        this.nextTableId = new AtomicLong(nextTableId);
    }

    static Opened open(Path root) {
        Disk.createPrivateDirectory(root);
        FileLock lock = Disk.lock(root);
        try {
            return open(root, lock);
        } catch (RuntimeException e) {
            Disk.unlock(lock);
            throw e;
        }
    }

    private static Opened open(Path root, FileLock lock) {
        boolean changed = false;
        Set<Long> present = new HashSet<>();
        long maxId = 0;
        for (Path path : Disk.list(root)) {
            String name = path.getFileName().toString();
            Matcher matcher = TABLE.matcher(name);
            if (name.endsWith(Disk.TEMPORARY_SUFFIX)) {
                Disk.delete(path);
                changed = true;
            } else if (matcher.matches()) {
                long id = Long.parseLong(matcher.group(1));
                present.add(id);
                maxId = Math.max(maxId, id);
            }
        }

        Manifest manifest;
        if (Files.exists(root.resolve(Manifest.FILE_NAME))) {
            manifest = Manifest.read(root);
        } else {
            Encoding.check(present.isEmpty(), "tables without a manifest in " + root);
            manifest = new Manifest(0, 1, List.of());
            manifest.write(root);
        }

        Set<Long> referenced = new HashSet<>();
        manifest.tables().forEach(table -> referenced.add(table.id()));
        StoreDirectory directory = new StoreDirectory(root, lock, Math.max(manifest.nextTableId(), maxId + 1));
        for (long id : present) {
            if (!referenced.contains(id)) {
                Disk.delete(directory.tablePath(id));
                changed = true;
            }
        }
        if (changed) {
            directory.sync();
        }
        referenced.removeAll(present);
        Encoding.check(referenced.isEmpty(), "manifest references missing tables " + referenced + " in " + root);
        return new Opened(directory, manifest);
    }

    Path tablePath(long id) {
        return root.resolve("%06d.sst".formatted(id));
    }

    Path temporaryPath(String prefix) {
        return root.resolve(prefix + "-" + UUID.randomUUID() + Disk.TEMPORARY_SUFFIX);
    }

    long allocateTableId() {
        return nextTableId.getAndIncrement();
    }

    void commit(long durableRevision, Version version) {
        new Manifest(durableRevision, nextTableId.get(), version.placements()).write(root);
    }

    void sync() {
        Disk.syncDirectory(root);
    }

    @Override
    public void close() {
        Disk.unlock(lock);
    }
}
