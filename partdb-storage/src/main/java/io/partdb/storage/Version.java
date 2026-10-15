package io.partdb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

final class Version {

    private static final Comparator<TableReader> BY_SMALLEST = Comparator.comparing(table -> table.metadata().smallest());

    private final TableReader[][] levels;
    private final AtomicInteger refs = new AtomicInteger(1);

    private Version(TableReader[][] levels) {
        this.levels = levels;
        for (TableReader[] level : levels) {
            for (TableReader table : level) {
                table.retain();
            }
        }
    }

    static Version of(List<TableReader> tables, int levelCount) {
        int count = levelCount;
        for (TableReader table : tables) {
            count = Math.max(count, table.metadata().level() + 1);
        }
        List<List<TableReader>> grouped = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            grouped.add(new ArrayList<>());
        }
        for (TableReader table : tables) {
            grouped.get(table.metadata().level()).add(table);
        }
        TableReader[][] levels = new TableReader[count][];
        for (int i = 0; i < count; i++) {
            List<TableReader> level = grouped.get(i);
            if (i > 0) {
                level.sort(BY_SMALLEST);
            }
            levels[i] = level.toArray(TableReader[]::new);
        }
        return new Version(levels);
    }

    Version retain() {
        if (refs.getAndIncrement() <= 0) {
            throw new IllegalStateException("Version was already released");
        }
        return this;
    }

    void release() {
        int remaining = refs.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Version released too many times");
        }
        if (remaining == 0) {
            for (TableReader[] level : levels) {
                for (TableReader table : level) {
                    table.release();
                }
            }
        }
    }

    int levelCount() {
        return levels.length;
    }

    List<TableReader> tables() {
        List<TableReader> tables = new ArrayList<>();
        for (TableReader[] level : levels) {
            tables.addAll(Arrays.asList(level));
        }
        return tables;
    }

    List<List<TableMetadata>> metadata() {
        List<List<TableMetadata>> metadata = new ArrayList<>(levels.length);
        for (TableReader[] level : levels) {
            metadata.add(Arrays.stream(level).map(TableReader::metadata).toList());
        }
        return metadata;
    }

    int tableCount() {
        int count = 0;
        for (TableReader[] level : levels) {
            count += level.length;
        }
        return count;
    }

    long sizeBytes() {
        long bytes = 0;
        for (TableReader[] level : levels) {
            for (TableReader table : level) {
                bytes += table.metadata().sizeBytes();
            }
        }
        return bytes;
    }

    Optional<TableReader> table(long id) {
        for (TableReader[] level : levels) {
            for (TableReader table : level) {
                if (table.id() == id) {
                    return Optional.of(table);
                }
            }
        }
        return Optional.empty();
    }

    boolean containsAll(Collection<TableReader> tables) {
        Set<TableReader> present = identitySet(tables());
        return present.containsAll(tables);
    }

    Optional<InternalEntry> get(Slice key, long index) {
        for (TableReader table : levels[0]) {
            Optional<InternalEntry> found = table.get(key, index);
            if (found.isPresent()) {
                return found;
            }
        }
        for (int i = 1; i < levels.length; i++) {
            TableReader[] level = levels[i];
            int position = firstEndingAtOrAfter(level, key);
            if (position < level.length && level[position].metadata().smallest().compareTo(key) <= 0) {
                Optional<InternalEntry> found = level[position].get(key, index);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    void addScans(List<Iterator<InternalEntry>> sources, Bounds bounds) {
        for (TableReader[] level : levels) {
            for (TableReader table : level) {
                TableMetadata metadata = table.metadata();
                if (bounds.overlaps(metadata.smallest(), metadata.largest())) {
                    sources.add(table.scan(bounds));
                }
            }
        }
    }

    Version withFlushed(TableReader table) {
        TableReader[][] next = copyLevels();
        TableReader[] levelZero = new TableReader[next[0].length + 1];
        levelZero[0] = table;
        System.arraycopy(next[0], 0, levelZero, 1, next[0].length);
        next[0] = levelZero;
        return new Version(next);
    }

    Version withCompaction(Collection<TableReader> removed, List<TableReader> added, int targetLevel) {
        Set<TableReader> gone = identitySet(removed);
        int count = Math.max(levels.length, targetLevel + 1);
        TableReader[][] next = new TableReader[count][];
        for (int i = 0; i < count; i++) {
            List<TableReader> level = new ArrayList<>();
            if (i < levels.length) {
                for (TableReader table : levels[i]) {
                    if (!gone.contains(table)) {
                        level.add(table);
                    }
                }
            }
            if (i == targetLevel) {
                level.addAll(added);
                if (i > 0) {
                    level.sort(BY_SMALLEST);
                }
            }
            next[i] = level.toArray(TableReader[]::new);
        }
        return new Version(next);
    }

    private TableReader[][] copyLevels() {
        TableReader[][] copy = new TableReader[levels.length][];
        for (int i = 0; i < levels.length; i++) {
            copy[i] = levels[i].clone();
        }
        return copy;
    }

    private static int firstEndingAtOrAfter(TableReader[] level, Slice key) {
        int low = 0;
        int high = level.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (level[mid].metadata().largest().compareTo(key) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private static Set<TableReader> identitySet(Collection<TableReader> tables) {
        Set<TableReader> set = Collections.newSetFromMap(new IdentityHashMap<>());
        set.addAll(tables);
        return set;
    }
}
