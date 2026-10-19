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

    static final int LEVELS = 7;

    private static final Comparator<TableReader> BY_SMALLEST = Comparator.comparing(table -> table.metadata().smallest());

    private final TableReader[][] levels;
    private final AtomicInteger refs = new AtomicInteger(1);

    private Version(TableReader[][] levels) {
        for (int level = 1; level < LEVELS; level++) {
            TableReader[] tables = levels[level];
            for (int i = 1; i < tables.length; i++) {
                if (tables[i - 1].metadata().largest().compareTo(tables[i].metadata().smallest()) >= 0) {
                    throw new IllegalStateException("Tables %d and %d overlap in level %d".formatted(tables[i - 1].id(), tables[i].id(), level));
                }
            }
        }
        this.levels = levels;
        for (TableReader[] level : levels) {
            for (TableReader table : level) {
                table.retain();
            }
        }
    }

    static Version empty() {
        return of(emptyLevels());
    }

    static List<List<TableReader>> emptyLevels() {
        List<List<TableReader>> levels = new ArrayList<>(LEVELS);
        for (int level = 0; level < LEVELS; level++) {
            levels.add(new ArrayList<>());
        }
        return levels;
    }

    static Version of(List<List<TableReader>> levels) {
        if (levels.size() != LEVELS) {
            throw new IllegalArgumentException("Expected " + LEVELS + " levels, got " + levels.size());
        }
        TableReader[][] sorted = new TableReader[LEVELS][];
        for (int level = 0; level < LEVELS; level++) {
            sorted[level] = levels.get(level).toArray(TableReader[]::new);
            if (level > 0) {
                Arrays.sort(sorted[level], BY_SMALLEST);
            }
        }
        return new Version(sorted);
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

    List<TableReader> level(int level) {
        return List.of(levels[level]);
    }

    List<TableReader> tables() {
        List<TableReader> tables = new ArrayList<>();
        for (TableReader[] level : levels) {
            tables.addAll(Arrays.asList(level));
        }
        return tables;
    }

    List<List<TableMetadata>> metadata() {
        List<List<TableMetadata>> metadata = new ArrayList<>(LEVELS);
        for (TableReader[] level : levels) {
            metadata.add(Arrays.stream(level).map(TableReader::metadata).toList());
        }
        return metadata;
    }

    List<Manifest.Placement> placements() {
        List<Manifest.Placement> placements = new ArrayList<>();
        for (int level = 0; level < LEVELS; level++) {
            for (TableReader table : levels[level]) {
                placements.add(new Manifest.Placement(table.id(), level));
            }
        }
        return placements;
    }

    int levelZeroCount() {
        return levels[0].length;
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
        return identitySet(tables()).containsAll(tables);
    }

    Optional<InternalEntry> get(Slice key) {
        for (TableReader table : levels[0]) {
            Optional<InternalEntry> found = table.get(key);
            if (found.isPresent()) {
                return found;
            }
        }
        for (int level = 1; level < LEVELS; level++) {
            TableReader[] tables = levels[level];
            int position = firstEndingAtOrAfter(tables, key);
            if (position < tables.length) {
                Optional<InternalEntry> found = tables[position].get(key);
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
        TableReader[][] next = levels.clone();
        TableReader[] levelZero = new TableReader[levels[0].length + 1];
        levelZero[0] = table;
        System.arraycopy(levels[0], 0, levelZero, 1, levels[0].length);
        next[0] = levelZero;
        return new Version(next);
    }

    Version withCompaction(Collection<TableReader> removed, List<TableReader> added, int targetLevel) {
        Set<TableReader> gone = identitySet(removed);
        TableReader[][] next = new TableReader[LEVELS][];
        for (int level = 0; level < LEVELS; level++) {
            List<TableReader> tables = new ArrayList<>();
            for (TableReader table : levels[level]) {
                if (!gone.contains(table)) {
                    tables.add(table);
                }
            }
            if (level == targetLevel) {
                tables.addAll(added);
                if (level > 0) {
                    tables.sort(BY_SMALLEST);
                }
            }
            next[level] = tables.toArray(TableReader[]::new);
        }
        return new Version(next);
    }

    private static int firstEndingAtOrAfter(TableReader[] tables, Slice key) {
        int low = 0;
        int high = tables.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (tables[mid].metadata().largest().compareTo(key) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    static Set<TableReader> identitySet(Collection<TableReader> tables) {
        Set<TableReader> set = Collections.newSetFromMap(new IdentityHashMap<>());
        set.addAll(tables);
        return set;
    }
}
