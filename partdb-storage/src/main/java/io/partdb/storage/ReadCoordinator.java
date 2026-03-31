package io.partdb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

final class ReadCoordinator {

    private final Supplier<MutableMemtable> activeMemtableSupplier;
    private final Supplier<List<ImmutableMemtable>> immutableMemtablesSupplier;
    private final TableCatalog tableCatalog;

    ReadCoordinator(
        Supplier<MutableMemtable> activeMemtableSupplier,
        Supplier<List<ImmutableMemtable>> immutableMemtablesSupplier,
        TableCatalog tableCatalog
    ) {
        this.activeMemtableSupplier = Objects.requireNonNull(activeMemtableSupplier, "activeMemtableSupplier");
        this.immutableMemtablesSupplier = Objects.requireNonNull(
            immutableMemtablesSupplier,
            "immutableMemtablesSupplier"
        );
        this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
    }

    Optional<EngineEntry> get(Slice key) {
        Optional<Mutation> result = lookupVisibleMutation(key);
        return result.flatMap(ReadCoordinator::resolveEntry);
    }

    EngineEntryCursor scan(ScanBounds bounds) {
        CatalogSnapshot readers = tableCatalog.acquire();
        try {
            List<Iterator<Mutation>> iterators = new ArrayList<>();

            iterators.add(activeMemtableSupplier.get().scan(bounds));
            addImmutableMemtableIterators(iterators, bounds, immutableMemtablesSupplier.get());

            for (SSTableReader sstable : readers.scanTables(bounds)) {
                iterators.add(sstable.scan(bounds));
            }

            return new ScanCursor(readers, new MergingIterator(iterators));
        } catch (RuntimeException e) {
            readers.close();
            throw e;
        }
    }

    Optional<Mutation> lookupVisibleMutation(Slice key) {
        Optional<Mutation> result = lookupMutation(
            key,
            activeMemtableSupplier.get(),
            immutableMemtablesSupplier.get()
        );
        if (result.isPresent()) {
            return result;
        }

        try (CatalogSnapshot readers = tableCatalog.acquire()) {
            return readers.get(key);
        }
    }

    static Optional<Mutation> lookupMutation(
        Slice key,
        MutableMemtable activeMemtable,
        List<ImmutableMemtable> immutableMemtables
    ) {
        Optional<Mutation> result = activeMemtable.get(key);
        if (result.isPresent()) {
            return result;
        }

        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            result = immutableMemtables.get(i).get(key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    private static Optional<EngineEntry> resolveEntry(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Tombstone _ -> Optional.empty();
            case Mutation.Put p -> Optional.of(new EngineEntry(p.key(), p.value(), p.revision()));
        };
    }

    private static void addImmutableMemtableIterators(
        List<Iterator<Mutation>> iterators,
        ScanBounds bounds,
        List<ImmutableMemtable> immutableMemtables
    ) {
        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            iterators.add(immutableMemtables.get(i).scan(bounds));
        }
    }

    private static final class ScanCursor implements EngineEntryCursor {
        private final CatalogSnapshot readers;
        private final MergingIterator merged;
        private EngineEntry next;
        private boolean closed;

        private ScanCursor(CatalogSnapshot readers, MergingIterator merged) {
            this.readers = readers;
            this.merged = merged;
            this.next = advance();
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public EngineEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            EngineEntry result = next;
            next = advance();
            return result;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                readers.close();
            }
        }

        private EngineEntry advance() {
            while (merged.hasNext()) {
                if (merged.next() instanceof Mutation.Put p) {
                    return new EngineEntry(p.key(), p.value(), p.revision());
                }
            }
            return null;
        }
    }
}
