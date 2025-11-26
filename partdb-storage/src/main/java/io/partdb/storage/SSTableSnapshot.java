package io.partdb.storage;

import io.partdb.storage.sstable.SSTableReader;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SSTableSnapshot {

    private static final int RETIRED_BIT = 1 << 30;
    private static final int COUNT_MASK = RETIRED_BIT - 1;

    private final List<SSTableReader> readers;
    private final AtomicInteger state = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile List<SSTableReader> readersToClose = List.of();

    private SSTableSnapshot(List<SSTableReader> readers) {
        this.readers = readers;
    }

    public static SSTableSnapshot of(List<SSTableReader> readers) {
        return new SSTableSnapshot(List.copyOf(readers));
    }

    public SSTableSnapshot tryAcquire() {
        while (true) {
            int current = state.get();
            if ((current & RETIRED_BIT) != 0) {
                return null;
            }
            if (state.compareAndSet(current, current + 1)) {
                return this;
            }
        }
    }

    public void release() {
        while (true) {
            int current = state.get();
            int currentCount = current & COUNT_MASK;
            if (currentCount <= 0) {
                throw new IllegalStateException("SSTableSnapshot over-released");
            }

            int newState = (current & RETIRED_BIT) | (currentCount - 1);
            if (state.compareAndSet(current, newState)) {
                if (newState == RETIRED_BIT) {
                    closeOrphanedReaders();
                }
                return;
            }
        }
    }

    public void retire(List<SSTableReader> orphanedReaders) {
        this.readersToClose = List.copyOf(orphanedReaders);
        while (true) {
            int current = state.get();
            if ((current & RETIRED_BIT) != 0) {
                return;
            }
            int newState = current | RETIRED_BIT;
            if (state.compareAndSet(current, newState)) {
                if ((current & COUNT_MASK) == 0) {
                    closeOrphanedReaders();
                }
                return;
            }
        }
    }

    private void closeOrphanedReaders() {
        if (closed.compareAndSet(false, true)) {
            for (SSTableReader reader : readersToClose) {
                reader.close();
            }
        }
    }

    public List<SSTableReader> readers() {
        return readers;
    }
}
