package io.partdb.storage;

import io.partdb.storage.sstable.SSTableReader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SSTableSnapshot {

    private static final int RETIRED_BIT = 1 << 30;
    private static final int COUNT_MASK = RETIRED_BIT - 1;

    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(SSTableSnapshot.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public sealed interface AcquireResult {
        record Success(SSTableSnapshot snapshot) implements AcquireResult {}
        record Retired() implements AcquireResult {}

        AcquireResult RETIRED = new Retired();
    }

    private final List<SSTableReader> readers;
    private volatile int state;
    private volatile List<SSTableReader> readersToClose;
    private final AtomicBoolean closed;

    private SSTableSnapshot(List<SSTableReader> readers) {
        this.readers = readers;
        this.state = 0;
        this.readersToClose = List.of();
        this.closed = new AtomicBoolean(false);
    }

    public static SSTableSnapshot of(List<SSTableReader> readers) {
        return new SSTableSnapshot(List.copyOf(readers));
    }

    public AcquireResult tryAcquire() {
        while (true) {
            int current = (int) STATE.getVolatile(this);
            if ((current & RETIRED_BIT) != 0) {
                return AcquireResult.RETIRED;
            }
            if (STATE.compareAndSet(this, current, current + 1)) {
                return new AcquireResult.Success(this);
            }
        }
    }

    public void release() {
        while (true) {
            int current = (int) STATE.getVolatile(this);
            int currentCount = current & COUNT_MASK;
            if (currentCount <= 0) {
                throw new IllegalStateException("SSTableSnapshot over-released");
            }

            int newState = (current & RETIRED_BIT) | (currentCount - 1);
            if (STATE.compareAndSet(this, current, newState)) {
                if (newState == RETIRED_BIT) {
                    VarHandle.acquireFence();
                    closeOrphanedReaders();
                }
                return;
            }
        }
    }

    public void retire(List<SSTableReader> orphanedReaders) {
        this.readersToClose = List.copyOf(orphanedReaders);
        VarHandle.releaseFence();

        while (true) {
            int current = (int) STATE.getVolatile(this);
            if ((current & RETIRED_BIT) != 0) {
                return;
            }
            int newState = current | RETIRED_BIT;
            if (STATE.compareAndSet(this, current, newState)) {
                if ((current & COUNT_MASK) == 0) {
                    closeOrphanedReaders();
                }
                return;
            }
        }
    }

    public boolean isDrained() {
        int current = (int) STATE.getVolatile(this);
        return current == RETIRED_BIT;
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
