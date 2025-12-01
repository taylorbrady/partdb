package io.partdb.raft;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ElectionTimer {

    private final ScheduledExecutorService scheduler;
    private final long baseTimeoutMs;
    private final long maxJitterMs;
    private final AtomicReference<ScheduledFuture<?>> currentTimer;

    public ElectionTimer(ScheduledExecutorService scheduler, long baseTimeoutMs, long maxJitterMs) {
        this.scheduler = scheduler;
        this.baseTimeoutMs = baseTimeoutMs;
        this.maxJitterMs = maxJitterMs;
        this.currentTimer = new AtomicReference<>();
    }

    public void start(Runnable onTimeout) {
        reset(onTimeout);
    }

    public void reset(Runnable onTimeout) {
        cancel();

        long timeout = baseTimeoutMs + ThreadLocalRandom.current().nextLong(maxJitterMs);
        ScheduledFuture<?> newTimer = scheduler.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);

        currentTimer.set(newTimer);
    }

    public void cancel() {
        ScheduledFuture<?> timer = currentTimer.getAndSet(null);
        if (timer != null && !timer.isDone()) {
            timer.cancel(false);
        }
    }
}
