package io.partdb.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ElectionTimer {
    private static final Logger logger = LoggerFactory.getLogger(ElectionTimer.class);

    private final ScheduledExecutorService scheduler;
    private final long baseTimeoutMs;
    private final long maxJitterMs;
    private final Random random;
    private final AtomicReference<ScheduledFuture<?>> currentTimer;

    public ElectionTimer(ScheduledExecutorService scheduler, long baseTimeoutMs, long maxJitterMs) {
        this.scheduler = scheduler;
        this.baseTimeoutMs = baseTimeoutMs;
        this.maxJitterMs = maxJitterMs;
        this.random = new Random();
        this.currentTimer = new AtomicReference<>();
    }

    public void start(Runnable onTimeout) {
        reset(onTimeout);
    }

    public void reset(Runnable onTimeout) {
        cancel();

        long timeout = baseTimeoutMs + random.nextLong(maxJitterMs);
        ScheduledFuture<?> newTimer = scheduler.schedule(
            () -> {
                try {
                    onTimeout.run();
                } catch (Exception e) {
                    logger.error("Election timer callback failed", e);
                }
            },
            timeout,
            TimeUnit.MILLISECONDS
        );

        currentTimer.set(newTimer);
    }

    public void cancel() {
        ScheduledFuture<?> timer = currentTimer.getAndSet(null);
        if (timer != null && !timer.isDone()) {
            timer.cancel(false);
        }
    }

    public boolean isActive() {
        ScheduledFuture<?> timer = currentTimer.get();
        return timer != null && !timer.isDone();
    }
}
