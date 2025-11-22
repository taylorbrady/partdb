package io.partdb.raft;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class HeartbeatScheduler {
    private final ScheduledExecutorService scheduler;
    private final long intervalMs;
    private final AtomicReference<ScheduledFuture<?>> currentTimer;

    public HeartbeatScheduler(ScheduledExecutorService scheduler, long intervalMs) {
        this.scheduler = scheduler;
        this.intervalMs = intervalMs;
        this.currentTimer = new AtomicReference<>();
    }

    public void start(Runnable onHeartbeat) {
        cancel();

        ScheduledFuture<?> newTimer = scheduler.scheduleAtFixedRate(
            onHeartbeat,
            0,
            intervalMs,
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
