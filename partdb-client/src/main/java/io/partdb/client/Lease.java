package io.partdb.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class Lease implements AutoCloseable {

    private final long leaseId;
    private final Duration ttl;
    private final ScheduledExecutorService scheduler;
    private final Function<Long, CompletableFuture<Void>> keepAliveFunc;
    private final Function<Long, CompletableFuture<Void>> revokeFunc;
    private final AtomicBoolean valid;
    private final AtomicInteger consecutiveFailures;
    private final ScheduledFuture<?> renewalTask;
    private volatile Runnable onLeaseAtRisk;
    private volatile Runnable onLeaseRevoked;

    Lease(
        long leaseId,
        Duration ttl,
        ScheduledExecutorService scheduler,
        Function<Long, CompletableFuture<Void>> keepAliveFunc,
        Function<Long, CompletableFuture<Void>> revokeFunc
    ) {
        this.leaseId = leaseId;
        this.ttl = ttl;
        this.scheduler = scheduler;
        this.keepAliveFunc = keepAliveFunc;
        this.revokeFunc = revokeFunc;
        this.valid = new AtomicBoolean(true);
        this.consecutiveFailures = new AtomicInteger(0);

        long renewalIntervalMs = ttl.toMillis() / 2;
        this.renewalTask = scheduler.scheduleAtFixedRate(
            this::doRenewal,
            renewalIntervalMs,
            renewalIntervalMs,
            TimeUnit.MILLISECONDS
        );
    }

    public long leaseId() {
        return leaseId;
    }

    public Duration ttl() {
        return ttl;
    }

    public boolean isValid() {
        return valid.get();
    }

    public CompletableFuture<Void> keepAlive() {
        if (!valid.get()) {
            return CompletableFuture.failedFuture(new KvClientException.LeaseExpired(leaseId));
        }
        return keepAliveFunc.apply(leaseId)
            .whenComplete((v, ex) -> {
                if (ex != null) {
                    consecutiveFailures.incrementAndGet();
                } else {
                    consecutiveFailures.set(0);
                }
            });
    }

    public CompletableFuture<Void> revoke() {
        if (valid.compareAndSet(true, false)) {
            renewalTask.cancel(false);
            return revokeFunc.apply(leaseId)
                .whenComplete((v, ex) -> {
                    Runnable callback = onLeaseRevoked;
                    if (callback != null) {
                        callback.run();
                    }
                });
        }
        return CompletableFuture.completedFuture(null);
    }

    public void onLeaseAtRisk(Runnable callback) {
        this.onLeaseAtRisk = callback;
    }

    public void onLeaseRevoked(Runnable callback) {
        this.onLeaseRevoked = callback;
    }

    @Override
    public void close() {
        if (valid.compareAndSet(true, false)) {
            renewalTask.cancel(false);
            Runnable callback = onLeaseRevoked;
            if (callback != null) {
                callback.run();
            }
        }
    }

    private void doRenewal() {
        if (!valid.get()) {
            return;
        }

        try {
            keepAliveFunc.apply(leaseId)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        int failures = consecutiveFailures.incrementAndGet();
                        if (failures >= 2) {
                            Runnable callback = onLeaseAtRisk;
                            if (callback != null) {
                                callback.run();
                            }
                        }
                    } else {
                        consecutiveFailures.set(0);
                    }
                });
        } catch (Exception e) {
            consecutiveFailures.incrementAndGet();
        }
    }
}
