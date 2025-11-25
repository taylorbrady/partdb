package io.partdb.server;

import io.partdb.common.Leases;
import io.partdb.common.statemachine.GrantLease;
import io.partdb.common.statemachine.KeepAliveLease;
import io.partdb.common.statemachine.RevokeLease;
import io.partdb.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Lessor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Lessor.class);
    private static final long CHECK_INTERVAL_MILLIS = 100;

    private final RaftNode raftNode;
    private final Leases leases;
    private final AtomicLong nextLeaseId;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> expirationTask;

    public Lessor(RaftNode raftNode, Leases leases) {
        this.raftNode = raftNode;
        this.leases = leases;
        this.nextLeaseId = new AtomicLong(1);
        this.scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
    }

    public CompletableFuture<Long> grant(long ttlMillis) {
        long leaseId = nextLeaseId.getAndIncrement();
        long grantedAtMillis = System.currentTimeMillis();
        GrantLease grantLease = new GrantLease(leaseId, ttlMillis, grantedAtMillis);
        return raftNode.propose(grantLease).thenApply(v -> leaseId);
    }

    public CompletableFuture<Void> revoke(long leaseId) {
        RevokeLease revokeLease = new RevokeLease(leaseId);
        return raftNode.propose(revokeLease);
    }

    public CompletableFuture<Void> keepAlive(long leaseId) {
        KeepAliveLease keepAliveLease = new KeepAliveLease(leaseId);
        return raftNode.propose(keepAliveLease);
    }

    public void start() {
        if (expirationTask != null) {
            logger.warn("Lease expiration task already running");
            return;
        }
        expirationTask = scheduler.scheduleAtFixedRate(
            this::checkAndRevokeExpiredLeases,
            CHECK_INTERVAL_MILLIS,
            CHECK_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS
        );
        logger.info("Lease expiration task started");
    }

    public void stop() {
        if (expirationTask != null) {
            expirationTask.cancel(false);
            expirationTask = null;
            logger.info("Lease expiration task stopped");
        }
    }

    @Override
    public void close() {
        stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void checkAndRevokeExpiredLeases() {
        if (!raftNode.isLeader()) {
            return;
        }

        try {
            long now = System.currentTimeMillis();
            List<Long> expiredLeases = leases.getExpired(now);

            for (long leaseId : expiredLeases) {
                revoke(leaseId).exceptionally(ex -> {
                    logger.error("Failed to revoke expired lease {}", leaseId, ex);
                    return null;
                });
            }
        } catch (Exception e) {
            logger.error("Failed to check/revoke expired leases", e);
        }
    }
}
