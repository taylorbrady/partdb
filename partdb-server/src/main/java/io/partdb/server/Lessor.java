package io.partdb.server;

import io.partdb.common.Leases;
import io.partdb.server.command.proto.CommandProto.Command;
import io.partdb.server.command.proto.CommandProto.GrantLease;
import io.partdb.server.command.proto.CommandProto.KeepAliveLease;
import io.partdb.server.command.proto.CommandProto.RevokeLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public final class Lessor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Lessor.class);
    private static final int MAX_REVOCATIONS_PER_BATCH = 500;

    private final Proposer proposer;
    private final Leases leases;
    private final AtomicLong nextLeaseId = new AtomicLong(1);
    private final Thread expirerThread;
    private volatile boolean running = true;

    public Lessor(Proposer proposer, Leases leases) {
        this.proposer = proposer;
        this.leases = leases;
        this.expirerThread = Thread.ofVirtual()
            .name("lease-expirer")
            .start(this::runExpirer);
    }

    public CompletableFuture<Long> grant(long ttlNanos) {
        long leaseId = nextLeaseId.getAndIncrement();
        return proposer.propose(Command.newBuilder()
                .setGrantLease(GrantLease.newBuilder()
                    .setLeaseId(leaseId)
                    .setTtlNanos(ttlNanos)))
            .thenApply(_ -> leaseId);
    }

    public CompletableFuture<Void> revoke(long leaseId) {
        return proposer.propose(Command.newBuilder()
            .setRevokeLease(RevokeLease.newBuilder()
                .setLeaseId(leaseId)));
    }

    public CompletableFuture<Void> keepAlive(long leaseId) {
        return proposer.propose(Command.newBuilder()
            .setKeepAliveLease(KeepAliveLease.newBuilder()
                .setLeaseId(leaseId)));
    }

    private void runExpirer() {
        while (running) {
            try {
                if (!proposer.isLeader()) {
                    Thread.sleep(Duration.ofMillis(500));
                    continue;
                }

                var entry = leases.pollExpired(Duration.ofMillis(500));
                if (entry == null) {
                    continue;
                }

                int count = 0;
                while (entry != null && count < MAX_REVOCATIONS_PER_BATCH) {
                    if (!leases.isStale(entry)) {
                        long leaseId = entry.leaseId();
                        revoke(leaseId)
                            .exceptionally(ex -> {
                                log.warn("Failed to revoke lease {}", leaseId, ex);
                                return null;
                            });
                        count++;
                    }
                    entry = leases.pollExpiredNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        expirerThread.interrupt();
    }
}
