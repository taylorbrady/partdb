package io.partdb.node.lease;

import io.partdb.node.command.CommandProposer;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.command.proto.CommandProto.GrantLease;
import io.partdb.node.command.proto.CommandProto.KeepAliveLease;
import io.partdb.node.command.proto.CommandProto.RevokeLease;
import io.partdb.node.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public final class LeaseManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);
    private static final int MAX_REVOCATIONS_PER_BATCH = 500;

    private final RaftNode raftNode;
    private final CommandProposer proposer;
    private final LeaseRegistry leaseRegistry;
    private final AtomicLong nextLeaseId = new AtomicLong(1);
    private final Thread expirerThread;
    private volatile boolean running = true;

    public LeaseManager(RaftNode raftNode, CommandProposer proposer, LeaseRegistry leaseRegistry) {
        this.raftNode = raftNode;
        this.proposer = proposer;
        this.leaseRegistry = leaseRegistry;
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

    public CompletableFuture<Long> revoke(long leaseId) {
        return proposer.propose(Command.newBuilder()
            .setRevokeLease(RevokeLease.newBuilder()
                .setLeaseId(leaseId)));
    }

    public CompletableFuture<Long> keepAlive(long leaseId) {
        return proposer.propose(Command.newBuilder()
            .setKeepAliveLease(KeepAliveLease.newBuilder()
                .setLeaseId(leaseId)));
    }

    private void runExpirer() {
        while (running) {
            try {
                if (!raftNode.isLeader()) {
                    Thread.sleep(Duration.ofMillis(500));
                    continue;
                }

                var entry = leaseRegistry.pollExpired(Duration.ofMillis(500));
                if (entry == null) {
                    continue;
                }

                int count = 0;
                while (entry != null && count < MAX_REVOCATIONS_PER_BATCH) {
                    if (!leaseRegistry.isStale(entry)) {
                        long leaseId = entry.leaseId();
                        revoke(leaseId)
                            .exceptionally(ex -> {
                                log.atWarn()
                                    .addKeyValue("leaseId", leaseId)
                                    .setCause(ex)
                                    .log("Failed to revoke lease");
                                return null;
                            });
                        count++;
                    }
                    entry = leaseRegistry.pollExpiredNow();
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
