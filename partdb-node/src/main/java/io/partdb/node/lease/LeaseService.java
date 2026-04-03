package io.partdb.node.lease;

import io.partdb.consensus.ConsensusNode;
import io.partdb.consensus.ConsensusRole;
import io.partdb.node.command.CommandProposer;
import io.partdb.node.command.PartDbCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class LeaseService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaseService.class);
    private static final int MAX_REVOCATIONS_PER_BATCH = 500;

    private final ConsensusNode consensus;
    private final CommandProposer proposer;
    private final LeaseRegistry leaseRegistry;
    private final Thread expirerThread;
    private volatile boolean running = true;

    public LeaseService(ConsensusNode consensus, CommandProposer proposer, LeaseRegistry leaseRegistry) {
        this.consensus = consensus;
        this.proposer = proposer;
        this.leaseRegistry = leaseRegistry;
        this.expirerThread = Thread.ofVirtual()
            .name("lease-expirer")
            .start(this::runExpirer);
    }

    public CompletableFuture<Long> grant(Duration ttl) {
        return proposer.propose(new PartDbCommand.GrantLease(ttl.toNanos()));
    }

    public CompletableFuture<Long> revoke(LeaseId leaseId) {
        return proposer.propose(new PartDbCommand.RevokeLease(leaseId.value()));
    }

    public CompletableFuture<Long> keepAlive(LeaseId leaseId) {
        return proposer.propose(new PartDbCommand.KeepAliveLease(leaseId.value()));
    }

    private void runExpirer() {
        while (running) {
            try {
                if (consensus.status().role() != ConsensusRole.LEADER) {
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
                        proposer.propose(new PartDbCommand.ExpireLease(leaseId))
                            .exceptionally(ex -> {
                                log.atWarn()
                                    .addKeyValue("leaseId", leaseId)
                                    .setCause(ex)
                                    .log("Failed to expire lease");
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
