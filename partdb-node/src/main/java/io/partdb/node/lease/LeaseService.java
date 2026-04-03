package io.partdb.node.lease;

import io.partdb.consensus.ConsensusNode;
import io.partdb.consensus.ConsensusRole;
import io.partdb.node.internal.command.PartDbCommands;
import io.partdb.node.internal.command.PartDbCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class LeaseService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaseService.class);
    private static final int MAX_REVOCATIONS_PER_BATCH = 500;

    private final ConsensusNode consensus;
    private final PartDbCommandExecutor commandExecutor;
    private final LeaseRegistry leaseRegistry;
    private final Thread expirerThread;
    private volatile boolean running = true;

    public LeaseService(ConsensusNode consensus, PartDbCommandExecutor commandExecutor, LeaseRegistry leaseRegistry) {
        this.consensus = consensus;
        this.commandExecutor = commandExecutor;
        this.leaseRegistry = leaseRegistry;
        this.expirerThread = Thread.ofVirtual()
            .name("lease-expirer")
            .start(this::runExpirer);
    }

    public CompletableFuture<LeaseGrant> grant(Duration ttl) {
        return commandExecutor.execute(PartDbCommands.grantLease(ttl));
    }

    public CompletableFuture<LeaseRevokeResult> revoke(LeaseId leaseId) {
        return commandExecutor.execute(PartDbCommands.revokeLease(leaseId));
    }

    public CompletableFuture<LeaseKeepAliveResult> keepAlive(LeaseId leaseId) {
        return commandExecutor.execute(PartDbCommands.keepAliveLease(leaseId));
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
                        commandExecutor.execute(PartDbCommands.expireLease(LeaseId.of(leaseId)))
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
