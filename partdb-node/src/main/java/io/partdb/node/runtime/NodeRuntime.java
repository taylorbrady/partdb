package io.partdb.node.runtime;

import io.partdb.consensus.ConsensusRuntime;
import io.partdb.consensus.ConsensusRuntimeFactory;
import io.partdb.node.PartDbNodeConfig;
import io.partdb.node.state.PartDbStateMachine;

import java.util.Objects;

public final class NodeRuntime implements AutoCloseable {
    private final PartDbStateMachine stateMachine;
    private final ConsensusRuntime consensus;
    private final CommandProposer commandProposer;

    public NodeRuntime(PartDbNodeConfig config, ConsensusRuntimeFactory runtimeFactory) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(runtimeFactory, "runtimeFactory must not be null");

        this.stateMachine = PartDbStateMachine.open(
            config.dataDirectory().resolve("db"),
            config.storage().toStorageOptions()
        );

        try {
            this.consensus = runtimeFactory.open(
                config.dataDirectory().resolve("consensus"),
                config.toConsensusConfig(),
                stateMachine
            );
            this.commandProposer = new CommandProposer(consensus);
        } catch (RuntimeException | Error e) {
            stateMachine.close();
            throw e;
        }
    }

    public PartDbStateMachine stateMachine() {
        return stateMachine;
    }

    public ConsensusRuntime consensus() {
        return consensus;
    }

    public CommandProposer commandProposer() {
        return commandProposer;
    }

    @Override
    public void close() {
        consensus.close();
        stateMachine.close();
    }
}
