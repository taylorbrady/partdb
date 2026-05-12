package io.partdb.consensus;


import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Supplier;

public final class RaftConsensusRuntimeFactory implements ConsensusRuntimeFactory {
    private final Supplier<? extends RaftPeerTransport> transportFactory;

    public RaftConsensusRuntimeFactory(Supplier<? extends RaftPeerTransport> transportFactory) {
        this.transportFactory = Objects.requireNonNull(transportFactory, "transportFactory must not be null");
    }

    @Override
    public ConsensusRuntime open(Path dataDirectory, ConsensusConfig config, ReplicatedStateMachine stateMachine) {
        return ConsensusNode.open(
            dataDirectory,
            config,
            transportFactory.get(),
            stateMachine
        );
    }
}
