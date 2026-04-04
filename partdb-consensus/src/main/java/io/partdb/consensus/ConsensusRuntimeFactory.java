package io.partdb.consensus;

import java.nio.file.Path;

@FunctionalInterface
public interface ConsensusRuntimeFactory {
    ConsensusRuntime open(Path dataDirectory, ConsensusConfig config, ReplicatedStateMachine stateMachine);

    static ConsensusRuntimeFactory singleNode() {
        return (dataDirectory, config, stateMachine) -> ConsensusNode.open(
            dataDirectory,
            config,
            new SingleNodeRaftPeerTransport(),
            stateMachine
        );
    }
}
