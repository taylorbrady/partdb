package io.partdb.consensus;

public interface ReplicatedStateMachine {
    StateMachineResult apply(CommittedCommand command);

    StoredSnapshot snapshot();

    void restore(StoredSnapshot snapshot);
}
