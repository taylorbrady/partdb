package io.partdb.node.internal.command;

public interface ReplicatedCommand<R> {
    PartDbCommand payload();

    R mapResult(PartDbCommandResult result);
}
