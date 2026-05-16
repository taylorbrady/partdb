package io.partdb.node.command;

public sealed interface KvCommandResult permits KvCommandResult.Applied {
    record Applied(long revision) implements KvCommandResult {
        public Applied {
            if (revision <= 0) {
                throw new IllegalArgumentException("revision must be positive");
            }
        }
    }
}
