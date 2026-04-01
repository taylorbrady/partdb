package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;

import java.util.Objects;
import java.util.Random;

public final class StorageWorkloadScript {

    private StorageWorkloadScript() {
    }

    public static Operation[] mixed(
        Bytes[] existingKeys,
        Bytes[] missingKeys,
        long insertKeyStart,
        int operationCount,
        int readPercent,
        long seed
    ) {
        Objects.requireNonNull(existingKeys, "existingKeys");
        Objects.requireNonNull(missingKeys, "missingKeys");
        if (existingKeys.length == 0 || missingKeys.length == 0) {
            throw new IllegalArgumentException("existingKeys and missingKeys must not be empty");
        }
        if (operationCount <= 0) {
            throw new IllegalArgumentException("operationCount must be positive");
        }
        if (readPercent < 0 || readPercent > 100) {
            throw new IllegalArgumentException("readPercent must be between 0 and 100");
        }

        Random random = new Random(seed);
        Operation[] operations = new Operation[operationCount];
        long nextInsertKey = insertKeyStart;

        for (int i = 0; i < operations.length; i++) {
            if (random.nextInt(100) < readPercent) {
                if (random.nextBoolean()) {
                    operations[i] = new Operation(
                        OperationKind.READ_EXISTING,
                        existingKeys[random.nextInt(existingKeys.length)]
                    );
                } else {
                    operations[i] = new Operation(
                        OperationKind.READ_MISSING,
                        missingKeys[random.nextInt(missingKeys.length)]
                    );
                }
            } else if (random.nextBoolean()) {
                operations[i] = new Operation(
                    OperationKind.UPDATE_EXISTING,
                    existingKeys[random.nextInt(existingKeys.length)]
                );
            } else {
                operations[i] = new Operation(
                    OperationKind.INSERT_NEW,
                    BenchmarkKeys.storageKey(nextInsertKey++)
                );
            }
        }

        return operations;
    }

    public enum OperationKind {
        READ_EXISTING,
        READ_MISSING,
        UPDATE_EXISTING,
        INSERT_NEW
    }

    public record Operation(OperationKind kind, Bytes key) {
        public Operation {
            kind = Objects.requireNonNull(kind, "kind");
            key = Objects.requireNonNull(key, "key");
        }
    }
}
