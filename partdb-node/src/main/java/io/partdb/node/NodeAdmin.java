package io.partdb.node;

import java.nio.file.Path;
import java.util.concurrent.CompletionStage;

public interface NodeAdmin {
    CompletionStage<Long> backup(Path target);

    NodeMetrics nodeMetrics();

    StorageMetrics storageMetrics();
}
