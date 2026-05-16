package io.partdb.node.admin;

import java.util.concurrent.CompletionStage;

public interface NodeAdmin {
    CompletionStage<PartDbBackup> createBackup();

    NodeMetrics nodeMetrics();

    StorageMetrics storageMetrics();
}
