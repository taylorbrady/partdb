package io.partdb.node.metrics;

import io.partdb.node.recovery.LogicalBackup;

import java.util.concurrent.CompletionStage;

public interface MaintenanceOperations {
    CompletionStage<LogicalBackup> createBackup();

    NodeMetrics nodeMetrics();

    StorageMetrics storageMetrics();
}
