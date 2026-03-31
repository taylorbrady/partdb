package io.partdb.storage;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class CompactionService implements AutoCloseable {

    private final CompactionScheduler scheduler;

    CompactionService(
        SSTableCatalog sstableCatalog,
        LsmConfig config,
        StorageRuntimeStats stats,
        Supplier<SSTableManifest> manifestSupplier,
        Consumer<CompactionResult> resultHandler
    ) {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(config);
        CompactionExecutor executor = new CompactionExecutor(sstableCatalog, config);
        this.scheduler = new CompactionScheduler(
            planner,
            executor,
            config.maxConcurrentCompactions(),
            manifestSupplier,
            stats,
            resultHandler
        );
    }

    void schedule() {
        scheduler.scheduleCompactions();
    }

    CompactionScheduler.Pause pauseAndAwaitQuiescence(Duration timeout) {
        return scheduler.pauseAndAwaitQuiescence(timeout);
    }

    @Override
    public void close() {
        scheduler.close();
    }
}
