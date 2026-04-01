package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.BenchmarkDirectories;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageCheckpoint;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.StorageEngine;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StorageRestoreBenchmark {

    @Benchmark
    public void restoreIntoNewStore(RestoreState state) throws IOException {
        state.restoreIntoNewStore();
    }

    @State(Scope.Benchmark)
    public static class RestoreState {
        @Param({"10000", "100000"})
        int keyCount;

        @Param({"100", "1024"})
        int valueSize;

        private final StorageConfig config = StorageFixtures.defaultConfig();

        private Path sourceDir;
        private StorageCheckpoint checkpoint;
        private Path targetDir;

        @Setup(Level.Trial)
        public void createCheckpoint() throws IOException {
            sourceDir = BenchmarkDirectories.createTempDirectory("partdb-restore-source");
            try (StorageEngine sourceStore = StorageEngine.open(sourceDir, config)) {
                Bytes[] keys = BenchmarkKeys.storageKeys(keyCount);
                StorageFixtures.populate(sourceStore, keys, BenchmarkValues.fixedValue(valueSize, 0x9eedL), 1);
                checkpoint = sourceStore.checkpoint();
            }
        }

        @Setup(Level.Invocation)
        public void createTargetDirectory() throws IOException {
            targetDir = BenchmarkDirectories.createTempDirectory("partdb-restore-target");
        }

        public void restoreIntoNewStore() {
            try (StorageEngine ignored = StorageEngine.restore(targetDir, checkpoint, config)) {
                // Benchmark the direct restore path into a new store directory.
            }
        }

        @TearDown(Level.Invocation)
        public void closeTargetStore() throws IOException {
            BenchmarkDirectories.deleteRecursively(targetDir);
            targetDir = null;
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws IOException {
            BenchmarkDirectories.deleteRecursively(sourceDir);
            sourceDir = null;
        }
    }
}
