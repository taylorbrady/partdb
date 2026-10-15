package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.BenchmarkDirectories;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageEngine;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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

        private final StorageOptions options = StorageFixtures.defaultOptions();

        private Path sourceDir;
        private Path checkpoint;
        private Path targetDir;

        @Setup(Level.Trial)
        public void createCheckpoint() throws IOException {
            sourceDir = BenchmarkDirectories.createTempDirectory("partdb-restore-source");
            try (StorageEngine sourceStore = StorageEngine.open(sourceDir, options)) {
                Bytes[] keys = BenchmarkKeys.storageKeys(keyCount);
                StorageFixtures.populate(sourceStore, keys, BenchmarkValues.fixedValue(valueSize, 0x9eedL), 1);
                checkpoint = sourceDir.resolveSibling(sourceDir.getFileName() + ".checkpoint");
                try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(checkpoint))) {
                    sourceStore.checkpoint(out);
                }
            }
        }

        @Setup(Level.Invocation)
        public void createTargetDirectory() throws IOException {
            targetDir = BenchmarkDirectories.createTempDirectory("partdb-restore-target");
        }

        public void restoreIntoNewStore() {
            try (StorageEngine store = StorageEngine.open(targetDir, options)) {
                try (InputStream in = new BufferedInputStream(Files.newInputStream(checkpoint))) {
                    store.restore(in);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
            Files.deleteIfExists(checkpoint);
            sourceDir = null;
            checkpoint = null;
        }
    }
}
