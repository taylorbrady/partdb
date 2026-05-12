# Benchmarks

PartDB uses JMH for performance measurement. Benchmarks are explicit developer
or performance-runner tasks; they are not part of `check` or `ci`.

## Layout

Low-level storage benchmarks live with the storage module:

```text
partdb-storage/src/jmh
```

Cross-module and API-level workload benchmarks live in the benchmark assembly
module:

```text
partdb-benchmarks/src/jmh
```

Use module-local `src/jmh` directories for benchmarks of internals owned by one
module. Use `partdb-benchmarks` for workloads that depend on multiple modules or
exercise public APIs.

## Tasks

List benchmarks:

```bash
./gradlew :partdb-storage:benchmarkList
./gradlew :partdb-benchmarks:benchmarkList
```

Run a quick smoke benchmark:

```bash
./gradlew :partdb-storage:jmhQuick
./gradlew :partdb-benchmarks:jmhQuick
```

`jmhQuick` uses one warmup iteration, one measurement iteration, one fork, and a
small include set. It verifies that benchmark wiring and representative
benchmark code still run.

Run the curated performance baseline:

```bash
./gradlew jmhBaseline
```

This runs `:partdb-storage:jmhBaseline` and `:partdb-benchmarks:jmhBaseline`.
Use it before and after performance-sensitive changes when a full suite is too
expensive but a smoke run is too shallow.

Run the full benchmark suites:

```bash
./gradlew :partdb-storage:jmh
./gradlew :partdb-benchmarks:jmh
```

The full `jmh` task uses the shared project JMH convention: three warmup
iterations, five measurement iterations, one fork, JSON output, text output, and
fixed heap arguments.

## Performance Baseline

The baseline intentionally tracks a small, representative slice of storage
behavior:

| Module | Benchmark | What it covers |
| --- | --- | --- |
| `partdb-storage` | `BlockCodecBenchmark.compress` | SSTable block compression cost |
| `partdb-storage` | `BlockCodecBenchmark.decompress` | SSTable block decompression cost |
| `partdb-storage` | `BlockCacheBenchmark.hotHit` | hot block-cache lookup overhead |
| `partdb-storage` | `BloomFilterBenchmark.miss` | negative point-lookup filter cost |
| `partdb-storage` | `DataBlockReadBenchmark.findMiddle` | indexed block point lookup |
| `partdb-storage` | `DataBlockReadBenchmark.scanFull` | sequential block cursor scan |
| `partdb-benchmarks` | `StoragePointReadBenchmark.hotHit` | API-level in-memory point read |
| `partdb-benchmarks` | `StoragePointReadBenchmark.persistedHit` | API-level reopened-store point read |
| `partdb-benchmarks` | `StorageWriteBenchmark.steadyStateUpdateRandom` | steady-state update write path |
| `partdb-benchmarks` | `StorageScanBenchmark.persistedRange1000` | persisted range scan |
| `partdb-benchmarks` | `StorageCheckpointBenchmark.checkpoint` | checkpoint creation |
| `partdb-benchmarks` | `StorageCompactionBenchmark.flushToL0Burst` | flush and L0 compaction pressure |

The baseline uses a single representative parameter point for each benchmark
family so it remains tractable. Use the full `jmh` task when tuning a subsystem
and you need the complete parameter matrix.

## Reports

Full benchmark reports are written under each module's build directory:

```text
build/reports/jmh/results.txt
build/reports/jmh/results.json
```

Quick benchmark reports use separate files:

```text
build/reports/jmh/quick-results.txt
build/reports/jmh/quick-results.json
```

Baseline benchmark reports use:

```text
build/reports/jmh/baseline-results.txt
build/reports/jmh/baseline-results.json
```

## Comparing Changes

Use the same machine, JDK, Gradle version, power settings, and background load
for both runs. JMH results are sensitive to environment noise.

For a local before/after comparison:

```bash
./gradlew clean jmhBaseline
mkdir -p build/reports/jmh-baselines/before
cp partdb-storage/build/reports/jmh/baseline-results.txt build/reports/jmh-baselines/before/storage-baseline-results.txt
cp partdb-storage/build/reports/jmh/baseline-results.json build/reports/jmh-baselines/before/storage-baseline-results.json
cp partdb-benchmarks/build/reports/jmh/baseline-results.txt build/reports/jmh-baselines/before/benchmarks-baseline-results.txt
cp partdb-benchmarks/build/reports/jmh/baseline-results.json build/reports/jmh-baselines/before/benchmarks-baseline-results.json

# make the performance-sensitive change

./gradlew clean jmhBaseline
mkdir -p build/reports/jmh-baselines/after
cp partdb-storage/build/reports/jmh/baseline-results.txt build/reports/jmh-baselines/after/storage-baseline-results.txt
cp partdb-storage/build/reports/jmh/baseline-results.json build/reports/jmh-baselines/after/storage-baseline-results.json
cp partdb-benchmarks/build/reports/jmh/baseline-results.txt build/reports/jmh-baselines/after/benchmarks-baseline-results.txt
cp partdb-benchmarks/build/reports/jmh/baseline-results.json build/reports/jmh-baselines/after/benchmarks-baseline-results.json
```

Compare `baseline-results.txt` first for a human-readable summary. Use
`baseline-results.json` for scripts or review artifacts. Treat small changes as
noise unless they are repeated across fresh runs or supported by subsystem
knowledge.

## Authoring Conventions

- Keep fixture construction out of `@Benchmark` methods.
- Use deterministic data and fixed seeds.
- Use `@Param` for workload dimensions, but keep parameter matrices bounded.
- Return benchmark results or consume them with `Blackhole`.
- Use `Scope.Thread` for mutable per-thread cursors, indexes, and random state.
- Use `Scope.Benchmark` for shared read-only fixtures or intentionally shared
  resources.
- Use `@Setup(Level.Trial)` for stable expensive fixtures.
- Use `@Setup(Level.Iteration)` only when each measurement needs fresh state.
- Close stores and delete temporary directories in `@TearDown`.
