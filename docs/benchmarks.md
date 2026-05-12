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

Run the full benchmark suites:

```bash
./gradlew :partdb-storage:jmh
./gradlew :partdb-benchmarks:jmh
```

The full `jmh` task uses the shared project JMH convention: three warmup
iterations, five measurement iterations, one fork, JSON output, text output, and
fixed heap arguments.

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
