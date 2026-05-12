# Repository Guidelines

## Project Structure & Module Organization

PartDB is a multi-project Gradle build. Production Java code lives in each
module under `src/main/java`; resources live under `src/main/resources`.

Core modules include `partdb-bytes`, `partdb-cluster`, `partdb-raft`,
`partdb-storage`, `partdb-consensus`, and `partdb-node`. Transport and API
modules include `partdb-grpc`, `partdb-transport-grpc`, and `partdb-client`.
Runtime assembly lives in `partdb-server` and `partdb-app`. Benchmarks live in
`partdb-storage/src/jmh` and `partdb-benchmarks/src/jmh`.

Keep module dependencies aligned with `docs/modules.md`. Core modules should not
depend on gRPC, CLI code, packaging, or process lifecycle concerns.

## Build, Test, and Development Commands

- `./gradlew build`: compile and run normal project checks.
- `./gradlew test`: run fast module-local unit tests.
- `./gradlew check`: run the standard local quality gate.
- `./gradlew integrationTest`: run in-process cross-module integration tests.
- `./gradlew packagedIntegrationTest`: test the installed app, generated
  scripts, runtime classpath, CLI, and real OS processes.
- `./gradlew ci`: run the required pre-merge gate.
- `./gradlew :partdb-app:run --args="..."`: run the CLI locally.

## Coding Style & Naming Conventions

Use modern Java with Gradle Kotlin DSL. Prefer immutable value types, records for
simple data carriers, defensive copies for arrays/collections, and
`AutoCloseable` for owned resources. Keep package names under `io.partdb`.

Use 4-space indentation in Java and Gradle files. Class and record names use
`UpperCamelCase`; methods, fields, and local variables use `lowerCamelCase`.
Test classes should end in `Test`, `IntegrationTest`, or `SmokeTest`.

## Testing Guidelines

JUnit Jupiter is the test framework. Keep fast unit tests in `src/test`.
Behavioral cluster tests belong in `src/integrationTest`. Packaged executable
and process tests belong in `src/packagedIntegrationTest`.

Do not put broad cluster scenarios in packaged tests unless they specifically
verify distribution behavior. JMH benchmarks are performance tools and should
not be wired into `check` or `ci`.

## Commit & Pull Request Guidelines

Commit subjects in this repo use short imperative, sentence-case messages, for
example: `Refactor consensus and transport around raft runtime boundaries`.
Keep commits focused and avoid mixing unrelated build, docs, and behavior
changes.

Pull requests should describe the behavior or structure changed, call out module
boundary impacts, and list commands run. Link issues when applicable. Include
CLI output or screenshots only when user-facing command behavior changes.

## Agent-Specific Instructions

Respect existing worktree changes. Do not revert unrelated edits. Before moving
code across modules, check `docs/modules.md`; before changing test wiring, check
`docs/testing.md`.
