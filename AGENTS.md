# Kestra ScyllaDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.scylladb`.
- Runs native CQL queries, mutations, batches, and polling triggers against ScyllaDB.

## Why

- Query and update ScyllaDB from flows without shelling out to cqlsh.
- Return structured rows or stream large results into Kestra internal storage.
- Poll pending rows and explicitly delete or mark them processed before emitting an execution.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `scylladb`

Infrastructure dependencies (Docker Compose services):

- `app`

Tests provision the official `scylladb/scylla` image through Testcontainers; Docker and Java 21 are required.

### Key Plugin Classes

- `ScyllaDbConnection`: shared session factory, authentication, TLS, and optional request timeout.
- `AbstractScyllaDbTask`: connection-bearing task base.
- `Query`: SELECT with FETCH_ONE, FETCH, or STORE.
- `Queries`: sequential SELECT statements with per-statement outputs.
- `Execute`: DML and DDL.
- `Batch`: LOGGED, UNLOGGED, or COUNTER CQL batches.
- `Trigger`: SELECT polling with explicit per-row acknowledgement.

Internal helpers `QueryService` and `CqlValues` share fetching and reversible result conversion.

### Project Structure

```
plugin-scylladb/
├── src/main/java/io/kestra/plugin/scylladb/
├── src/test/java/io/kestra/plugin/scylladb/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.
- Render configurable values through `Property<T>`; keep secrets out of logs and `toString`.
- STORE must stream all driver pages without collecting the entire result.
- Trigger acknowledgement precedes execution delivery and is not transactional with it; do not claim exactly-once or guaranteed at-least-once delivery.
- Run `./gradlew test`, `./gradlew build`, and `./gradlew lintPluginDocs` before submitting changes.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
