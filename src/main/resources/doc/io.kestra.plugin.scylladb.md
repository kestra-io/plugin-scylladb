Run CQL against ScyllaDB using its native protocol, normally on port 9042.

## Tasks

- `Query` executes a SELECT and returns a single row (`FETCH_ONE`), every row (`FETCH`), or an internal-storage Ion file (`STORE`).
- `Queries` executes SELECT statements sequentially and returns `outputs` in statement order.
- `Execute` executes INSERT, UPDATE, DELETE, and DDL. `wasApplied` reports conditional-write success; `affectedRows` is null because CQL does not report affected-row counts.
- `Batch` executes a native LOGGED, UNLOGGED, or COUNTER batch. `statements` counts submitted statements, not affected rows. Keep batches small, preferably within a partition. COUNTER batches cannot contain ordinary mutations; batches cannot contain SELECT or DDL and are not SQL transactions.
- `Trigger` polls a SELECT and acknowledges each returned row before creating an execution.

## Connection

All tasks and the trigger share `connection.contactPoints`, `localDatacenter`, optional `keyspace`, and optional paired `username`/`password`. Contact points accept `host:port` or `[IPv6]:port`; an omitted port defaults to 9042. Use Kestra secrets for credentials.

`tlsEnabled: true` enables certificate and hostname verification. JVM trust roots are used unless `truststorePath` is set. Optional `keystorePath` supplies client certificates for mutual TLS. Store files must be readable on the process evaluating the task or trigger; they are local paths, not internal-storage URIs. Configure store passwords through `truststorePassword` and `keystorePassword` secret expressions. No insecure trust-all mode is provided.

`requestTimeout` optionally overrides the driver's per-request timeout (for example `PT30S`) for slow queries or maintenance statements. It must be positive. It is neither a task deadline nor a retry policy; the driver default applies when omitted.

## Fetching and values

Query, Queries, and Trigger default to `FETCH`. `size` is the emitted row count: zero or one for FETCH_ONE, all fetched rows for FETCH and STORE. An empty FETCH returns an empty list, an empty FETCH_ONE returns a null row, and an empty STORE returns a URI to an empty file. Fields for other fetch modes are null.

FETCH consumes all pages into memory. STORE iterates all pages into a local Ion file, closes it, then uploads it into Kestra internal storage. Prefer STORE for large results.

Optional `fetchSize` is a positive driver page-size hint for balancing memory use and round trips. It is not a total row limit or a strict memory bound. Omit it for the driver default; use CQL `LIMIT` to bound a query or poll.

Rows use selected column names. UUIDs, temporal values, addresses, and CQL durations are strings; blobs are base64 strings; tuples and collections are lists; UDTs are maps. CQL maps with non-string keys are lists of `{key, value}` entries to preserve key types. Numbers, booleans, and nulls retain their value types.

CQL templates are rendered before execution. Do not interpolate untrusted input into query strings. Trigger acknowledgement values are instead bound using prepared statements.

## Polling and acknowledgement

The trigger requires `acknowledgeCql`, an idempotent UPDATE or DELETE that removes each emitted row from the SELECT's future results. Use named markers matching selected column names, and select the complete primary key. For example:

```yaml
id: scylladb_events
namespace: company.team

triggers:
  - id: pending
    type: io.kestra.plugin.scylladb.Trigger
    interval: PT1M
    connection:
      contactPoints:
        - "{{ secret('SCYLLADB_HOST') }}:9042"
      localDatacenter: datacenter1
      keyspace: events
      username: "{{ secret('SCYLLADB_USERNAME') }}"
      password: "{{ secret('SCYLLADB_PASSWORD') }}"
    cql: SELECT event_id, event_type, payload FROM pending_events LIMIT 100
    acknowledgeCql: DELETE FROM pending_events WHERE event_id = :event_id
    fetchType: FETCH

tasks:
  - id: handle
    type: io.kestra.plugin.core.log.Log
    message: "Processing {{ trigger.size }} events: {{ trigger.rows }}"
```

Before enabling this flow, create the `events` keyspace and `pending_events (event_id uuid PRIMARY KEY, event_type text, payload text)` table, and populate it from your producer. For a mark-processed design, SELECT pending rows and UPDATE their `processed` flag using their full primary key.

An empty poll emits nothing and acknowledges nothing. FETCH_ONE acknowledges only its returned row. Fetching and storage upload finish before acknowledgement; missing or incompatible marker values fail before the first mutation. Do not use broad updates that also advance rows which were not selected.

**Delivery limitation:** acknowledgement happens before the flow processes the data. ScyllaDB mutations and Kestra execution delivery do not commit atomically. Partial acknowledgement failure, or a crash after acknowledgement but before execution delivery, can lose events. Concurrent consumers may process duplicates. This trigger offers neither exactly-once nor guaranteed at-least-once delivery. Use one consumer and idempotent acknowledgements, and do not use it as a lossless message queue.

## Development

Use Java 21 and Docker with Linux containers. Tests provision a pinned official `scylladb/scylla` image through Testcontainers; no existing cluster is required. Docker must be available rather than silently skipping integration tests.

Run `./gradlew test`, `./gradlew build`, and `./gradlew lintPluginDocs` (or `gradlew.bat` on Windows).
