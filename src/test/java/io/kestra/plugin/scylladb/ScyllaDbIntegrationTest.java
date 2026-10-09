package io.kestra.plugin.scylladb;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ScyllaDbIntegrationTest extends ScyllaDbContainer {
    private static final String KEYSPACE = "kestra_" + UUID.randomUUID().toString().replace("-", "");
    private static final String SELECT_EVENTS = "SELECT tenant, id, value FROM events WHERE tenant = 'test'";
    private CqlSession admin;

    @BeforeAll
    void startScylla() {
        SCYLLA.start();
        admin = session();
        // ScyllaDB 6.2 tablets do not support counters; exercise native counter batches on vnodes.
        admin.execute("CREATE KEYSPACE " + KEYSPACE
            + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            + " AND tablets = {'enabled': false}");
        admin.execute("CREATE TABLE " + KEYSPACE
            + ".events (tenant text, id int, value text, PRIMARY KEY (tenant, id))");
        admin.execute("CREATE TABLE " + KEYSPACE
            + ".counters (tenant text, id int, total counter, PRIMARY KEY (tenant, id))");
        admin.execute("CREATE TABLE " + KEYSPACE
            + ".pending (bucket text, event_id text, value text, PRIMARY KEY (bucket, event_id))");
    }

    @AfterAll
    void stopScylla() {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            SCYLLA.stop();
        }
    }

    @BeforeEach
    void resetTables() {
        admin.execute("TRUNCATE " + KEYSPACE + ".events");
        admin.execute("TRUNCATE " + KEYSPACE + ".counters");
        admin.execute("TRUNCATE " + KEYSPACE + ".pending");
    }

    @Test
    void queryFetchTraversesEveryPage() throws Exception {
        seedEvents(11);
        Query query = query(SELECT_EVENTS, FetchType.FETCH);
        Query.Output output = query.run(context(query));

        assertEquals(11L, output.getSize());
        assertEquals(IntStream.range(0, 11).boxed().toList(),
            output.getRows().stream().map(row -> row.get("id")).toList());
        assertEquals("value-10", output.getRows().getLast().get("value"));
        assertNull(output.getRow());
        assertNull(output.getUri());
    }

    @Test
    void queryFetchOneReturnsOnlyFirstRowAndHandlesEmptyResults() throws Exception {
        seedEvents(5);
        Query query = query(SELECT_EVENTS, FetchType.FETCH_ONE);
        Query.Output output = query.run(context(query));
        assertEquals(1L, output.getSize());
        assertEquals(0, output.getRow().get("id"));
        assertNull(output.getRows());
        assertNull(output.getUri());

        Query empty = query(SELECT_EVENTS + " AND id = 99", FetchType.FETCH_ONE);
        Query.Output emptyOutput = empty.run(context(empty));
        assertEquals(0L, emptyOutput.getSize());
        assertNull(emptyOutput.getRow());
    }

    @Test
    void queryFetchReturnsEmptyList() throws Exception {
        Query query = query(SELECT_EVENTS, FetchType.FETCH);
        Query.Output output = query.run(context(query));
        assertEquals(0L, output.getSize());
        assertNotNull(output.getRows());
        assertTrue(output.getRows().isEmpty());
    }

    @Test
    void queryStoreReadsBackEveryPageAndEmptyFile() throws Exception {
        seedEvents(11);
        Query query = query(SELECT_EVENTS, FetchType.STORE);
        RunContext context = context(query);
        Query.Output output = query.run(context);
        assertEquals(11L, output.getSize());
        assertNotNull(output.getUri());
        assertNull(output.getRows());
        assertNull(output.getRow());
        List<Map<String, Object>> rows = readStored(context, output.getUri());
        assertEquals(IntStream.range(0, 11).boxed().toList(),
            rows.stream().map(row -> ((Number) row.get("id")).intValue()).toList());
        assertEquals("value-10", rows.getLast().get("value"));

        Query empty = query(SELECT_EVENTS + " AND id = 99", FetchType.STORE);
        RunContext emptyContext = context(empty);
        Query.Output emptyOutput = empty.run(emptyContext);
        assertEquals(0L, emptyOutput.getSize());
        assertNotNull(emptyOutput.getUri());
        assertTrue(readStored(emptyContext, emptyOutput.getUri()).isEmpty());
    }

    @Test
    void queryRejectsNoneWithoutExecutingMutation() throws Exception {
        Query query = query("INSERT INTO events (tenant, id, value) VALUES ('test', 7, 'written')", FetchType.NONE);
        assertThrows(IllegalArgumentException.class, () -> query.run(context(query)));
        assertNull(admin.execute("SELECT value FROM " + KEYSPACE
            + ".events WHERE tenant = 'test' AND id = 7").one());
    }

    @Test
    void queryRendersYamlConnectionCqlFetchModeAndPageSize() throws Exception {
        seedEvents(7);
        Query query = yamlTask(Query.class, """
            cql: "{{ inputs.statement }}"
            fetchType: "{{ inputs.mode }}"
            fetchSize: "{{ inputs.pageSize }}"
            """);
        RunContext context = context(query, Map.of(
            "statement", SELECT_EVENTS,
            "mode", "FETCH",
            "pageSize", 2
        ));
        Query.Output output = query.run(context);
        assertEquals(7L, output.getSize());
        assertEquals(6, output.getRows().getLast().get("id"));
    }

    @Test
    void queriesExecuteInOrderAndReturnOneOutputPerStatement() throws Exception {
        seedEvents(2);
        Queries queries = yamlTask(Queries.class, """
            cql:
              - "SELECT value FROM events WHERE tenant = 'test' AND id = {{ inputs.id }}"
              - "SELECT value FROM events WHERE tenant = 'test' AND id = 1"
              - "SELECT value FROM events WHERE tenant = 'test' AND id = 99"
            fetchType: FETCH_ONE
            fetchSize: 1
            """);
        Queries.Output output = queries.run(context(queries, Map.of("id", 0)));
        assertEquals(3, output.getOutputs().size());
        assertEquals("value-0", output.getOutputs().get(0).getRow().get("value"));
        assertEquals("value-1", output.getOutputs().get(1).getRow().get("value"));
        assertEquals(0L, output.getOutputs().get(2).getSize());
    }

    @Test
    void queriesRenderWholeListAndStoreSeparateResults() throws Exception {
        seedEvents(7);
        Queries queries = yamlTask(Queries.class, """
            cql: "{{ inputs.statements }}"
            fetchType: STORE
            fetchSize: 2
            """);
        RunContext context = context(queries, Map.of("statements", List.of(
            SELECT_EVENTS, SELECT_EVENTS + " AND id = 99"
        )));
        List<Query.Output> outputs = queries.run(context).getOutputs();
        assertEquals(2, outputs.size());
        assertEquals(7L, outputs.getFirst().getSize());
        assertEquals(7, readStored(context, outputs.getFirst().getUri()).size());
        assertEquals(0L, outputs.getLast().getSize());
        assertTrue(readStored(context, outputs.getLast().getUri()).isEmpty());
        assertNotEquals(outputs.getFirst().getUri(), outputs.getLast().getUri());
    }

    @Test
    void queriesFetchConsumesAllPagesForEachStatement() throws Exception {
        seedEvents(7);
        Queries queries = Queries.builder()
            .id("queries")
            .type(Queries.class.getName())
            .connection(connection())
            .cql(Property.ofValue(List.of(SELECT_EVENTS, SELECT_EVENTS + " AND id = 99")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .fetchSize(Property.ofValue(2))
            .build();
        List<Query.Output> outputs = queries.run(context(queries)).getOutputs();
        assertEquals(2, outputs.size());
        assertEquals(7L, outputs.getFirst().getSize());
        assertEquals(6, outputs.getFirst().getRows().getLast().get("id"));
        assertEquals(0L, outputs.getLast().getSize());
        assertTrue(outputs.getLast().getRows().isEmpty());
    }

    @Test
    void executeSupportsDdlInsertUpdateDeleteAndConditionalWrites() throws Exception {
        Execute ddl = execute("CREATE TABLE IF NOT EXISTS ddl_check (id int PRIMARY KEY)");
        assertTrue(ddl.run(context(ddl)).isWasApplied());

        Execute insert = yamlTask(Execute.class, """
            cql: "INSERT INTO events (tenant, id, value) VALUES ('test', 1, '{{ inputs.value }}') IF NOT EXISTS"
            """);
        Execute.Output inserted = insert.run(context(insert, Map.of("value", "first")));
        assertTrue(inserted.isWasApplied());
        Execute.Output duplicate = insert.run(context(insert, Map.of("value", "duplicate")));
        assertFalse(duplicate.isWasApplied());

        Execute update = execute("UPDATE events SET value = 'updated' WHERE tenant = 'test' AND id = 1 IF value = 'first'");
        assertTrue(update.run(context(update)).isWasApplied());
        assertFalse(update.run(context(update)).isWasApplied());
        assertEquals("updated", admin.execute("SELECT value FROM " + KEYSPACE
            + ".events WHERE tenant = 'test' AND id = 1").one().getString("value"));

        Execute delete = execute("DELETE FROM events WHERE tenant = 'test' AND id = 1");
        Execute.Output deleted = delete.run(context(delete));
        assertTrue(deleted.isWasApplied());
        assertNull(admin.execute("SELECT * FROM " + KEYSPACE + ".events WHERE tenant = 'test' AND id = 1").one());
    }

    @Test
    void bindsNamedParametersWithoutInterpolatingValues() throws Exception {
        var quote = "O'Reilly";
        var injected = "x'; DELETE FROM events; --";
        Execute insert = yamlTask(Execute.class, """
            cql: "INSERT INTO events (tenant, id, value) VALUES (:tenant, :id, :value)"
            parameters:
              tenant: "{{ inputs.tenant }}"
              id: "{{ inputs.id }}"
              value: "x'; DELETE FROM events; --"
            """);
        assertTrue(insert.run(context(insert, Map.of("tenant", quote, "id", "7"))).isWasApplied());

        Query query = Query.builder()
            .id("query").type(Query.class.getName()).connection(connection())
            .cql(Property.ofValue("SELECT value FROM events WHERE tenant = :tenant AND id = :id"))
            .parameters(Property.ofValue(Map.of("tenant", quote, "id", 7)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        assertEquals(injected, query.run(context(query)).getRow().get("value"));

        Queries queries = Queries.builder()
            .id("queries").type(Queries.class.getName()).connection(connection())
            .cql(Property.ofValue(List.of(
                "SELECT value FROM events WHERE tenant = :tenant AND id = :id",
                "SELECT id FROM events WHERE tenant = :tenant"
            )))
            .parameters(Property.ofValue(Map.of("tenant", quote, "id", 7)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        List<Query.Output> outputs = queries.run(context(queries)).getOutputs();
        assertEquals(1L, outputs.get(0).getSize());
        assertEquals(injected, outputs.get(0).getRows().getFirst().get("value"));
        assertEquals(1L, outputs.get(1).getSize());

        Batch batch = Batch.builder()
            .id("batch").type(Batch.class.getName()).connection(connection())
            .cql(Property.ofValue(List.of(
                "INSERT INTO events (tenant, id, value) VALUES (:tenant, :id, :value)"
            )))
            .parameters(Property.ofValue(Map.of("tenant", quote, "id", 8, "value", "second")))
            .build();
        assertTrue(batch.run(context(batch)).isWasApplied());

        Execute missing = Execute.builder()
            .id("missing").type(Execute.class.getName()).connection(connection())
            .cql(Property.ofValue("INSERT INTO events (tenant, id, value) VALUES (:tenant, :id, :value)"))
            .parameters(Property.ofValue(Map.of("tenant", quote)))
            .build();
        assertThrows(IllegalArgumentException.class, () -> missing.run(context(missing)));

        Execute anonymous = Execute.builder()
            .id("anonymous").type(Execute.class.getName()).connection(connection())
            .cql(Property.ofValue("INSERT INTO events (tenant, id, value) VALUES (?, ?, ?)"))
            .parameters(Property.ofValue(Map.of("tenant", quote, "id", 9, "value", "nope")))
            .build();
        assertThrows(IllegalArgumentException.class, () -> anonymous.run(context(anonymous)));

        var count = admin.prepare("SELECT count(*) FROM " + KEYSPACE + ".events WHERE tenant = ?");
        assertEquals(2L, admin.execute(count.bind(quote)).one().getLong(0));
    }

    @Test
    void batchSupportsLoggedAndUnloggedAndDynamicYaml() throws Exception {
        for (Batch.BatchType type : List.of(Batch.BatchType.LOGGED, Batch.BatchType.UNLOGGED)) {
            Batch batch = yamlTask(Batch.class, """
                batchType: "{{ inputs.batchType }}"
                cql:
                  - "INSERT INTO events (tenant, id, value) VALUES ('{{ inputs.tenant }}', 1, 'one')"
                  - "INSERT INTO events (tenant, id, value) VALUES ('{{ inputs.tenant }}', 2, 'two')"
                """);
            Batch.Output output = batch.run(context(batch, Map.of("batchType", type.name(), "tenant", type.name())));
            assertTrue(output.isWasApplied());
            assertEquals(2, output.getStatements());
            var rows = admin.execute("SELECT id, value FROM " + KEYSPACE + ".events WHERE tenant = '" + type.name() + "'").all();
            assertEquals(2, rows.size());
            assertEquals("one", rows.getFirst().getString("value"));
            assertEquals("two", rows.getLast().getString("value"));
        }
    }

    @Test
    void counterBatchActuallyIncrementsCounters() throws Exception {
        Batch batch = Batch.builder()
            .id("counter")
            .type(Batch.class.getName())
            .connection(connection())
            .batchType(Property.ofValue(Batch.BatchType.COUNTER))
            .cql(Property.ofValue(List.of(
                "UPDATE counters SET total = total + 2 WHERE tenant = 'test' AND id = 1",
                "UPDATE counters SET total = total + 3 WHERE tenant = 'test' AND id = 1"
            )))
            .build();
        Batch.Output output = batch.run(context(batch));
        assertTrue(output.isWasApplied());
        assertEquals(2, output.getStatements());
        assertEquals(5L, admin.execute("SELECT total FROM " + KEYSPACE
            + ".counters WHERE tenant = 'test' AND id = 1").one().getLong("total"));
    }

    @Test
    void conditionalBatchReportsNotAppliedWithoutOverwriting() throws Exception {
        Batch batch = Batch.builder()
            .id("conditional")
            .type(Batch.class.getName())
            .connection(connection())
            .batchType(Property.ofValue(Batch.BatchType.LOGGED))
            .cql(Property.ofValue(List.of(
                "INSERT INTO events (tenant, id, value) VALUES ('test', 1, 'first') IF NOT EXISTS",
                "INSERT INTO events (tenant, id, value) VALUES ('test', 2, 'second') IF NOT EXISTS"
            )))
            .build();
        assertTrue(batch.run(context(batch)).isWasApplied());
        assertFalse(batch.run(context(batch)).isWasApplied());
        assertEquals(2, admin.execute("SELECT * FROM " + KEYSPACE + ".events WHERE tenant = 'test'").all().size());
    }

    @Test
    void triggerAcknowledgesQuotedKeysAndPollsOnlyNewRows() throws Exception {
        insertPending("first");
        insertPending("O'Reilly");
        insertPending("x'; DELETE FROM pending; --");
        Trigger trigger = trigger(FetchType.FETCH);
        RunContext context = triggerContext(trigger);
        Query.Output first = trigger.poll(context);
        assertEquals(3L, first.getSize());
        assertTrue(first.getRows().stream().anyMatch(row -> "O'Reilly".equals(row.get("event_id"))));
        assertEquals(0L, trigger.poll(context).getSize());

        insertPending("new-arrival");
        Query.Output next = trigger.poll(context);
        assertEquals(1L, next.getSize());
        assertEquals("new-arrival", next.getRows().getFirst().get("event_id"));
        assertEquals(0L, trigger.poll(context).getSize());
    }

    @Test
    void triggerFetchOneAcknowledgesOnlyTheEmittedRow() throws Exception {
        insertPending("a");
        insertPending("b");
        Trigger trigger = trigger(FetchType.FETCH_ONE);
        RunContext context = triggerContext(trigger);
        Query.Output first = trigger.poll(context);
        assertEquals(1L, first.getSize());
        assertEquals("a", first.getRow().get("event_id"));
        Query.Output second = trigger.poll(context);
        assertEquals(1L, second.getSize());
        assertEquals("b", second.getRow().get("event_id"));
        assertEquals(0L, trigger.poll(context).getSize());
    }

    @Test
    void triggerStoreAcknowledgesEveryPageAndKeepsReadableOutput() throws Exception {
        for (int i = 0; i < 7; i++) {
            insertPending("event-" + i);
        }
        Trigger trigger = trigger(FetchType.STORE);
        RunContext context = triggerContext(trigger);
        Query.Output output = trigger.poll(context);
        assertEquals(7L, output.getSize());
        List<Map<String, Object>> rows = readStored(context, output.getUri());
        assertEquals(7, rows.size());
        assertEquals("event-0", rows.getFirst().get("event_id"));
        assertEquals("event-6", rows.getLast().get("event_id"));
        assertEquals(0L, trigger.poll(context).getSize());
        // Acknowledgements must not mutate the already persisted event payload.
        assertEquals(7, readStored(context, output.getUri()).size());
    }

    @Test
    void triggerDefaultIntervalAndEvaluationEmitOnlyForNonEmptyResults() throws Exception {
        Trigger trigger = trigger(FetchType.FETCH);
        assertNotNull(trigger.getInterval());
        assertFalse(trigger.getInterval().isZero());
        assertFalse(trigger.getInterval().isNegative());
        var scheduler = TestsUtils.mockTrigger(runContextFactory, trigger);
        assertTrue(trigger.evaluate(scheduler.getKey(), scheduler.getValue()).isEmpty());
        insertPending("scheduled");
        assertTrue(trigger.evaluate(scheduler.getKey(), scheduler.getValue()).isPresent());
        assertTrue(trigger.evaluate(scheduler.getKey(), scheduler.getValue()).isEmpty());
    }

    @Test
    void triggerRejectsMissingKeyWithoutAdvancingRows() throws Exception {
        insertPending("a");
        insertPending("b");
        Trigger trigger = Trigger.builder()
            .id("missing_key").type(Trigger.class.getName()).connection(connection())
            .cql(Property.ofValue("SELECT bucket, value FROM pending WHERE bucket = 'test'"))
            .acknowledgeCql(Property.ofValue("DELETE FROM pending WHERE bucket = :bucket AND event_id = :event_id"))
            .build();
        assertThrows(IllegalArgumentException.class, () -> trigger.poll(triggerContext(trigger)));
        assertEquals(2L, admin.execute("SELECT count(*) FROM " + KEYSPACE + ".pending").one().getLong(0));
    }

    @Test
    void triggerBindsUuidKeysAndMarksRowsProcessed() throws Exception {
        admin.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE
            + ".uuid_events (id uuid PRIMARY KEY, processed boolean)");
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        var insert = admin.prepare("INSERT INTO " + KEYSPACE + ".uuid_events (id, processed) VALUES (?, false)");
        admin.execute(insert.bind(first));
        admin.execute(insert.bind(second));
        Trigger trigger = Trigger.builder()
            .id("uuid_events").type(Trigger.class.getName()).connection(connection())
            .cql(Property.ofValue("SELECT id FROM uuid_events WHERE processed = false ALLOW FILTERING"))
            .acknowledgeCql(Property.ofValue("UPDATE uuid_events SET processed = true WHERE id = :id"))
            .fetchType(Property.ofValue(FetchType.STORE)).fetchSize(Property.ofValue(1)).build();
        RunContext context = triggerContext(trigger);
        assertEquals(2L, trigger.poll(context).getSize());
        assertEquals(0L, trigger.poll(context).getSize());
        assertTrue(admin.execute("SELECT processed FROM " + KEYSPACE + ".uuid_events WHERE id = " + first)
            .one().getBoolean("processed"));
    }

    private ScyllaDbConnection connection() {
        return ScyllaDbConnection.builder()
            .contactPoints(Property.ofValue(List.of(contactPoint())))
            .localDatacenter(Property.ofValue("datacenter1"))
            .keyspace(Property.ofValue(KEYSPACE))
            .build();
    }

    private Query query(String cql, FetchType fetchType) {
        return Query.builder()
            .id("query")
            .type(Query.class.getName())
            .connection(connection())
            .cql(Property.ofValue(cql))
            .fetchType(Property.ofValue(fetchType))
            .fetchSize(Property.ofValue(2))
            .build();
    }

    private Execute execute(String cql) {
        return Execute.builder()
            .id("execute")
            .type(Execute.class.getName())
            .connection(connection())
            .cql(Property.ofValue(cql))
            .build();
    }

    private Trigger trigger(FetchType fetchType) {
        return Trigger.builder()
            .id("pending")
            .type(Trigger.class.getName())
            .connection(connection())
            .cql(Property.ofValue("SELECT bucket, event_id, value FROM pending WHERE bucket = 'test'"))
            .acknowledgeCql(Property.ofValue("DELETE FROM pending WHERE bucket = :bucket AND event_id = :event_id"))
            .fetchType(Property.ofValue(fetchType))
            .fetchSize(Property.ofValue(2))
            .build();
    }

    private RunContext triggerContext(Trigger trigger) {
        return TestsUtils.mockTrigger(runContextFactory, trigger).getKey().getRunContext();
    }

    private RunContext context(Task task) {
        return context(task, Map.of());
    }

    private RunContext context(Task task, Map<String, Object> extraInputs) {
        var inputs = new java.util.HashMap<String, Object>(extraInputs);
        inputs.put("contactPoints", List.of(contactPoint()));
        inputs.put("datacenter", "datacenter1");
        inputs.put("keyspace", KEYSPACE);
        return TestsUtils.mockRunContext(runContextFactory, task, inputs);
    }

    private <T extends Task> T yamlTask(Class<T> type, String body) throws Exception {
        return JacksonMapper.ofYaml().readValue("""
            id: integration
            type: %s
            connection:
              contactPoints: "{{ inputs.contactPoints }}"
              localDatacenter: "{{ inputs.datacenter }}"
              keyspace: "{{ inputs.keyspace }}"
            %s
            """.formatted(type.getName(), body), type);
    }

    private void seedEvents(int count) {
        var statement = admin.prepare("INSERT INTO " + KEYSPACE + ".events (tenant, id, value) VALUES (?, ?, ?)");
        for (int i = 0; i < count; i++) {
            admin.execute(statement.bind("test", i, "value-" + i));
        }
    }

    private void insertPending(String id) {
        var statement = admin.prepare("INSERT INTO " + KEYSPACE + ".pending (bucket, event_id, value) VALUES (?, ?, ?)");
        admin.execute(statement.bind("test", id, "payload"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readStored(RunContext context, URI uri) throws Exception {
        assertNotNull(uri);
        List<Map<String, Object>> rows = new ArrayList<>();
        try (var input = context.storage().getFile(uri)) {
            FileSerde.read(input, row -> rows.add((Map<String, Object>) row));
        }
        return rows;
    }
}
