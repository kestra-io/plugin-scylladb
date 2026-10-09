package io.kestra.plugin.scylladb;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll ScyllaDB and acknowledge returned rows",
    description = "Starts an execution when a SELECT returns rows, after applying acknowledgeCql to each emitted row. "
        + "Acknowledgement happens before flow processing and is not atomic with execution delivery. Partial failures "
        + "can lose events and concurrent consumers can duplicate them; neither exactly-once nor guaranteed "
        + "at-least-once delivery is provided. Use idempotent acknowledgements and a single consumer."
)
@Plugin(examples = @Example(
    title = "Consume pending events",
    full = true,
    code = """
        id: scylladb_pending_events
        namespace: company.team
        triggers:
          - id: events
            type: io.kestra.plugin.scylladb.Trigger
            interval: PT1M
            connection:
              contactPoints:
                - "{{ secret('SCYLLADB_HOST') }}:9042"
              localDatacenter: datacenter1
              keyspace: events
              username: "{{ secret('SCYLLADB_USERNAME') }}"
              password: "{{ secret('SCYLLADB_PASSWORD') }}"
            # Requires pending_events(event_id uuid PRIMARY KEY, event_type text, payload text).
            cql: SELECT event_id, event_type, payload FROM pending_events LIMIT 100
            acknowledgeCql: DELETE FROM pending_events WHERE event_id = :event_id
            fetchType: FETCH
        tasks:
          - id: handle_events
            type: io.kestra.plugin.core.log.Log
            message: "Processing {{ trigger.size }} events: {{ trigger.rows }}"
        """
))
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output> {
    @NotNull
    @Valid
    @PluginProperty(group = "connection")
    @Schema(title = "ScyllaDB connection")
    private ScyllaDbConnection connection;

    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(title = "SELECT statement", description = "Select pending rows and every primary-key column needed by acknowledgeCql. Use LIMIT to bound each poll.")
    private Property<String> cql;

    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(
        title = "Per-row acknowledgement CQL",
        description = "An idempotent UPDATE or DELETE targeting the returned row's complete primary key. Named "
            + "markers bind to selected column names, for example DELETE FROM pending_events WHERE event_id = :event_id. "
            + "Do not use anonymous markers, counters, or broad updates. The mutation must remove the row from "
            + "the SELECT's future results. Values are bound, not interpolated into CQL."
    )
    private Property<String> acknowledgeCql;

    @Builder.Default
    @PluginProperty(group = "processing")
    @Schema(title = "Fetch mode", description = "FETCH, FETCH_ONE, or STORE. Only emitted rows are acknowledged.", defaultValue = "FETCH")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @PluginProperty(group = "advanced")
    @Schema(title = "Driver page size", description = "Optional positive page-size hint, not a total result limit.", minimum = "1")
    private Property<Integer> fetchSize;

    @Builder.Default
    @NotNull
    @PluginProperty(group = "execution")
    @Schema(title = "Polling interval", description = "Time between polls as an ISO-8601 duration.", defaultValue = "PT1M")
    private Duration interval = Duration.ofMinutes(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var output = poll(conditionContext.getRunContext());
        return output.getSize() == 0
            ? Optional.empty()
            : Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    Query.Output poll(RunContext runContext) throws Exception {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be strictly positive");
        }
        var rCql = runContext.render(cql).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("cql is required"));
        var rAcknowledgeCql = runContext.render(acknowledgeCql).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("acknowledgeCql is required"));
        if (!rAcknowledgeCql.stripLeading().matches("(?is)^(UPDATE|DELETE)\\s+.*")) {
            throw new IllegalArgumentException("acknowledgeCql must be an UPDATE or DELETE");
        }
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (rFetchType == FetchType.NONE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE, or STORE");
        }
        var rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(null);
        QueryService.validate(rCql, rFetchSize);
        try (CqlSession session = connection.connect(runContext)) {
            var prepared = session.prepare(rAcknowledgeCql);
            if (prepared.getVariableDefinitions().size() == 0) {
                throw new IllegalArgumentException("acknowledgeCql must bind the selected row's key columns");
            }
            var result = QueryService.fetch(session, runContext, rCql, rFetchType, rFetchSize);
            if (result.output().getSize() == 0) {
                return result.output();
            }
            forEachRow(runContext, result, row -> bind(session, prepared, row));
            forEachRow(runContext, result, row -> {
                if (!session.execute(bind(session, prepared, row)).wasApplied()) {
                    throw new IllegalStateException("Conditional row acknowledgement was not applied");
                }
            });
            runContext.logger().debug("Acknowledged {} ScyllaDB rows", result.output().getSize());
            return result.output();
        }
    }

    private static BoundStatement bind(CqlSession session, PreparedStatement prepared, Map<String, Object> row) {
        var builder = prepared.boundStatementBuilder();
        var codecs = session.getContext().getCodecRegistry();
        var variables = prepared.getVariableDefinitions();
        for (int i = 0; i < variables.size(); i++) {
            var variable = variables.get(i);
            var name = variable.getName().asInternal();
            if (!row.containsKey(name) || row.get(name) == null) {
                throw new IllegalArgumentException("SELECT must include a non-null acknowledgement column: " + name);
            }
            var value = CqlValues.fromStorage(row.get(name), variable.getType(), codecs);
            builder.set(i, value, codecs.<Object>codecFor(variable.getType()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static void forEachRow(RunContext runContext, QueryService.FetchedResult result, Consumer<Map<String, Object>> consumer) throws Exception {
        if (result.output().getUri() != null) {
            // putFile may consume the temp file; replay via the uploaded URI.
            try (var input = runContext.storage().getFile(result.output().getUri())) {
                FileSerde.read(input, value -> consumer.accept((Map<String, Object>) value));
            }
        } else if (result.output().getRows() != null) {
            result.output().getRows().forEach(consumer);
        } else if (result.output().getRow() != null) {
            consumer.accept(result.output().getRow());
        }
    }
}
