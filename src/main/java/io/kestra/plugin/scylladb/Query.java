package io.kestra.plugin.scylladb;

import java.net.URI;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a CQL query in ScyllaDB",
    description = "Executes one SELECT statement. FETCH loads all result pages into memory; use STORE for large results. "
        + "UUIDs and temporal values are strings, blobs are base64 strings, tuples are lists, and UDTs are maps. "
        + "CQL maps with non-string keys are lists of key/value objects to preserve key types."
)
@Plugin(
    examples = {
        @Example(
            title = "Store ScyllaDB node information",
            full = true,
            code = """
                id: scylladb_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.scylladb.Query
                    connection:
                      contactPoints:
                        - "{{ secret('SCYLLADB_HOST') }}:9042"
                      localDatacenter: datacenter1
                      username: "{{ secret('SCYLLADB_USERNAME') }}"
                      password: "{{ secret('SCYLLADB_PASSWORD') }}"
                    cql: SELECT * FROM system.local
                    fetchType: STORE
                    fetchSize: 1000
                """
        )
    }
)
public class Query extends AbstractScyllaDbTask implements RunnableTask<Query.Output> {
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(title = "CQL statement", description = "One non-blank SELECT CQL statement, rendered before execution. Do not interpolate untrusted values.")
    private Property<String> cql;

    @Builder.Default
    @PluginProperty(group = "processing")
    @Schema(
        title = "Result fetch mode",
        description = "FETCH returns every row, FETCH_ONE returns at most one row, STORE streams every row to "
            + "an Ion file in internal storage.",
        defaultValue = "FETCH"
    )
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Driver page size",
        description = "Optional positive number of rows requested per page. This is not a result limit; "
            + "FETCH and STORE consume all pages. When omitted, the driver default is used.",
        minimum = "1"
    )
    private Property<Integer> fetchSize;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCql = runContext.render(cql).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("cql is required"));
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        var rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(null);
        QueryService.validate(rCql, rFetchSize);

        try (CqlSession session = connect(runContext)) {
            return QueryService.fetch(session, runContext, rCql, rFetchType, rFetchSize).output();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "First fetched row", description = "Populated for FETCH_ONE; null when the result is empty.")
        private final Map<String, Object> row;

        @Schema(title = "All fetched rows", description = "Populated for FETCH in result order, including all pages.")
        private final List<Map<String, Object>> rows;

        @Schema(title = "Stored result URI", description = "Internal storage URI of the Ion file, populated for STORE.")
        private final URI uri;

        @Schema(
            title = "Number of emitted rows",
            description = "Zero or one for FETCH_ONE, or the total row count for FETCH and STORE."
        )
        private final Long size;
    }
}
