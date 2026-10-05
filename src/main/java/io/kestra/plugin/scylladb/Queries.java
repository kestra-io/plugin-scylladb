package io.kestra.plugin.scylladb;

import java.util.ArrayList;
import java.util.List;

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
    title = "Execute SELECT statements sequentially in ScyllaDB",
    description = "Runs SELECT statements in their supplied order using one session, returning one output per statement. "
        + "Queries do not share a transactional snapshot."
)
@Plugin(
    examples = {
        @Example(
            title = "Read cluster and data center information in order",
            full = true,
            code = """
                id: scylladb_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.scylladb.Queries
                    connection:
                      contactPoints:
                        - "{{ secret('SCYLLADB_HOST') }}:9042"
                      localDatacenter: datacenter1
                      username: "{{ secret('SCYLLADB_USERNAME') }}"
                      password: "{{ secret('SCYLLADB_PASSWORD') }}"
                    cql:
                      - SELECT cluster_name FROM system.local
                      - SELECT data_center FROM system.local
                    fetchType: FETCH_ONE
                    fetchSize: 100
                """
        )
    }
)
public class Queries extends AbstractScyllaDbTask implements RunnableTask<Queries.Output> {
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(
        title = "Ordered CQL statements",
        description = "A non-empty list of non-blank SELECT statements. All statements are rendered and validated "
            + "before the first is executed. Named :name markers bind the shared parameters map."
    )
    private Property<List<String>> cql;

    @Builder.Default
    @PluginProperty(group = "processing")
    @Schema(
        title = "Result fetch mode",
        description = "Applied to every statement: FETCH returns all rows, FETCH_ONE returns at most one, "
            + "STORE streams rows to a separate Ion file per statement.",
        defaultValue = "FETCH"
    )
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Driver page size",
        description = "Optional positive page size applied to every statement. This does not limit the total "
            + "number of rows; FETCH and STORE consume all pages.",
        minimum = "1"
    )
    private Property<Integer> fetchSize;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCql = runContext.render(cql).asList(String.class);
        if (rCql == null || rCql.isEmpty()) {
            throw new IllegalArgumentException("cql must contain at least one statement");
        }
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        var rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(null);
        var rParameters = renderParameters(runContext);
        rCql.forEach(statement -> QueryService.validate(statement, rFetchSize));

        var outputs = new ArrayList<Query.Output>(rCql.size());
        try (CqlSession session = connect(runContext)) {
            for (String statement : rCql) {
                outputs.add(QueryService.fetch(session, runContext, statement, rFetchType, rFetchSize, rParameters).output());
            }
        }
        return Output.builder().outputs(outputs).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Ordered statement results",
            description = "One Query output per CQL statement, in the same order as the input statements."
        )
        private final List<Query.Output> outputs;
    }
}
