package io.kestra.plugin.scylladb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
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
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a CQL mutation or DDL statement",
    description = "Execute one INSERT, UPDATE, DELETE, or schema statement using the native ScyllaDB driver. Conditional writes expose wasApplied; CQL does not provide an affected-row count."
)
@Plugin(examples = {
    @Example(
        title = "Create a ScyllaDB keyspace",
        full = true,
        code = """
            id: scylladb_create_keyspace
            namespace: company.team
            tasks:
              - id: create_keyspace
                type: io.kestra.plugin.scylladb.Execute
                connection:
                  contactPoints:
                    - "{{ secret('SCYLLADB_CONTACT_POINT') }}"
                  localDatacenter: "{{ secret('SCYLLADB_DATACENTER') }}"
                  username: "{{ secret('SCYLLADB_USERNAME') }}"
                  password: "{{ secret('SCYLLADB_PASSWORD') }}"
                  tlsEnabled: true
                cql: >-
                  CREATE KEYSPACE IF NOT EXISTS kestra
                  WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
            """
    )
})
public class Execute extends AbstractScyllaDbTask implements RunnableTask<Execute.Output> {
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(title = "CQL statement", description = "One non-blank INSERT, UPDATE, DELETE, or DDL statement. Bind dynamic values with named :name markers and parameters instead of interpolating them into the statement.")
    private Property<String> cql;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCql = ScyllaDbConnection.requireNonBlank(
            runContext.render(cql).as(String.class).orElse(null), "cql"
        );
        var rParameters = renderParameters(runContext);
        try (var session = connect(runContext)) {
            return Output.builder()
                .wasApplied(session.execute(QueryService.statement(session, rCql, rParameters, null)).wasApplied())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Was applied", description = "For conditional writes, whether the condition was satisfied. True for successful non-conditional statements.")
        private final boolean wasApplied;
    }
}
