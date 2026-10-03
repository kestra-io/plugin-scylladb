package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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

import java.util.List;

@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a batch of CQL mutations",
    description = "Execute a bounded list of INSERT, UPDATE, or DELETE statements as a native CQL batch. Prefer small same-partition batches; batches are not a bulk-loading mechanism. COUNTER batches accept only counter mutations."
)
@Plugin(examples = {
    @Example(
        title = "Create a table and write a logged batch",
        full = true,
        code = """
            id: scylladb_batch
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
              - id: create_table
                type: io.kestra.plugin.scylladb.Execute
                connection:
                  contactPoints:
                    - "{{ secret('SCYLLADB_CONTACT_POINT') }}"
                  localDatacenter: "{{ secret('SCYLLADB_DATACENTER') }}"
                  username: "{{ secret('SCYLLADB_USERNAME') }}"
                  password: "{{ secret('SCYLLADB_PASSWORD') }}"
                  tlsEnabled: true
                  keyspace: kestra
                cql: CREATE TABLE IF NOT EXISTS events (tenant text, id int, value text, PRIMARY KEY (tenant, id))
              - id: write_events
                type: io.kestra.plugin.scylladb.Batch
                connection:
                  contactPoints:
                    - "{{ secret('SCYLLADB_CONTACT_POINT') }}"
                  localDatacenter: "{{ secret('SCYLLADB_DATACENTER') }}"
                  username: "{{ secret('SCYLLADB_USERNAME') }}"
                  password: "{{ secret('SCYLLADB_PASSWORD') }}"
                  tlsEnabled: true
                  keyspace: kestra
                batchType: LOGGED
                cql:
                  - "INSERT INTO events (tenant, id, value) VALUES ('demo', 1, 'first')"
                  - "INSERT INTO events (tenant, id, value) VALUES ('demo', 2, 'second')"
            """
    )
})
public class Batch extends AbstractScyllaDbTask implements RunnableTask<Batch.Output> {
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main")
    @Schema(title = "CQL statements", description = "Non-empty list of non-blank CQL mutations, with at most 65535 statements. Each entry is one statement, not a BEGIN BATCH block. Do not interpolate untrusted values.")
    private Property<List<String>> cql;

    @Builder.Default
    @NotNull
    @PluginProperty(group = "processing")
    @Schema(title = "Batch type", description = "LOGGED uses the batch log; UNLOGGED omits it; COUNTER is required for counter mutations. Defaults to LOGGED.", defaultValue = "LOGGED")
    private Property<BatchType> batchType = Property.ofValue(BatchType.LOGGED);

    public enum BatchType {
        LOGGED,
        UNLOGGED,
        COUNTER
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCql = runContext.render(cql).asList(String.class);
        if (rCql == null || rCql.isEmpty() || rCql.size() > 65535) {
            throw new IllegalArgumentException("cql must contain between 1 and 65535 statements");
        }
        var rBatchType = runContext.render(batchType).as(BatchType.class).orElse(BatchType.LOGGED);
        var batch = BatchStatement.builder(DefaultBatchType.valueOf(rBatchType.name()));
        for (String statement : rCql) {
            batch.addStatement(SimpleStatement.newInstance(ScyllaDbConnection.requireNonBlank(statement, "cql entry")));
        }
        try (var session = connect(runContext)) {
            return Output.builder()
                .wasApplied(session.execute(batch.build()).wasApplied())
                .statements(rCql.size())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Was applied", description = "For a conditional batch, whether its conditions were satisfied. True for successful non-conditional batches.")
        private final boolean wasApplied;

        @Schema(title = "Statement count", description = "Number of statements submitted in the batch, not the number of affected rows.")
        private final int statements;
    }
}
