package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
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
public abstract class AbstractScyllaDbTask extends Task {
    @NotNull
    @Valid
    @PluginProperty(group = "connection")
    @Schema(title = "ScyllaDB connection", description = "Native CQL connection settings, including contact points, local datacenter, authentication, and optional TLS.")
    protected ScyllaDbConnection connection;

    protected CqlSession connect(RunContext runContext) throws Exception {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        return connection.connect(runContext);
    }
}
