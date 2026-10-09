package io.kestra.plugin.scylladb;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
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

    @PluginProperty(group = "main")
    @Schema(
        title = "Bound parameters",
        description = "Values for named :name markers in cql. Each value is rendered, then bound to a prepared "
            + "statement using that marker's CQL type. Use this for flow inputs and upstream outputs instead of "
            + "interpolating them into the statement text. Keys that are not markers of a statement are ignored."
    )
    protected Property<Map<String, Object>> parameters;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile CqlSession activeSession;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile boolean killed;

    protected CqlSession connect(RunContext runContext) throws Exception {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        activeSession = connection.connect(runContext);
        if (killed) {
            activeSession.close();
            throw new IllegalStateException("Task was killed");
        }
        return activeSession;
    }

    protected Map<String, Object> renderParameters(RunContext runContext) throws Exception {
        if (parameters == null) {
            return Map.of();
        }
        Map<String, Object> rendered = runContext.render(parameters).asMap(String.class, Object.class);
        return rendered == null ? Map.of() : rendered;
    }

    public void kill() {
        killed = true;
        var session = activeSession;
        if (session != null) {
            session.closeAsync();
        }
    }
}
