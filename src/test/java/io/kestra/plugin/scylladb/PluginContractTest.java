package io.kestra.plugin.scylladb;

import java.util.List;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PluginContractTest {
    @Test
    void examplesAreCompleteYamlFlows() throws Exception {
        for (Class<?> type : List.of(Query.class, Queries.class, Execute.class, Batch.class, Trigger.class)) {
            var plugin = type.getAnnotation(Plugin.class);
            assertNotNull(plugin, type.getSimpleName());
            assertTrue(plugin.examples().length > 0, type.getSimpleName());
            for (var example : plugin.examples()) {
                assertTrue(example.full(), type.getSimpleName());
                var flow = JacksonMapper.ofYaml().readTree(String.join("\n", example.code()));
                assertTrue(flow.hasNonNull("id"), type.getSimpleName());
                assertTrue(flow.hasNonNull("namespace"), type.getSimpleName());
                assertTrue(flow.path("tasks").isArray(), type.getSimpleName());
                if (type == Trigger.class) {
                    assertTrue(flow.path("triggers").isArray());
                    assertTrue(flow.path("triggers").get(0).hasNonNull("acknowledgeCql"));
                }
            }
        }
    }

    @Test
    void everyConfigurationAndOutputFieldHasSchema() {
        for (Class<?> type : List.of(
            ScyllaDbConnection.class, AbstractScyllaDbTask.class, Query.class, Queries.class,
            Execute.class, Batch.class, Trigger.class, Query.Output.class, Queries.Output.class,
            Execute.Output.class, Batch.Output.class
        )) {
            for (var field : type.getDeclaredFields()) {
                if (!field.isSynthetic() && !java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    assertNotNull(field.getAnnotation(Schema.class), type.getSimpleName() + "." + field.getName());
                }
            }
        }
    }
}
