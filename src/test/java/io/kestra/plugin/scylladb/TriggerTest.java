package io.kestra.plugin.scylladb;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.WorkingDir;
import io.kestra.core.storages.Storage;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@MicronautTest
class TriggerTest {
    @TempDir
    java.nio.file.Path directory;

    @Inject
    RunContextFactory contexts;

    @Test
    void validatesEveryBindingBeforeAnyAcknowledgement() throws Exception {
        Fixture fixture = fixture(List.of(Map.of("id", "a"), Map.of("other", "b")));
        assertThrows(IllegalArgumentException.class, () -> fixture.trigger.poll(fixture.context));
        verify(fixture.session, never()).execute(any(BoundStatement.class));
        verify(fixture.session).close();
    }

    @Test
    void stopsOnPartialAcknowledgementFailure() throws Exception {
        Fixture fixture = fixture(List.of(Map.of("id", "a"), Map.of("id", "b"), Map.of("id", "c")));
        ResultSet applied = mock(ResultSet.class);
        when(applied.wasApplied()).thenReturn(true);
        when(fixture.session.execute(any(BoundStatement.class)))
            .thenReturn(applied).thenThrow(new IllegalStateException("acknowledgement failed"));

        assertThrows(IllegalStateException.class, () -> fixture.trigger.poll(fixture.context));
        verify(fixture.session, times(2)).execute(any(BoundStatement.class));
        verify(fixture.session).close();
    }

    @Test
    void doesNotEmitWhenConditionalAcknowledgementIsNotApplied() throws Exception {
        Fixture fixture = fixture(List.of(Map.of("id", "a")));
        ResultSet notApplied = mock(ResultSet.class);
        when(fixture.session.execute(any(BoundStatement.class))).thenReturn(notApplied);
        assertThrows(IllegalStateException.class, () -> fixture.trigger.poll(fixture.context));
        verify(fixture.session).close();
    }

    @Test
    void fetchFailureDoesNotAcknowledge() throws Exception {
        Fixture fixture = fixture(List.of(Map.of("id", "a")));
        when(fixture.session.execute(any(SimpleStatement.class))).thenThrow(new IllegalStateException("query failed"));
        assertThrows(IllegalStateException.class, () -> fixture.trigger.poll(fixture.context));
        verify(fixture.session, never()).execute(any(BoundStatement.class));
        verify(fixture.session).close();
    }

    @Test
    void uploadFailureDoesNotAcknowledge() throws Exception {
        Fixture fixture = fixture(List.of(Map.of("id", "a")));
        RunContext context = spy(fixture.context);
        var workingDir = mock(WorkingDir.class);
        var storage = mock(Storage.class);
        doReturn(workingDir).when(context).workingDir();
        doReturn(storage).when(context).storage();
        when(workingDir.createTempFile(".ion")).thenReturn(directory.resolve("rows.ion"));
        when(storage.putFile(any(java.io.File.class))).thenThrow(new java.io.IOException("upload failed"));
        var connection = mock(ScyllaDbConnection.class);
        when(connection.connect(context)).thenReturn(fixture.session);
        Trigger trigger = Trigger.builder().connection(connection)
            .cql(Property.ofValue("SELECT id FROM events"))
            .acknowledgeCql(Property.ofValue("DELETE FROM events WHERE id = :id"))
            .fetchType(Property.ofValue(FetchType.STORE)).build();

        assertThrows(java.io.IOException.class, () -> trigger.poll(context));
        verify(fixture.session, never()).execute(any(BoundStatement.class));
        verify(fixture.session).close();
    }

    @Test
    void emptyPollDoesNotAcknowledge() throws Exception {
        Fixture fixture = fixture(List.of());
        assertEquals(0L, fixture.trigger.poll(fixture.context).getSize());
        verify(fixture.session, never()).execute(any(BoundStatement.class));
        verify(fixture.session).close();
    }

    @Test
    void rejectsNoneBeforeConnecting() throws Exception {
        ScyllaDbConnection connection = mock(ScyllaDbConnection.class);
        Trigger trigger = Trigger.builder().connection(connection)
            .cql(Property.ofValue("SELECT id FROM events"))
            .acknowledgeCql(Property.ofValue("DELETE FROM events WHERE id = :id"))
            .fetchType(Property.ofValue(FetchType.NONE)).build();
        assertThrows(IllegalArgumentException.class, () -> trigger.poll(contexts.of()));
        verifyNoInteractions(connection);
    }

    private Fixture fixture(List<Map<String, Object>> values) throws Exception {
        RunContext context = contexts.of();
        ScyllaDbConnection connection = mock(ScyllaDbConnection.class);
        CqlSession session = mock(CqlSession.class);
        when(connection.connect(context)).thenReturn(session);
        DriverContext driverContext = mock(DriverContext.class);
        when(session.getContext()).thenReturn(driverContext);
        when(driverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
        PreparedStatement prepared = mock(PreparedStatement.class);
        when(session.prepare(anyString())).thenReturn(prepared);
        ColumnDefinitions variables = columns("id");
        when(prepared.getVariableDefinitions()).thenReturn(variables);
        BoundStatementBuilder builder = mock(BoundStatementBuilder.class, RETURNS_SELF);
        when(prepared.boundStatementBuilder()).thenReturn(builder);
        BoundStatement bound = mock(BoundStatement.class);
        when(builder.build()).thenReturn(bound);
        List<Row> rows = values.stream().map(value -> {
            Row row = mock(Row.class);
            String key = value.keySet().iterator().next();
            ColumnDefinitions definitions = columns(key);
            when(row.getColumnDefinitions()).thenReturn(definitions);
            when(row.getObject(0)).thenReturn(value.get(key));
            return row;
        }).toList();
        ResultSet result = mock(ResultSet.class);
        when(result.iterator()).thenAnswer(ignored -> rows.iterator());
        when(session.execute(any(SimpleStatement.class))).thenReturn(result);
        Trigger trigger = Trigger.builder().connection(connection)
            .cql(Property.ofValue("SELECT id FROM events"))
            .acknowledgeCql(Property.ofValue("DELETE FROM events WHERE id = :id")).build();
        return new Fixture(trigger, context, session);
    }

    private ColumnDefinitions columns(String name) {
        ColumnDefinition column = mock(ColumnDefinition.class);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal(name));
        when(column.getType()).thenReturn(DataTypes.TEXT);
        ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        return columns;
    }

    private record Fixture(Trigger trigger, RunContext context, CqlSession session) {
    }
}
