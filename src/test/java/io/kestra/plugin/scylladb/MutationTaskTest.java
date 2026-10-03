package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@MicronautTest
class MutationTaskTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void executeRendersCqlReturnsConditionalResultAndClosesSession() throws Exception {
        var context = runContextFactory.of(Map.of("statement", "UPDATE events SET value = 'new' WHERE id = 1 IF value = 'old'"));
        var connection = mock(ScyllaDbConnection.class);
        var session = mock(CqlSession.class);
        var resultSet = mock(ResultSet.class);
        when(connection.connect(context)).thenReturn(session);
        when(session.execute("UPDATE events SET value = 'new' WHERE id = 1 IF value = 'old'")).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(false);

        var output = Execute.builder().connection(connection)
            .cql(Property.ofExpression("{{ statement }}")).build().run(context);

        assertFalse(output.isWasApplied());
        assertNull(output.getAffectedRows());
        verify(session).close();
    }

    @Test
    void executeClosesSessionOnFailure() throws Exception {
        var context = runContextFactory.of();
        var connection = mock(ScyllaDbConnection.class);
        var session = mock(CqlSession.class);
        when(connection.connect(context)).thenReturn(session);
        when(session.execute("invalid")).thenThrow(new IllegalStateException("server rejected statement"));
        var task = Execute.builder().connection(connection).cql(Property.ofValue("invalid")).build();

        assertThrows(IllegalStateException.class, () -> task.run(context));
        verify(session).close();
    }

    @Test
    void batchSupportsEveryTypeAndReportsStatementsNotRows() throws Exception {
        var statements = List.of("INSERT INTO events (id) VALUES (1)", "DELETE FROM events WHERE id = 2");
        var context = runContextFactory.of(Map.of("statements", statements));
        for (Batch.BatchType type : Batch.BatchType.values()) {
            var connection = mock(ScyllaDbConnection.class);
            var session = mock(CqlSession.class);
            var resultSet = mock(ResultSet.class);
            when(connection.connect(context)).thenReturn(session);
            when(session.execute(any(BatchStatement.class))).thenReturn(resultSet);
            when(resultSet.wasApplied()).thenReturn(true);

            var output = Batch.builder().connection(connection)
                .cql(Property.ofExpression("{{ statements }}"))
                .batchType(Property.ofValue(type)).build().run(context);

            assertTrue(output.isWasApplied());
            assertEquals(2, output.getStatements());
            var captured = ArgumentCaptor.forClass(BatchStatement.class);
            verify(session).execute(captured.capture());
            assertEquals(DefaultBatchType.valueOf(type.name()), captured.getValue().getBatchType());
            assertEquals(2, captured.getValue().size());
            verify(session).close();
        }
    }

    @Test
    void batchDefaultsToLogged() throws Exception {
        var context = runContextFactory.of();
        assertEquals(Batch.BatchType.LOGGED, context.render(Batch.builder().build().getBatchType()).as(Batch.BatchType.class).orElseThrow());
    }

    @Test
    void rejectsBlankAndEmptyStatementsBeforeConnecting() {
        var context = runContextFactory.of();
        var connection = mock(ScyllaDbConnection.class);
        assertThrows(IllegalArgumentException.class, () -> Execute.builder().connection(connection)
            .cql(Property.ofValue(" ")).build().run(context));
        assertThrows(IllegalArgumentException.class, () -> Batch.builder().connection(connection)
            .cql(Property.ofValue(List.of())).build().run(context));
        assertThrows(IllegalArgumentException.class, () -> Batch.builder().connection(connection)
            .cql(Property.ofValue(List.of("valid", " "))).build().run(context));
        verifyNoInteractions(connection);
    }
}
