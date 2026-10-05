package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
        when(session.execute(any(Statement.class))).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(false);

        var output = Execute.builder().connection(connection)
            .cql(Property.ofExpression("{{ statement }}")).build().run(context);

        assertFalse(output.isWasApplied());
        verify(session).close();
    }

    @Test
    void executeClosesSessionOnFailure() throws Exception {
        var context = runContextFactory.of();
        var connection = mock(ScyllaDbConnection.class);
        var session = mock(CqlSession.class);
        when(connection.connect(context)).thenReturn(session);
        when(session.execute(any(Statement.class))).thenThrow(new IllegalStateException("server rejected statement"));
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

    @Test
    void killClosesTheActiveSession() throws Exception {
        var context = runContextFactory.of();
        var connection = mock(ScyllaDbConnection.class);
        var session = mock(CqlSession.class);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(connection.connect(context)).thenReturn(session);
        when(session.execute(any(Statement.class))).thenAnswer(invocation -> {
            started.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            var resultSet = mock(ResultSet.class);
            when(resultSet.wasApplied()).thenReturn(true);
            return resultSet;
        });
        var task = Execute.builder().connection(connection).cql(Property.ofValue("SELECT * FROM system.local")).build();
        var worker = new Thread(() -> {
            try {
                task.run(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        worker.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        task.kill();
        verify(session).closeAsync();
        release.countDown();
        worker.join(5000);
        assertFalse(worker.isAlive());
    }

    @Test
    void killDuringConnectClosesSessionBeforeExecute() throws Exception {
        var context = runContextFactory.of();
        var connection = mock(ScyllaDbConnection.class);
        var session = mock(CqlSession.class);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(connection.connect(context)).thenAnswer(invocation -> {
            started.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return session;
        });
        var task = Execute.builder().connection(connection).cql(Property.ofValue("SELECT * FROM system.local")).build();
        var failure = new java.util.concurrent.atomic.AtomicReference<Exception>();
        var worker = new Thread(() -> {
            try {
                task.run(context);
            } catch (Exception e) {
                failure.set(e);
            }
        });
        worker.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        task.kill();
        release.countDown();
        worker.join(5000);
        assertFalse(worker.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        verify(session).close();
        verify(session, never()).execute(any(Statement.class));
        verify(session, never()).closeAsync();
    }
}
