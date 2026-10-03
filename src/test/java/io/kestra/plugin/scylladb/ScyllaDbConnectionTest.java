package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@MicronautTest
class ScyllaDbConnectionTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void parsesContactPoints() {
        assertEquals(9042, ScyllaDbConnection.parseContactPoint("127.0.0.1").getPort());
        assertEquals(9142, ScyllaDbConnection.parseContactPoint("127.0.0.1:9142").getPort());
        assertEquals(9142, ScyllaDbConnection.parseContactPoint("[::1]:9142").getPort());
        assertEquals(9042, ScyllaDbConnection.parseContactPoint("[::1]").getPort());
        for (String invalid : List.of("", " ", ":9042", "localhost:", "localhost:0", "localhost:65536", "localhost:abc", "::1", "[::1", "[::1]oops", "http://localhost")) {
            assertThrows(IllegalArgumentException.class, () -> ScyllaDbConnection.parseContactPoint(invalid));
        }
    }

    @Test
    void validatesRenderedConnectionBeforeOpeningSession() {
        var context = runContextFactory.of(Map.of("points", List.of(), "dc", "datacenter1"));
        var connection = ScyllaDbConnection.builder()
            .contactPoints(Property.ofExpression("{{ points }}"))
            .localDatacenter(Property.ofExpression("{{ dc }}"))
            .build();
        assertTrue(assertThrows(IllegalArgumentException.class, () -> connection.connect(context))
            .getMessage().contains("contactPoints"));
    }

    @Test
    void rejectsIncompleteAuthentication() {
        var connection = base().username(Property.ofValue("user")).build();
        assertTrue(assertThrows(IllegalArgumentException.class, () -> connection.connect(runContextFactory.of()))
            .getMessage().contains("configured together"));
    }

    @Test
    void rejectsNonPositiveTimeout() {
        for (Duration invalid : List.of(Duration.ZERO, Duration.ofSeconds(-1))) {
            var connection = base().requestTimeout(Property.ofValue(invalid)).build();
            assertTrue(assertThrows(IllegalArgumentException.class, () -> connection.connect(runContextFactory.of()))
                .getMessage().contains("strictly positive"));
        }
    }

    @Test
    void rejectsTlsStoreConfigurationWithoutTls() {
        var connection = base().truststorePath(Property.ofValue("unused.jks")).build();
        assertTrue(assertThrows(IllegalArgumentException.class, () -> connection.connect(runContextFactory.of()))
            .getMessage().contains("tlsEnabled"));
    }

    @Test
    void rejectsStorePasswordWithoutStorePath() {
        var connection = base().tlsEnabled(Property.ofValue(true))
            .truststorePassword(Property.ofValue("sensitive")).build();
        assertTrue(assertThrows(IllegalArgumentException.class, () -> connection.connect(runContextFactory.of()))
            .getMessage().contains("requires connection.truststorePath"));
    }

    @Test
    void excludesCredentialsFromToString() {
        var connection = base()
            .username(Property.ofValue("private-user"))
            .password(Property.ofValue("private-password"))
            .truststorePassword(Property.ofValue("private-trust-password"))
            .keystorePassword(Property.ofValue("private-key-password"))
            .build();
        assertFalse(connection.toString().contains("private-"));
        assertFalse(Execute.builder().connection(connection).build().toString().contains("private-"));
    }

    @Test
    void credentialsHaveRequiredSecretAnnotations() throws Exception {
        for (String name : List.of("username", "password", "truststorePassword", "keystorePassword")) {
            assertTrue(ScyllaDbConnection.class.getDeclaredField(name).getAnnotation(PluginProperty.class).secret());
        }
    }

    @Test
    void rendersYamlTimeoutAuthenticationAndVerifiedTlsIntoDriverConfiguration() throws Exception {
        var connection = JacksonMapper.ofYaml().readValue("""
            contactPoints: ["{{ host }}:9042"]
            localDatacenter: "{{ dc }}"
            username: "{{ user }}"
            password: "{{ pass }}"
            tlsEnabled: "{{ tls }}"
            requestTimeout: "{{ timeout }}"
            """, ScyllaDbConnection.class);
        var context = runContextFactory.of(Map.of(
            "host", "127.0.0.1", "dc", "datacenter1", "user", "alice",
            "pass", "sensitive", "tls", true, "timeout", "PT45S"
        ));
        var builder = mock(CqlSessionBuilder.class, RETURNS_SELF);
        var session = mock(CqlSession.class);
        when(builder.build()).thenReturn(session);
        try (var factory = mockStatic(CqlSession.class)) {
            factory.when(CqlSession::builder).thenReturn(builder);
            try (var actual = connection.connect(context)) {
                assertSame(session, actual);
                verify(builder).withAuthCredentials("alice", "sensitive");
                verify(builder).withLocalDatacenter("datacenter1");
                var loader = org.mockito.ArgumentCaptor.forClass(DriverConfigLoader.class);
                verify(builder).withConfigLoader(loader.capture());
                var profile = loader.getValue().getInitialConfig().getDefaultProfile();
                assertEquals(Duration.ofSeconds(45), profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
                assertTrue(profile.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION));
                assertEquals("DefaultSslEngineFactory", profile.getString(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS));
                loader.getValue().close();
            }
            verify(session).close();
        }
    }

    private ScyllaDbConnection.ScyllaDbConnectionBuilder<?, ?> base() {
        return ScyllaDbConnection.builder()
            .contactPoints(Property.ofValue(List.of("127.0.0.1:9042")))
            .localDatacenter(Property.ofValue("datacenter1"));
    }
}
