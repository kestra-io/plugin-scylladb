package io.kestra.plugin.scylladb;

import java.net.InetSocketAddress;
import java.time.Duration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real, pinned ScyllaDB server. Missing Docker is a test failure, never a skip.
 * Readiness executes CQL; a listening native-transport port alone is insufficient.
 */
abstract class ScyllaDbContainer {
    protected static final GenericContainer<?> SCYLLA =
        new GenericContainer<>(DockerImageName.parse("scylladb/scylla:6.2.3"))
            .withExposedPorts(9042)
            .withCommand("--smp", "1", "--memory", "750M", "--overprovisioned", "1", "--developer-mode", "1")
            .waitingFor(Wait.forSuccessfulCommand(
                "cqlsh $(hostname -i) 9042 -e 'SELECT release_version FROM system.local'"
            ).withStartupTimeout(Duration.ofMinutes(5)));

    @Inject
    protected RunContextFactory runContextFactory;

    protected static String contactPoint() {
        String host = SCYLLA.getHost();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + SCYLLA.getMappedPort(9042);
    }

    protected static CqlSession session() {
        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress(SCYLLA.getHost(), SCYLLA.getMappedPort(9042)))
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .build())
            .build();
    }
}
