package io.kestra.plugin.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "ScyllaDB connection", description = "Native CQL connection settings rendered at task execution.")
public class ScyllaDbConnection {
    @NotNull
    @PluginProperty(group = "connection")
    @Schema(title = "Contact points", description = "Non-empty list of hosts, host:port, or [IPv6]:port addresses. The default port is 9042.")
    private Property<List<String>> contactPoints;

    @NotNull
    @PluginProperty(group = "connection")
    @Schema(title = "Local datacenter", description = "Datacenter used by the driver's load balancing policy, for example datacenter1.")
    private Property<String> localDatacenter;

    @PluginProperty(group = "connection")
    @Schema(title = "Keyspace", description = "Optional default keyspace. Omit when creating a keyspace with DDL.")
    private Property<String> keyspace;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(secret = true, group = "connection")
    @Schema(title = "Username", description = "Native CQL authentication username. Set together with password; secret expressions are supported.")
    private Property<String> username;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(secret = true, group = "connection")
    @Schema(title = "Password", description = "Native CQL authentication password. Use a Kestra secret expression.", format = "password", accessMode = Schema.AccessMode.WRITE_ONLY)
    private Property<String> password;

    @Builder.Default
    @PluginProperty(group = "connection")
    @Schema(title = "Enable TLS", description = "Enable certificate and hostname verification. Uses JVM trust roots unless a truststore is supplied.")
    private Property<Boolean> tlsEnabled = Property.ofValue(false);

    @PluginProperty(group = "connection")
    @Schema(title = "Truststore path", description = "Optional JKS truststore path on the worker filesystem. Requires TLS.")
    private Property<String> truststorePath;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(secret = true, group = "connection")
    @Schema(title = "Truststore password", description = "Optional truststore password. Requires truststorePath. Use a Kestra secret expression.", format = "password", accessMode = Schema.AccessMode.WRITE_ONLY)
    private Property<String> truststorePassword;

    @PluginProperty(group = "connection")
    @Schema(title = "Client keystore path", description = "Optional JKS client keystore path on the worker filesystem for mutual TLS. Requires TLS.")
    private Property<String> keystorePath;

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(secret = true, group = "connection")
    @Schema(title = "Client keystore password", description = "Optional client keystore password. Requires keystorePath. Use a Kestra secret expression.", format = "password", accessMode = Schema.AccessMode.WRITE_ONLY)
    private Property<String> keystorePassword;

    @PluginProperty(group = "advanced")
    @Schema(title = "Request timeout", description = "Optional strictly positive CQL request timeout as an ISO-8601 duration, for example PT30S. Otherwise the driver default applies.")
    private Property<Duration> requestTimeout;

    public CqlSession connect(RunContext runContext) throws Exception {
        var rContactPoints = runContext.render(contactPoints).asList(String.class);
        if (rContactPoints == null || rContactPoints.isEmpty()) {
            throw new IllegalArgumentException("connection.contactPoints must not be empty");
        }
        var addresses = rContactPoints.stream().map(ScyllaDbConnection::parseContactPoint).toList();
        var rLocalDatacenter = requireNonBlank(
            runContext.render(localDatacenter).as(String.class).orElse(null),
            "connection.localDatacenter"
        );
        var rKeyspace = optionalNonBlank(runContext.render(keyspace).as(String.class).orElse(null), "connection.keyspace");
        var rUsername = optionalNonBlank(runContext.render(username).as(String.class).orElse(null), "connection.username");
        var rPassword = runContext.render(password).as(String.class).orElse(null);
        if ((rUsername == null) != (rPassword == null)) {
            throw new IllegalArgumentException("connection.username and connection.password must be configured together");
        }
        if (rPassword != null && rPassword.isEmpty()) {
            throw new IllegalArgumentException("connection.password must not be empty");
        }

        var rTlsEnabled = runContext.render(tlsEnabled).as(Boolean.class).orElse(false);
        var rTruststorePath = optionalNonBlank(
            runContext.render(truststorePath).as(String.class).orElse(null),
            "connection.truststorePath"
        );
        var rTruststorePassword = runContext.render(truststorePassword).as(String.class).orElse(null);
        var rKeystorePath = optionalNonBlank(
            runContext.render(keystorePath).as(String.class).orElse(null),
            "connection.keystorePath"
        );
        var rKeystorePassword = runContext.render(keystorePassword).as(String.class).orElse(null);
        if (!rTlsEnabled && (rTruststorePath != null || rTruststorePassword != null || rKeystorePath != null || rKeystorePassword != null)) {
            throw new IllegalArgumentException("TLS store settings require connection.tlsEnabled");
        }
        if (rTruststorePassword != null && rTruststorePath == null) {
            throw new IllegalArgumentException("connection.truststorePassword requires connection.truststorePath");
        }
        if (rKeystorePassword != null && rKeystorePath == null) {
            throw new IllegalArgumentException("connection.keystorePassword requires connection.keystorePath");
        }
        validateStorePath(rTruststorePath, "connection.truststorePath");
        validateStorePath(rKeystorePath, "connection.keystorePath");
        var rRequestTimeout = runContext.render(requestTimeout).as(Duration.class).orElse(null);
        if (rRequestTimeout != null && (rRequestTimeout.isNegative() || rRequestTimeout.isZero())) {
            throw new IllegalArgumentException("connection.requestTimeout must be strictly positive");
        }

        var config = DriverConfigLoader.programmaticBuilder();
        if (rRequestTimeout != null) {
            config.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, rRequestTimeout);
        }
        if (rTlsEnabled) {
            config.withString(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory")
                .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true)
                .withBoolean(DefaultDriverOption.SSL_ALLOW_DNS_REVERSE_LOOKUP_SAN, false);
            if (rTruststorePath != null) {
                config.withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, rTruststorePath);
            }
            if (rTruststorePassword != null) {
                config.withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, rTruststorePassword);
            }
            if (rKeystorePath != null) {
                config.withString(DefaultDriverOption.SSL_KEYSTORE_PATH, rKeystorePath);
            }
            if (rKeystorePassword != null) {
                config.withString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, rKeystorePassword);
            }
        }
        var session = CqlSession.builder()
            .addContactPoints(addresses)
            .withLocalDatacenter(rLocalDatacenter)
            .withConfigLoader(config.build());
        if (rKeyspace != null) {
            session.withKeyspace(rKeyspace);
        }
        if (rUsername != null) {
            session.withAuthCredentials(rUsername, rPassword);
        }
        return session.build();
    }

    static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static String optionalNonBlank(String value, String name) {
        return value == null ? null : requireNonBlank(value, name);
    }

    private static void validateStorePath(String value, String name) {
        if (value != null && (!Files.isRegularFile(Path.of(value)) || !Files.isReadable(Path.of(value)))) {
            throw new IllegalArgumentException(name + " must be a readable file on the worker");
        }
    }

    static InetSocketAddress parseContactPoint(String value) {
        var point = requireNonBlank(value, "connection.contactPoints entry").trim();
        String host;
        String port = null;
        if (point.startsWith("[")) {
            int end = point.indexOf(']');
            if (end < 2) {
                throw new IllegalArgumentException("Invalid bracketed IPv6 contact point");
            }
            host = point.substring(1, end);
            if (end + 1 < point.length()) {
                if (point.charAt(end + 1) != ':') {
                    throw new IllegalArgumentException("Expected :port after bracketed contact point");
                }
                port = point.substring(end + 2);
            }
        } else {
            int separator = point.indexOf(':');
            if (separator != point.lastIndexOf(':')) {
                throw new IllegalArgumentException("IPv6 contact points must be enclosed in brackets");
            }
            host = separator < 0 ? point : point.substring(0, separator);
            port = separator < 0 ? null : point.substring(separator + 1);
        }
        requireNonBlank(host, "Contact point host");
        if (host.chars().anyMatch(Character::isWhitespace) || host.contains("/") || host.contains("[") || host.contains("]")) {
            throw new IllegalArgumentException("Invalid contact point host");
        }
        int portNumber = 9042;
        if (port != null) {
            try {
                portNumber = Integer.parseInt(port);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Contact point port must be an integer from 1 to 65535");
            }
        }
        if (portNumber < 1 || portNumber > 65535) {
            throw new IllegalArgumentException("Contact point port must be an integer from 1 to 65535");
        }
        return new InetSocketAddress(host, portNumber);
    }
}
