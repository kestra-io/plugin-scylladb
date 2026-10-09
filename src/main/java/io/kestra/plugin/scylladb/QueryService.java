package io.kestra.plugin.scylladb;

import java.io.BufferedOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

final class QueryService {
    private QueryService() {
    }

    static void validate(String cql, Integer fetchSize) {
        if (cql == null || cql.isBlank()) {
            throw new IllegalArgumentException("cql must not be blank");
        }
        if (fetchSize != null && fetchSize <= 0) {
            throw new IllegalArgumentException("fetchSize must be greater than zero");
        }
    }

    static FetchedResult fetch(
        CqlSession session,
        RunContext runContext,
        String cql,
        FetchType type,
        Integer fetchSize
    ) throws Exception {
        return fetch(session, runContext, cql, type, fetchSize, Map.of());
    }

    static FetchedResult fetch(
        CqlSession session,
        RunContext runContext,
        String cql,
        FetchType type,
        Integer fetchSize,
        Map<String, Object> parameters
    ) throws Exception {
        Objects.requireNonNull(type, "fetchType is required");
        if (type == FetchType.NONE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE, or STORE");
        }
        var resultSet = session.execute(statement(session, cql, parameters, fetchSize));

        return switch (type) {
            case NONE -> throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE, or STORE");
            case FETCH_ONE -> {
                var row = resultSet.one();
                yield new FetchedResult(
                    Query.Output.builder().row(row == null ? null : CqlValues.row(row))
                        .size(row == null ? 0L : 1L).build()
                );
            }
            case FETCH -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (Row row : resultSet) {
                    rows.add(CqlValues.row(row));
                }
                yield new FetchedResult(
                    Query.Output.builder().rows(rows).size((long) rows.size()).build()
                );
            }
            case STORE -> store(resultSet, runContext);
        };
    }

    static BatchableStatement<?> statement(CqlSession session, String cql, Map<String, Object> parameters, Integer fetchSize) {
        validate(cql, fetchSize);
        if (parameters == null || parameters.isEmpty()) {
            var builder = SimpleStatement.builder(cql);
            if (fetchSize != null) {
                builder.setPageSize(fetchSize);
            }
            return builder.build();
        }
        if (hasAnonymousMarker(cql)) {
            throw new IllegalArgumentException("CQL markers must be named, for example :tenant");
        }
        var prepared = session.prepare(cql);
        var variables = prepared.getVariableDefinitions();
        var builder = prepared.boundStatementBuilder();
        var codecs = session.getContext().getCodecRegistry();
        for (int i = 0; i < variables.size(); i++) {
            var variable = variables.get(i);
            var name = variable.getName().asInternal();
            if (name == null || name.isBlank() || "?".equals(name)) {
                throw new IllegalArgumentException("CQL markers must be named, for example :tenant");
            }
            if (!parameters.containsKey(name)) {
                throw new IllegalArgumentException("Missing bound parameter: " + name);
            }
            var raw = parameters.get(name);
            if (raw == null) {
                builder.setToNull(i);
            } else {
                var value = CqlValues.fromStorage(raw, variable.getType(), codecs);
                builder.set(i, value, codecs.<Object>codecFor(variable.getType()));
            }
        }
        if (fetchSize != null) {
            builder.setPageSize(fetchSize);
        }
        return builder.build();
    }

    private static boolean hasAnonymousMarker(String cql) {
        boolean inString = false;
        for (int i = 0; i < cql.length(); i++) {
            char current = cql.charAt(i);
            if (current == '\'') {
                if (inString && i + 1 < cql.length() && cql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
            } else if (!inString && current == '?') {
                return true;
            }
        }
        return false;
    }

    private static FetchedResult store(ResultSet resultSet, RunContext runContext) throws Exception {
        var localFile = runContext.workingDir().createTempFile(".ion").toFile();
        long size = 0;
        try (var stream = new BufferedOutputStream(Files.newOutputStream(localFile.toPath()), FileSerde.BUFFER_SIZE)) {
            for (Row row : resultSet) {
                FileSerde.write(stream, CqlValues.row(row));
                size++;
            }
        }
        var output = Query.Output.builder()
            .uri(runContext.storage().putFile(localFile))
            .size(size)
            .build();
        return new FetchedResult(output);
    }

    /**
     * STORE results must be replayed from {@link Query.Output#getUri()} because {@code putFile}
     * may consume the temporary file.
     */
    record FetchedResult(Query.Output output) {
    }
}
