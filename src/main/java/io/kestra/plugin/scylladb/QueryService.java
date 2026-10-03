package io.kestra.plugin.scylladb;

import java.io.BufferedOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.datastax.oss.driver.api.core.CqlSession;
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
        validate(cql, fetchSize);
        Objects.requireNonNull(type, "fetchType is required");
        if (type == FetchType.NONE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE, or STORE");
        }
        var statement = SimpleStatement.builder(cql);
        if (fetchSize != null) {
            statement.setPageSize(fetchSize);
        }
        var resultSet = session.execute(statement.build());

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
