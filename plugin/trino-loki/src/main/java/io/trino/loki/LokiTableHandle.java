package io.trino.loki;

import io.trino.spi.connector.ConnectorTableHandle;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record LokiTableHandle(String query, Instant start, Instant end) implements ConnectorTableHandle {
    public LokiTableHandle
    {
        requireNonNull(query, "query is null");
    }

}
