package io.trino.loki;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record LokiTableHandle(String query, LongTimestampWithTimeZone start, LongTimestampWithTimeZone end) implements ConnectorTableHandle {
    public LokiTableHandle
    {
        requireNonNull(query, "query is null");
    }

}
