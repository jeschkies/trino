package io.trino.loki;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.*;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LokiTableFunction
        extends AbstractConnectorTableFunction
{
    public LokiTableFunction()
    {
        super(
                "default",
                "loki",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("START")
                                .type(TIMESTAMP_TZ_NANOS)
                                .defaultValue(LongTimestampWithTimeZone.fromEpochSecondsAndFraction(0, 0, TimeZoneKey.UTC_KEY))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("END")
                                .type(TIMESTAMP_TZ_NANOS)
                                .defaultValue(LongTimestampWithTimeZone.fromEpochSecondsAndFraction(0, 0, TimeZoneKey.UTC_KEY))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("QUERY")
                                .type(VarcharType.VARCHAR)
                                .build()),
                GENERIC_TABLE);
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl) {
        io.airlift.slice.Slice selector = (io.airlift.slice.Slice) ((ScalarArgument) arguments.get("QUERY")).getValue();
        String strSelector = new String(selector.byteArray());

       var start = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("START")).getValue();
       var end = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("END")).getValue();

        if (Strings.isNullOrEmpty(strSelector)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, strSelector);
        }

        // determine the returned row type
        List<Descriptor.Field> fields = new ArrayList<>();
        fields.add(new Descriptor.Field(strSelector, Optional.of(VarcharType.VARCHAR)));

        Descriptor returnedType = new Descriptor(fields);

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new QueryHandle(new LokiTableHandle(strSelector, start, end)))
                .build();
    }

    public static class QueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final LokiTableHandle tableHandle;

        @JsonCreator
        public QueryHandle(@JsonProperty("tableHandle") LokiTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public LokiTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
