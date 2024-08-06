/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.loki;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.loki.model.Data;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.*;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.util.Objects.requireNonNull;

public class LokiMetadata
        implements ConnectorMetadata
{
    private final LokiClient lokiClient;

    // TODO: this might not be the right spot
    static final Type TIMESTAMP_COLUMN_TYPE = createTimestampWithTimeZoneType(3);

    public Type getVarcharMapType()
    {
        return this.lokiClient.getVarcharMapType();
    }

    @Inject
    public LokiMetadata(LokiClient lokiClient)
    {
        this.lokiClient = requireNonNull(lokiClient, "lokiClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    private static List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(ImmutableSet.of("default"));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(ImmutableSet.of("default")));

        return schemaNames.stream()
                .flatMap(schemaName ->
                        lokiClient.getTableNames(schemaName).stream().map(tableName -> new SchemaTableName(schemaName, tableName)))
                .collect(toImmutableList());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof LokiTableFunction.QueryHandle queryHandle)) {
            return Optional.empty();
        }

        LokiTableHandle tableHandle = queryHandle.getTableHandle();

        // Column handles are for the connector to understand the target type.
        List<ColumnHandle> columnHandles = getColumnHandles(tableHandle.query());

        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LokiTableHandle lokiTableHandle = (LokiTableHandle) table;

        // Column Metadata tells Trino about the expected types.
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("labels", getVarcharMapType()));
        columns.add(new ColumnMetadata("timestamp", TIMESTAMP_COLUMN_TYPE));

        if (lokiClient.getExpectedResultType(lokiTableHandle.query()) == Data.ResultType.Matrix) {
            columns.add(new ColumnMetadata("value", DoubleType.DOUBLE));
        }
        else {
            columns.add(new ColumnMetadata("value", VarcharType.VARCHAR));
        }

        return new ConnectorTableMetadata(new SchemaTableName("default", "test"), columns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return new ColumnMetadata("a_xxx", VarcharType.VARCHAR);
    }

    public List<ColumnHandle> getColumnHandles(String query)
    {
        // TODO: cache result for query to avoid too many calls to Loki.
        ImmutableList.Builder<ColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(3);
        columnsBuilder.add(new LokiColumnHandle("labels", getVarcharMapType(), 0));
        columnsBuilder.add(new LokiColumnHandle("timestamp", TIMESTAMP_COLUMN_TYPE, 1));

        if (lokiClient.getExpectedResultType(query) == Data.ResultType.Matrix) {
            columnsBuilder.add(new LokiColumnHandle("value", DoubleType.DOUBLE, 2));
        }
        else {
            columnsBuilder.add(new LokiColumnHandle("value", VarcharType.VARCHAR, 2));
        }
        return columnsBuilder.build();
    }
}
