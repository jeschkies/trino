package io.trino.loki;

import io.trino.loki.model.Matrix;
import io.trino.loki.model.MetricPoint;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;

public class LokiMatrixRecordCursor
        extends LokiRecordCursor
{
    private final Iterator<LokiMatrixRecordCursor.Point> metricItr;

    record Point(MetricPoint p, Map<String, String> labels) {}

    private Point current;

    public LokiMatrixRecordCursor(List<LokiColumnHandle> columnHandles, Matrix matrix)
    {
        super(columnHandles);

        this.metricItr = matrix.getMetrics()
                .stream()
                .flatMap(metric -> metric.values().stream()
                        .map(value -> new LokiMatrixRecordCursor.Point(value, metric.labels()))).iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!metricItr.hasNext()) {
            return false;
        }
        current = metricItr.next();
        return true;
    }

    @Override
    Object getFieldValue(int field)
    {
        checkState(current != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return switch (columnIndex) {
            case 0 -> getSqlMapFromMap(columnHandles.get(columnIndex).type(), current.labels);
            case 1 -> current.p.getTs();
            case 2 -> current.p.getValue();
            default -> null;
        };
    }

    @Override
    long toTimeWithTimeZone(Long seconds)
    {
        Instant ts = Instant.ofEpochSecond(seconds);

        // render with the fixed offset of the Trino server
        int offsetMinutes = ts.atZone(ZoneId.systemDefault()).getOffset().getTotalSeconds() / 60;
        return packDateTimeWithZone(ts.toEpochMilli(), offsetMinutes);
    }
}
