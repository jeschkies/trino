package io.trino.loki.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = MatrixDeserializer.class)
public final class Matrix
        extends QueryResult.Result
{
    List<Metric> metrics;

    public Matrix()
    {
        metrics = new ArrayList<>();
    }

    public List<Metric> getMetrics()
    {
        return metrics;
    }

    public void setMetrics(List<Metric> metrics)
    {
        this.metrics = metrics;
    }

    public record Metric(
            @JsonProperty("metric")
            Map<String, String> labels,
            List<MetricPoint> values
    ) {}
}
