package io.trino.loki.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public final class Matrix
        extends QueryResult.Result
{
    List<Metric> metrics;

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
