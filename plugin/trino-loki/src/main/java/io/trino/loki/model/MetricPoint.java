package io.trino.loki.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = MetricPointDeserializer.class)
public class MetricPoint
{
    public Long getTs()
    {
        return ts;
    }

    public void setTs(Long ts)
    {
        this.ts = ts;
    }

    public double getValue()
    {
        return v;
    }

    public void setValue(double v)
    {
        this.v = v;
    }

    private Long ts;
    private double v;
}
