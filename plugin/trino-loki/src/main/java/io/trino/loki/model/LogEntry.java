package io.trino.loki.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = LogEntryDeserializer.class)
public class LogEntry
{
    public Long getTs()
    {
        return ts;
    }

    public void setTs(Long ts)
    {
        this.ts = ts;
    }

    public String getLine()
    {
        return line;
    }

    public void setLine(String line)
    {
        this.line = line;
    }

    private Long ts;
    private String line;
}
