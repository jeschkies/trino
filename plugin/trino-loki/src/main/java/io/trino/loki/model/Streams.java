package io.trino.loki.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

@JsonDeserialize(using = StreamsDeserializer.class)
public final class Streams
        extends QueryResult.Result
{

    public List<Stream> getStreams()
    {
        return streams;
    }

    public void setStreams(List<Stream> streams)
    {
        this.streams = streams;
    }

    private List<Stream> streams;

    public record Stream(
            @JsonProperty("stream")
            Map<String, String> labels,
            List<LogEntry> values
    ) {}
}
