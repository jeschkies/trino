package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

class LogEntryDeserializer
        extends StdDeserializer<LogEntry>
{
    LogEntryDeserializer()
    {
        super(LogEntry.class);
    }

    @Override
    public LogEntry deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException
    {
        final JsonNode node = p.getCodec().readTree(p);
        LogEntry entry = new LogEntry();
        entry.setTs(node.get(0).asLong());
        entry.setLine(node.get(1).asText());
        return entry;
    }
}
