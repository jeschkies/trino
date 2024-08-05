package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

class MetricPointDeserializer
        extends StdDeserializer<MetricPoint>
{
    MetricPointDeserializer()
    {
        super(MetricPoint.class);
    }

    @Override
    public MetricPoint deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException
    {
        final JsonNode node = p.getCodec().readTree(p);
        MetricPoint point = new MetricPoint();
        point.setTs(node.get(0).asLong());
        point.setValue(node.get(1).asDouble());
        return point;
    }
}
