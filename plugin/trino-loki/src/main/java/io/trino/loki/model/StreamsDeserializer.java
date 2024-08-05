package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class StreamsDeserializer
        extends JsonDeserializer<Streams>
{
    @Override
    public Streams deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        List<Streams.Stream> streams = new ArrayList<>();
        if (jp.currentToken() == JsonToken.START_ARRAY) {
            jp.nextToken();
            while (jp.currentToken() != JsonToken.END_ARRAY) {
                streams.add(jp.readValueAs(Streams.Stream.class));
                jp.nextToken();
            }
        }

        Streams s = new Streams();
        s.setStreams(streams);
        return s;
    }
}
