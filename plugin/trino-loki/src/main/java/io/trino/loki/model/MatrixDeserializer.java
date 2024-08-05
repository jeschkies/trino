package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MatrixDeserializer
        extends JsonDeserializer<Matrix>
{
    @Override
    public Matrix deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        Matrix matrix = new Matrix();
        if (jp.currentToken() == JsonToken.START_ARRAY) {
            jp.nextToken();
            while (jp.currentToken() != JsonToken.END_ARRAY) {
                jp.readValuesAs(Matrix.Metric.class);
                jp.nextToken();
            }
        }

        return matrix;
    }
}
