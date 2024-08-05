package io.trino.loki.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.Lists;

import java.io.IOException;

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
            matrix.setMetrics(Lists.newArrayList(jp.readValuesAs(Matrix.Metric.class)));
        }

        return matrix;
    }
}
