/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.loki;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryResult
{

    static ObjectMapper mapper = new ObjectMapper();

    public static QueryResult fromJSON(InputStream input)
            throws IOException
    {
        return mapper.readValue(input, QueryResult.class);
    }

    public String getStatus()
    {
        return status;
    }

    public Data getData()
    {
        return data;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public void setData(Data data)
    {
        this.data = data;
    }

    private String status;
    private Data data;

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Data
    {

        public String getResultType()
        {
            return resultType;
        }

        public void setResultType(String resultType)
        {
            this.resultType = resultType;
        }

        private String resultType;

        @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "resultType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
        @JsonSubTypes(value = {
                @JsonSubTypes.Type(value = Streams.class, name = "streams"),
                @JsonSubTypes.Type(value = Matrix.class, name = "matrix")
        })
        private Result result;

        public Result getResult()
        {
            return result;
        }

        public void setResult(Result result)
        {
            this.result = result;
        }
    }

    static abstract class Result {}

    @JsonDeserialize(using = StreamsDeserializer.class)
    static final class Streams
            extends Result
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
    }

    static class StreamsDeserializer
            extends JsonDeserializer<Streams>
    {
        @Override
        public Streams deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException
        {
            List<Stream> streams = new ArrayList<>();
            if (jp.currentToken() == JsonToken.START_ARRAY) {
                jp.nextToken();
                while (jp.currentToken() != JsonToken.END_ARRAY) {
                    streams.add(jp.readValueAs(Stream.class));
                    jp.nextToken();
                }
            }

            Streams s = new Streams();
            s.setStreams(streams);
            return s;
        }
    }

    static final class Matrix
            extends Result
    {}

    static class Stream
    {
        public Map<String, String> getLabels()
        {
            return labels;
        }

        public void setLabels(Map<String, String> labels)
        {
            this.labels = labels;
        }

        public List<LogEntry> getValues()
        {
            return values;
        }

        public void setValues(List<LogEntry> values)
        {
            this.values = values;
        }

        @JsonProperty("stream")
        private Map<String, String> labels;

        private List<LogEntry> values;
    }

    @JsonDeserialize(using = LogEntryDeserializer.class)
    static class LogEntry
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

    static class LogEntryDeserializer
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
}
