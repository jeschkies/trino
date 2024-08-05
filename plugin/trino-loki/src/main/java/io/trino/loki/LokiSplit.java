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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static java.util.Objects.requireNonNull;

// TODO: support time based splits
public record LokiSplit(
        @JsonProperty("uri")
        String uri,
        @JsonProperty("query")
        String query,
        @JsonProperty("start")
        LongTimestampWithTimeZone start,
        @JsonProperty("end")
        LongTimestampWithTimeZone end
) implements ConnectorSplit {

    @JsonCreator
    public LokiSplit
    {
       requireNonNull(uri, "uri is null");
       requireNonNull(query, "query is null");
    }
}
