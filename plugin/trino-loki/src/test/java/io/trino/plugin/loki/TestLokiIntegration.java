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
package io.trino.plugin.loki;

import io.github.jeschkies.loki.client.LokiClient;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TestLokiIntegration
        extends AbstractTestQueryFramework
{
    private LokiClient client;

    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter timestampFormatterAtEasternTime = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss'-05:00'").withZone(ZoneId.of("US/Eastern"));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        final TestingLokiServer server = closeAfterClass(new TestingLokiServer());
        this.client = server.createLokiClient();
        return LokiQueryRunner.builder(server).build();
    }

    @Test
    public void testLogsQuery()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        this.client.pushLogLine("line 1", end.minus(Duration.ofMinutes(10)), ImmutableMap.of("test", "logs_query"));
        this.client.pushLogLine("line 2", end.minus(Duration.ofMinutes(5)), ImmutableMap.of("test", "logs_query"));
        this.client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "logs_query"));
        this.client.flush();

        assertQuery(String.format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         '{test="logs_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('line 1')");

        assertQuery(String.format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         '{test="logs_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatterAtEasternTime.format(start), timestampFormatterAtEasternTime.format(end)),
                "VALUES ('line 1')");
    }

    @Test
    public void testMetricsQuery()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        this.client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "metrics_query"));
        this.client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "metrics_query"));
        this.client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "metrics_query"));
        this.client.flush();
        assertQuery(String.format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         'count_over_time({test="metrics_query"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES (1.0)");
    }

    @Test
    public void testLabels()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        this.client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "labels"));
        this.client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "labels"));
        this.client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "labels"));
        this.client.flush();
        assertQuery(String.format("""
                        SELECT labels['test'] FROM
                        TABLE(system.query_range(
                         'count_over_time({test="labels"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('labels')");
    }

    @Test
    public void testLabelsComplex()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        this.client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "labels_complex", "service", "one"));
        this.client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "labels_complex", "service", "two"));
        this.client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "labels_complex", "service", "one"));
        this.client.flush();
        assertQuery(String.format("""
                        SELECT labels['service'], COUNT(*) FROM
                        TABLE(system.query_range(
                          '{test="labels_complex"}',
                          TIMESTAMP '%s',
                          TIMESTAMP '%s'
                        ))
                        GROUP BY labels['service']
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('one', 2.0), ('two', 1.0)");
    }

    @Test
    public void testSelectTimestamp()
            throws Exception
    {
        DateTimeFormatter isoTimestampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
        Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofHours(12));
        Instant end = start.plus(Duration.ofHours(4));
        Instant firstLineTimestamp = start.truncatedTo(ChronoUnit.MILLIS);

        this.client.pushLogLine("line 1", firstLineTimestamp, ImmutableMap.of("test", "select_timestamp_query"));
        this.client.pushLogLine("line 2", firstLineTimestamp.plus(Duration.ofHours(1)), ImmutableMap.of("test", "select_timestamp_query"));
        this.client.pushLogLine("line 3", firstLineTimestamp.plus(Duration.ofHours(2)), ImmutableMap.of("test", "select_timestamp_query"));
        this.client.flush();
        assertQuery(String.format("""
                        SELECT
                          -- H2 does not support TIMESTAMP WITH TIME ZONE so cast to VARCHAR
                          to_iso8601(timestamp), value
                        FROM
                        TABLE(system.query_range(
                         '{test="select_timestamp_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        ORDER BY timestamp
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                String.format("VALUES ('%s', 'line 1')", isoTimestampFormatter.format(firstLineTimestamp)));

        assertQuery(String.format("""
                        SELECT
                          -- H2 does not support TIMESTAMP WITH TIME ZONE so cast to VARCHAR
                          to_iso8601(timestamp), value
                        FROM
                        TABLE(system.query_range(
                         'count_over_time({test="select_timestamp_query"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        ORDER BY timestamp ASC
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                // Since the start is always noon the previous day we know how the steps align.
                String.format("VALUES ('%s', 1.0)", isoTimestampFormatter.format(start.plus(Duration.ofSeconds(48)))));
    }

    @Test
    public void testSelectFromTableFails()
    {
        assertQueryFails("SELECT * FROM default", "Loki connector does not support querying tables directly. Use the TABLE function instead.");
    }
}
