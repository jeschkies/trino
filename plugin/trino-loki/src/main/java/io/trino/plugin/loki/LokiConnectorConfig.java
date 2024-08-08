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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class LokiConnectorConfig
{
    private URI lokiURI = URI.create("http://localhost:3100");
    private String user;
    private String password;
    private Duration readTimeout = new Duration(10, TimeUnit.SECONDS);

    @NotNull
    public URI getLokiURI()
    {
        return lokiURI;
    }

    @Config("loki.uri")
    @ConfigDescription("Loki query endpoint") // TODO: decide if with or without /loki/api/v1/query
    public LokiConnectorConfig setLokiURI(URI lokiURI)
    {
        this.lokiURI = lokiURI;
        return this;
    }

    @NotNull
    public Optional<String> getUser()
    {
        return Optional.ofNullable(user);
    }

    @Config("loki.auth.user")
    public LokiConnectorConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("loki.auth.password")
    @ConfigSecuritySensitive
    public LokiConnectorConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("1s")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("loki.read-timeout")
    @ConfigDescription("How much time a query to Loki has before timing out")
    public LokiConnectorConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }
}
