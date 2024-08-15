# Trino Loki Connector

## Testing

The test query runner in `src/test/java/io.trino.loki` starts a Docker compose with Loki and Grafana. It can be used with `trino-cli  http://127.0.0.1:8080/loki/default` or through
Grafana under http://localhost:3000.
