# observability

`alloy/config.alloy` ships container logs to the Loki already running on
the VPS, and scrapes ClickHouse (`:9363/metrics`, enabled via
`clickhouse/config/prometheus.xml`) and Redpanda (`:9644/public_metrics`)
into the same VPS's Prometheus/Mimir.

**Not verified against a live Alloy binary** -- there was no way to
install/run Alloy in the sandbox this was built in, and network access to
fetch it was blocked. The component names and River syntax
(`discovery.docker`, `loki.source.docker`, `prometheus.scrape`,
`prometheus.remote_write`, the `env()` stdlib function) are correct as of
recent Alloy releases at the time of writing, but this file should be
smoke-tested with `alloy fmt` / `alloy run` against the real endpoints
before being trusted.

Required env vars (see `.env.example`): `LOKI_ENDPOINT_URL`,
`PROMETHEUS_REMOTE_WRITE_URL`.
