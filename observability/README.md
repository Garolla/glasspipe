# observability

`alloy/config.alloy` ships container logs to whatever Loki you already
have running (`LOKI_ENDPOINT_URL`, push-based -- Loki accepts this
natively).

Metrics are deliberately **not** pushed through Alloy. The original
version of this file did `prometheus.scrape` + `prometheus.remote_write`
to push ClickHouse/Redpanda metrics to a Prometheus -- dropped after a
real deploy: vanilla Prometheus doesn't accept remote-write pushes unless
started with `--web.enable-remote-write-receiver`, which isn't something
this file can assume about a Prometheus it didn't set up. Point your own
Prometheus at these instead, the normal pull way:

```yaml
scrape_configs:
  - job_name: clickhouse
    static_configs:
      - targets: ["clickhouse:9363"]
    metrics_path: /metrics
  - job_name: redpanda
    static_configs:
      - targets: ["redpanda:9644"]
    metrics_path: /public_metrics
```

Requires `clickhouse` and `redpanda` reachable from your Prometheus
container -- on the same Docker network, or wherever your Prometheus can
already reach scrape targets. ClickHouse's `/metrics` endpoint is enabled
via `clickhouse/config/prometheus.xml`; Redpanda exposes
`/public_metrics` on its admin port (`9644`) by default.

**Not verified against a live Alloy binary** -- there was no way to
install/run Alloy in the sandbox this was built in, and network access to
fetch it was blocked. The component names and River syntax
(`discovery.docker`, `loki.source.docker`, `loki.write`, the `env()`
stdlib function) are correct as of recent Alloy releases at the time of
writing, but this file should be smoke-tested with `alloy fmt` / `alloy
run` before being trusted. The logs-only version above *has* been run for
real, on real infrastructure -- see the repo's commit history.

Required env vars (see `.env.example`): `LOKI_ENDPOINT_URL`.
