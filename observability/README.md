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

**Verified against a live Alloy binary and a real Loki**, via
`docker compose --profile observability up`. The component names and
River syntax (`discovery.docker`, `loki.source.docker`, `loki.write`, the
`env()` stdlib function) work as documented against recent Alloy
releases.

One thing to know if you're relying on this for filtering by
container in your own Grafana/Loki: whether logs are queryable *by
container name* (vs. just full-text search) depends on your Docker
daemon's own logging config, not on Alloy. If your Docker daemon uses
the default `json-file` driver without a `tag` log-opt set, container
identity isn't attached to log lines the way some Loki pipelines expect.
This is host-level Docker config, out of scope for this repo the same
way the rest of deploy infra is -- but worth checking if per-container
filtering doesn't work as expected.

Required env vars (see `.env.example`): `LOKI_ENDPOINT_URL`.
