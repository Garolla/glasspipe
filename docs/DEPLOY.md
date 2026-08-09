# Deploy

Source of truth is GitHub (`Garolla/glasspipe`). Deploy is triggered from
exord's Gitea, on the same VPS that already runs Grafana + Loki and has a
Gitea Actions runner registered. This document is the one-time manual
setup — none of it can live in the repo itself, it's account/instance
configuration.

## 1. Mirror GitHub into Gitea

In Gitea: **New Migration** → paste the GitHub HTTPS URL → check **"This
repository will be a mirror"**. Gitea then pulls on its own schedule
(default interval is usually a few hours; lower it in the repo's
**Settings → Repository → Mirror Settings** to something like `10m` if
you want deploys to follow a merge to `main` more promptly).

Pull mirror, not push mirror, on purpose: Gitea only needs read access to
a public/personal GitHub repo, so no Gitea credentials ever have to live
in GitHub secrets. If `10m` polling is too slow later, the upgrade is a
small GitHub Action that calls Gitea's `POST /api/v1/repos/{owner}/{repo}/mirror-sync`
on push — not set up here, this is the simple version first.

## 2. Runner

Confirmed available: a Gitea Actions runner registered on the target
VPS. `.gitea/workflows/deploy.yml` targets `runs-on: self-hosted` — check
the runner's actual registered labels (Gitea repo → **Settings → Actions
→ Runners**) and adjust that line if the label doesn't match.

## 3. Secrets on the VPS

`docker-compose.yml` reads its env from `--env-file /opt/glasspipe/.env`
(deliberately *not* the checkout directory — the Actions runner's
workspace is a fresh path per run, so anything the deploy needs that
isn't in git has to live at a fixed path instead). One-time setup on the
VPS:

```bash
mkdir -p /opt/glasspipe
cat > /opt/glasspipe/.env <<'EOF'
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<pick one>
LOKI_ENDPOINT_URL=<your existing Loki push endpoint>
PROMETHEUS_REMOTE_WRITE_URL=<your existing Prometheus/Mimir remote_write endpoint>
EOF
chmod 600 /opt/glasspipe/.env
```

This file is never committed (`.gitignore` already excludes `.env`) and
the workflow never touches it — it's set once, by hand, and persists
across every deploy.

## 4. Why the project name is pinned

`docker-compose.yml` sets `name: glasspipe` explicitly at the top.
Without that, Compose infers the project name from the current
directory's basename — fine for local dev, but the Actions runner
checks out into a different temp path on every run, which would make
Compose think each deploy is a brand new stack and mint fresh named
volumes (`clickhouse_data` included) instead of reusing the existing
ones. Pinning the name is what makes `clickhouse_data` durable across
deploys regardless of where the runner happens to check out the code.

## What happens on push to `main`

1. Gitea's mirror picks up the new commit (poll interval, or manual sync
   from the Gitea UI while `main` is quiet if you want it sooner).
2. `.gitea/workflows/deploy.yml` fires: per-package test suites, the same
   SQLMesh parse/render check and `dagster definitions validate` used
   during development, `docker compose config`, then
   `docker compose up -d --build`.
3. Any failure before the deploy step stops the workflow — nothing gets
   redeployed on a red build.

## Not yet verified

Same honesty as the rest of this repo (see root `README.md`): this
workflow file has been YAML-validated and reasoned through carefully,
but has not run against a real Gitea Actions runner — there wasn't one
reachable from the sandbox this was built in. The first real push to
`main` after setting this up is the actual test.
