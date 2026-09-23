# Kubernetes Sidecar Example

Deploy Chronicle as a **sidecar** that scrapes Prometheus-format metrics from
the application container in the same pod (localhost), tags them with
Kubernetes Downward API metadata, and keeps a short local retention window.

This example covers Bounty #11 from `docs/BOUNTY_PROGRAM.md`.

## What you get

| Path | Purpose |
|------|---------|
| `main.go` | Local demo: app `/metrics` on `:9090` + Chronicle `K8sSidecar` on `:8080` |
| `manifests/deployment.yaml` | Multi-container Deployment (app + chronicle-sidecar) |
| `manifests/service.yaml` | ClusterIP Service for app + sidecar health |
| `manifests/configmap.yaml` | Optional scrape tuning annotations / notes |

## Quick local demo (no cluster required)

```bash
cd examples/kubernetes-sidecar
POD_NAME=demo-0 POD_NAMESPACE=default NODE_NAME=local go run .

# in another shell
curl -s localhost:9090/metrics | head
curl -s localhost:8080/health
curl -s localhost:8080/stats
```

The sidecar scrapes `localhost:9090/metrics` every few seconds (default library
interval is 15s; the demo uses 3s) and stores points in a local Chronicle DB.
Pod identity tags come from `POD_NAME`, `POD_NAMESPACE`, `NODE_NAME`, `POD_IP`,
and `HOST_IP` — the same variables populated by the Downward API in Kubernetes.
See [ENVIRONMENT_VARIABLES.md](../../docs/ENVIRONMENT_VARIABLES.md).

## Kubernetes layout

```
┌──────────────────────────── Pod ────────────────────────────┐
│  app container (:8080 app, :9090 /metrics)                   │
│  chronicle-sidecar (:8080 health/stats, scrapes localhost)  │
└─────────────────────────────────────────────────────────────┘
```

Apply (after building/pushing images you control):

```bash
kubectl apply -f manifests/
kubectl -n default get pods -l app=chronicle-sidecar-demo
kubectl -n default port-forward svc/chronicle-sidecar-demo 9090:9090 8081:8081
```

### Image notes

Upstream `k8s/Dockerfile` currently builds `./cmd/chronicle`, which is not
present in this repository tip. For a reproducible demo pod you can:

1. Build the local Go example into two images (app metrics exporter + sidecar
   binary wrapping `chronicle.NewK8sSidecar`), **or**
2. Point the sidecar container at your own Chronicle image that exposes the
   sidecar mode documented in [K8S_DEPLOYMENT.md](../../docs/K8S_DEPLOYMENT.md).

The manifests use placeholder image names (`chronicle-sidecar-demo/app` and
`chronicle-sidecar-demo/sidecar`) so you can substitute your registry without
editing scrape logic.

## Sidecar behaviour (library)

`chronicle.NewK8sSidecar` (see `k8s_sidecar.go`):

- Scrapes `http://localhost:<MetricsPort><MetricsPath>` by default (`9090` + `/metrics`)
- Adds pod/namespace/node tags from Downward API env vars
- Serves `/health`, `/ready`, `/stats`, `/targets`, `/api/v1/sidecar/metrics` on `HealthPort` (default `8080`)
- Optional remote write via `RemoteWriteEnabled` / `RemoteWriteURL`

## Verify in-cluster

```bash
# App metrics
kubectl exec -it deploy/chronicle-sidecar-demo -c app -- wget -qO- http://127.0.0.1:9090/metrics | head

# Sidecar health (from sidecar container)
kubectl exec -it deploy/chronicle-sidecar-demo -c chronicle-sidecar -- wget -qO- http://127.0.0.1:8080/health
kubectl exec -it deploy/chronicle-sidecar-demo -c chronicle-sidecar -- wget -qO- http://127.0.0.1:8080/stats
```

## Related docs

- [K8S_DEPLOYMENT.md](../../docs/K8S_DEPLOYMENT.md) — standalone vs sidecar modes
- [ENVIRONMENT_VARIABLES.md](../../docs/ENVIRONMENT_VARIABLES.md) — Downward API vars
- [examples/docker-quickstart](../docker-quickstart) — Compose stack without Kubernetes
