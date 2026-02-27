# Kubernetes Deployment

This directory contains the container image and Kubernetes deployment resources for Chronicle.

## Contents

| Path | Description |
|------|-------------|
| `Dockerfile` | Multi-architecture container image (amd64, arm64, arm/v7) |
| `helm/chronicle/` | Helm chart for Kubernetes deployment |

## Docker

Build the container image:

```bash
# Single architecture
docker build -t chronicle -f k8s/Dockerfile .

# Multi-architecture
docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 -t chronicle -f k8s/Dockerfile .
```

The image uses a multi-stage build with `golang:1.24-alpine` for building and `distroless/static-debian12` for the runtime.

## Helm Chart

### Installation

```bash
helm install chronicle ./k8s/helm/chronicle
```

### Configuration

See `helm/chronicle/values.yaml` for all configurable values. Key options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Container image | `chronicle` |
| `image.tag` | Image tag | `latest` |
| `service.port` | HTTP service port | `8086` |
| `persistence.enabled` | Enable persistent storage | `true` |
| `persistence.size` | PV size | `10Gi` |

### Upgrading

```bash
helm upgrade chronicle ./k8s/helm/chronicle -f custom-values.yaml
```

### Uninstalling

```bash
helm uninstall chronicle
```
