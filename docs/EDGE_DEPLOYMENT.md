# Edge Deployment Cookbook

Step-by-step guides for deploying Chronicle on edge devices and constrained environments.

## Raspberry Pi 4/5

### Prerequisites
- Raspberry Pi 4 (2GB+ RAM) or Pi 5
- Raspberry Pi OS (64-bit recommended)
- Go 1.24+ installed

### Installation

```bash
# Install Go (ARM64)
wget https://go.dev/dl/go1.24.4.linux-arm64.tar.gz
sudo tar -C /usr/local -xzf go1.24.4.linux-arm64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Build your application
mkdir ~/chronicle-app && cd ~/chronicle-app
go mod init myapp
go get github.com/chronicle-db/chronicle
```

### Optimized Configuration

```go
cfg := chronicle.DefaultConfig("/data/chronicle.db")

// Constrained memory: 64MB budget
cfg.Storage.MaxMemory = 64 * 1024 * 1024

// Small buffer for limited RAM
cfg.Storage.BufferSize = 1000

// Short partitions for small files
cfg.Storage.PartitionDuration = 30 * time.Minute

// Aggressive retention for SD card longevity
cfg.Retention.RetentionDuration = 24 * time.Hour

// Disable HTTP if not needed (saves ~5MB RAM)
cfg.HTTP.HTTPEnabled = false
```

### systemd Service

```ini
# /etc/systemd/system/chronicle.service
[Unit]
Description=Chronicle Time-Series Collector
After=network.target

[Service]
Type=simple
User=pi
ExecStart=/home/pi/chronicle-app/collector
Restart=always
RestartSec=5
MemoryMax=128M

[Install]
WantedBy=multi-user.target
```

### Performance on Pi 4 (4GB)
- Write throughput: ~50K points/sec
- Query latency: <10ms for last-hour queries
- Memory usage: ~40MB at steady state
- Disk usage: ~50MB/day for 100 sensors at 10s interval

---

## NVIDIA Jetson (Nano/Xavier/Orin)

### Prerequisites
- JetPack SDK installed
- Go 1.24+ (ARM64)

### Installation

```bash
# Go is the same as ARM64 Linux
wget https://go.dev/dl/go1.24.4.linux-arm64.tar.gz
sudo tar -C /usr/local -xzf go1.24.4.linux-arm64.tar.gz
```

### GPU-Accelerated Workload

```go
cfg := chronicle.DefaultConfig("/ssd/chronicle.db")

// Jetson has more memory — use it
cfg.Storage.MaxMemory = 512 * 1024 * 1024

// Larger buffers for high-throughput sensor fusion
cfg.Storage.BufferSize = 50000

// NVMe SSD — larger partitions are fine
cfg.Storage.PartitionDuration = 2 * time.Hour

// Enable HTTP for local dashboard
cfg.HTTP.HTTPEnabled = true
cfg.HTTP.HTTPPort = 8080
```

### Performance on Jetson Xavier NX
- Write throughput: ~200K points/sec
- Query latency: <5ms
- Memory usage: ~100MB at steady state

---

## K3s (Lightweight Kubernetes)

### Prerequisites
- K3s cluster running
- kubectl configured

### Deployment

```yaml
# chronicle-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chronicle
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: chronicle
  template:
    metadata:
      labels:
        app: chronicle
    spec:
      containers:
      - name: chronicle
        image: ghcr.io/chronicle-db/chronicle:latest
        ports:
        - containerPort: 8080
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "64Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 5
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 3
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: chronicle-data
---
apiVersion: v1
kind: Service
metadata:
  name: chronicle
  namespace: monitoring
spec:
  selector:
    app: chronicle
  ports:
  - port: 8080
    targetPort: 8080
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: chronicle-data
  namespace: monitoring
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f chronicle-deployment.yaml
```

---

## AWS Greengrass / Azure IoT Edge

### AWS Greengrass Component

```json
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "com.chronicle.collector",
  "ComponentVersion": "0.1.0",
  "Manifests": [{
    "Platform": { "os": "linux", "architecture": "arm64" },
    "Lifecycle": {
      "Run": "/greengrass/v2/packages/chronicle/bin/collector -config /greengrass/v2/packages/chronicle/config.yaml"
    },
    "Artifacts": [{
      "URI": "s3://my-bucket/chronicle/collector-arm64"
    }]
  }]
}
```

### Azure IoT Edge Module

```json
{
  "modulesContent": {
    "$edgeAgent": {
      "properties.desired": {
        "modules": {
          "chronicle": {
            "type": "docker",
            "settings": {
              "image": "ghcr.io/chronicle-db/chronicle:latest",
              "createOptions": "{\"HostConfig\":{\"Binds\":[\"/data/chronicle:/data\"]}}"
            },
            "env": {
              "CHRONICLE_HTTP_PORT": { "value": "8080" }
            }
          }
        }
      }
    }
  }
}
```

---

## Resource Tuning Guide

| Device | RAM | Recommended MaxMemory | BufferSize | PartitionDuration |
|--------|-----|-----------------------|------------|-------------------|
| Pi Zero 2 | 512MB | 32MB | 500 | 15min |
| Pi 4 (2GB) | 2GB | 64MB | 1,000 | 30min |
| Pi 4 (4GB) | 4GB | 256MB | 5,000 | 1h |
| Jetson Nano | 4GB | 256MB | 10,000 | 1h |
| Jetson Xavier | 8GB | 512MB | 50,000 | 2h |
| Jetson Orin | 32GB | 2GB | 100,000 | 4h |
| K3s node (4GB) | 4GB | 256MB | 5,000 | 1h |
