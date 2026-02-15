# Environment Variables

Chronicle uses environment variables for Kubernetes integration and test configuration. All variables are **optional** and have sensible defaults or fallback mechanisms.

## Kubernetes Integration

### Operator Variables

| Variable | Purpose | Default | File |
|----------|---------|---------|------|
| `KUBERNETES_SERVICE_HOST` | Detects whether Chronicle is running inside a Kubernetes cluster. When set, the operator enables in-cluster configuration. | *(not set)* | `k8s_operator.go` |
| `HOSTNAME` | Retrieves the pod name when running inside Kubernetes. Used as a fallback when the Downward API is unavailable. | *(not set)* | `k8s_operator.go` |
| `CHRONICLE_NAMESPACE` | Sets the Kubernetes namespace for the Chronicle operator. Used as a fallback when the service account namespace file (`/var/run/secrets/kubernetes.io/serviceaccount/namespace`) is not available. | `"default"` | `k8s_operator.go` |

### Sidecar Variables (Downward API)

These variables are typically populated by the Kubernetes [Downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/) and are used by the Chronicle sidecar to tag collected metrics with pod metadata.

| Variable | Purpose | Default | File |
|----------|---------|---------|------|
| `POD_NAME` | Name of the pod running the Chronicle sidecar. Added as a tag to all scraped metrics. | `""` | `k8s_sidecar.go` |
| `POD_NAMESPACE` | Namespace of the pod. Added as a tag to all scraped metrics. | `""` | `k8s_sidecar.go` |
| `NODE_NAME` | Name of the Kubernetes node hosting the pod. Added as a tag to all scraped metrics. | `""` | `k8s_sidecar.go` |
| `POD_IP` | IP address of the pod. Added as a tag to all scraped metrics. | `""` | `k8s_sidecar.go` |
| `HOST_IP` | IP address of the host/node. Added as a tag to all scraped metrics. | `""` | `k8s_sidecar.go` |

To configure these in a Kubernetes deployment spec:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: HOST_IP
    valueFrom:
      fieldRef:
        fieldPath: status.hostIP
```

## Testing Variables

| Variable | Purpose | Default | File |
|----------|---------|---------|------|
| `S3_TEST_ENDPOINT` | S3-compatible endpoint URL for integration tests (e.g., MinIO). | `"http://localhost:9000"` | `storage_backend_s3_integration_test.go` |
| `S3_TEST_BUCKET` | Bucket name used by S3 integration tests. | `"chronicle-test"` | `storage_backend_s3_integration_test.go` |

### Running S3 Integration Tests

```bash
# Using MinIO locally
export S3_TEST_ENDPOINT=http://localhost:9000
export S3_TEST_BUCKET=chronicle-test
go test -run TestS3 -v ./...
```
