// Playground Dataset Definitions
// These datasets are available in the Chronicle interactive playground
// at /playground on the documentation website.

export const PLAYGROUND_DATASETS = {
  iot_sensors: {
    name: 'IoT Sensor Fleet',
    description: '50 sensors reporting temperature, humidity, and pressure every 10 seconds',
    metrics: ['temperature', 'humidity', 'pressure', 'battery_voltage'],
    tags: {
      device: ['sensor-001', 'sensor-002', 'sensor-003', 'sensor-004', 'sensor-005',
               'sensor-010', 'sensor-020', 'sensor-030', 'sensor-040', 'sensor-050'],
      location: ['warehouse-A', 'warehouse-B', 'outdoor', 'cold-storage', 'server-room'],
      firmware: ['v1.2.0', 'v1.3.0', 'v2.0.0'],
    },
    pointCount: 500,
    timeRange: '1 hour',
  },
  infrastructure: {
    name: 'Infrastructure Monitoring',
    description: 'Server metrics: CPU, memory, disk, network across a small cluster',
    metrics: ['cpu_usage', 'memory_used_mb', 'disk_io_ops', 'network_bytes_in', 'network_bytes_out'],
    tags: {
      host: ['web-01', 'web-02', 'api-01', 'db-01', 'cache-01'],
      region: ['us-east', 'eu-west'],
      environment: ['production', 'staging'],
    },
    pointCount: 300,
    timeRange: '30 minutes',
  },
  application: {
    name: 'Application Metrics',
    description: 'Request latencies, error rates, and queue depths for a microservice',
    metrics: ['request_latency_ms', 'error_rate', 'queue_depth', 'active_connections'],
    tags: {
      service: ['auth', 'api', 'worker', 'gateway'],
      method: ['GET', 'POST', 'PUT', 'DELETE'],
      status: ['200', '400', '500'],
    },
    pointCount: 400,
    timeRange: '15 minutes',
  },
};

export const EXAMPLE_QUERIES = [
  // SQL-like queries
  { category: 'SQL', label: 'All CPU data', query: 'SELECT * FROM cpu_usage' },
  { category: 'SQL', label: 'Average temperature by location', query: 'SELECT mean(value) FROM temperature GROUP BY location WINDOW 5m' },
  { category: 'SQL', label: 'Top 10 highest latencies', query: 'SELECT * FROM request_latency_ms LIMIT 10' },
  { category: 'SQL', label: 'Max memory by host', query: 'SELECT max(value) FROM memory_used_mb GROUP BY host WINDOW 1m' },
  { category: 'SQL', label: 'Disk I/O sum', query: 'SELECT sum(value) FROM disk_io_ops WHERE host = "db-01" WINDOW 1m' },
  { category: 'SQL', label: 'Error rate by service', query: 'SELECT mean(value) FROM error_rate GROUP BY service WINDOW 5m' },

  // PromQL queries
  { category: 'PromQL', label: 'CPU rate of change', query: 'rate(cpu_usage[5m])' },
  { category: 'PromQL', label: 'Average memory by region', query: 'avg by (region) (memory_used_mb)' },
  { category: 'PromQL', label: 'Max temperature', query: 'max(temperature)' },
  { category: 'PromQL', label: '95th percentile latency', query: 'histogram_quantile(0.95, request_latency_ms)' },

  // CQL queries
  { category: 'CQL', label: 'Gap-filled temperature', query: 'SELECT value FROM temperature WINDOW 1m GAP_FILL linear' },
  { category: 'CQL', label: 'Aligned metrics', query: 'SELECT value FROM cpu_usage ALIGN 30s' },

  // GraphQL
  { category: 'GraphQL', label: 'Query with filters', query: '{ query(metric: "cpu_usage", limit: 10) { points { value timestamp } } }' },
];
