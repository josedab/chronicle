---
sidebar_position: 105
---

# Case Studies

Real-world examples of Chronicle in action. These case studies demonstrate how different teams use Chronicle to solve time-series data challenges.

:::note
These are representative examples based on typical Chronicle use cases. If you'd like to share your Chronicle story, [open a discussion](https://github.com/chronicle-db/chronicle/discussions).
:::

---

## 🏭 Industrial IoT: Factory Floor Monitoring

### The Challenge

A manufacturing company needed to monitor CNC machines across multiple factory floors. Each machine generates 50+ metrics at 10Hz (temperature, vibration, power consumption, spindle speed, etc.).

**Constraints:**
- Each factory has unreliable internet connectivity
- Local storage on industrial PCs with 4GB RAM
- Need real-time anomaly detection for predictive maintenance
- Must retain 30 days of raw data locally

### The Solution

Chronicle runs on each industrial PC, storing metrics locally with automatic retention:

```go
db, _ := chronicle.Open("/data/cnc-metrics.db", chronicle.Config{
    MaxMemory:         256 * 1024 * 1024,   // 256MB
    PartitionDuration: 15 * time.Minute,
    RetentionDuration: 30 * 24 * time.Hour, // 30 days
    HTTPEnabled:       true,
    HTTPPort:          8086,
})

// Anomaly detection runs locally
detector := chronicle.NewAnomalyDetector(db, chronicle.AnomalyConfig{
    Metric:    "spindle_vibration",
    Method:    chronicle.AnomalyMAD,
    Threshold: 3.5,
})
```

**Results:**
- **10ms query latency** for local dashboards (vs 500ms+ to cloud)
- **Zero data loss** during 4-hour internet outage
- **Early warning** caught bearing failure 3 days before catastrophic failure
- **80% bandwidth reduction** by syncing only aggregated data to cloud

---

## 📱 Mobile App: Fitness Tracker

### The Challenge

A fitness app needed to store workout metrics (heart rate, GPS, cadence) on device for offline analysis and privacy-sensitive data handling.

**Constraints:**
- Must work offline during workouts
- Battery-efficient storage operations
- Users want historical analysis without cloud dependency
- WASM for web version, native Go for backend sync

### The Solution

Chronicle's WASM build runs in the mobile app's embedded browser view:

```javascript
// WASM version for in-app analytics
import { Chronicle } from '@chronicle-db/wasm';

const db = await Chronicle.open('workouts');

// Store workout data
await db.write({
    metric: 'heart_rate',
    tags: { workout_id: 'run-2024-01-15' },
    value: 145,
    timestamp: Date.now() * 1000000, // nanoseconds
});

// Query for personal records
const maxHR = await db.query({
    metric: 'heart_rate',
    aggregation: { function: 'max' },
});
```

**Results:**
- **Works offline** during outdoor activities
- **Under 5MB storage** for 1 year of daily workouts
- **Sub-second queries** for historical analysis
- **Privacy-first**: Data stays on device by default

---

## 🖥️ Application Monitoring: SaaS Platform

### The Challenge

A SaaS startup needed application metrics but couldn't justify Datadog/New Relic costs at their scale. They wanted self-hosted observability that would grow with them.

**Constraints:**
- Small team, limited ops capacity
- Multi-tenant platform needs isolated metrics
- Grafana dashboards for visualization
- Budget: $0 for metrics infrastructure

### The Solution

Chronicle embedded in their Go backend with Prometheus-compatible API:

```go
// Initialize in main application
metricsDB, _ := chronicle.Open("app-metrics.db", chronicle.Config{
    HTTPEnabled:       true,
    HTTPPort:          9090,
    RetentionDuration: 14 * 24 * time.Hour,
})

// Multi-tenant middleware
tenants := chronicle.NewTenantManager(chronicle.TenantConfig{
    BasePath: "/data/tenants",
})

// Metrics middleware
func metricsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        tenantID := r.Header.Get("X-Tenant-ID")
        
        next.ServeHTTP(w, r)
        
        db, _ := tenants.GetTenant(tenantID)
        db.Write(chronicle.Point{
            Metric:    "http_request_duration_seconds",
            Tags:      map[string]string{"path": r.URL.Path, "method": r.Method},
            Value:     time.Since(start).Seconds(),
            Timestamp: time.Now().UnixNano(),
        })
    })
}
```

Grafana connects directly to Chronicle's Prometheus-compatible API:

```yaml
# Grafana datasource
apiVersion: 1
datasources:
  - name: Chronicle
    type: prometheus
    url: http://localhost:9090
    access: proxy
```

**Results:**
- **$0/month** vs $500+/month for hosted solutions
- **Single binary** deployment with the main application
- **Tenant isolation** without separate infrastructure
- **Seamless Grafana** integration with existing dashboards

---

## 🚗 Connected Vehicles: Fleet Telematics

### The Challenge

A fleet management company needed edge processing for vehicle telematics. Vehicles generate GPS, OBD-II, and sensor data that must be processed locally and synced efficiently.

**Constraints:**
- Cellular bandwidth is expensive
- Need local geofencing and alert logic
- Must handle 48+ hours offline (tunnels, rural areas)
- Runs on vehicle gateway with 512MB RAM

### The Solution

Chronicle on each vehicle gateway with smart sync:

```go
// Vehicle gateway configuration
db, _ := chronicle.Open("/data/vehicle.db", chronicle.Config{
    MaxMemory:         64 * 1024 * 1024,  // 64MB
    PartitionDuration: 10 * time.Minute,
    RetentionDuration: 48 * time.Hour,    // 48hr offline buffer
})

// Local geofencing
geo := chronicle.NewGeoIndex(db)
geo.AddFence("warehouse", chronicle.Polygon{...})

// Check fence violations locally
db.Subscribe(chronicle.Subscription{
    Metric: "gps_location",
    Handler: func(p chronicle.Point) {
        lat, lon := p.Tags["lat"], p.Tags["lon"]
        if !geo.Contains("warehouse", lat, lon) {
            triggerLocalAlert("Vehicle left warehouse zone")
        }
    },
})

// Smart sync: only send aggregated data when connected
replicator := chronicle.NewReplicator(db, chronicle.ReplicatorConfig{
    RemoteURL: "https://fleet.example.com/ingest",
    BatchSize: 500,
    Filter: func(p chronicle.Point) bool {
        // Only sync 1-minute aggregates, not raw GPS
        return p.Tags["aggregated"] == "true"
    },
})
```

**Results:**
- **95% bandwidth reduction** by syncing aggregates only
- **Zero data loss** during 36-hour tunnel transit
- **Real-time geofencing** without cellular latency
- **$15/month cellular** vs $150/month with full telemetry

---

## 🏥 Healthcare: Patient Monitoring

### The Challenge

A medical device company builds portable patient monitors. Vital signs must be stored reliably with strict data integrity requirements.

**Constraints:**
- FDA compliance requires data integrity
- Encryption at rest for HIPAA
- Must work during hospital WiFi outages
- Battery-powered devices with power constraints

### The Solution

Chronicle with encryption and verified storage:

```go
db, _ := chronicle.Open("/data/patient-vitals.db", chronicle.Config{
    MaxMemory:         32 * 1024 * 1024,
    PartitionDuration: 5 * time.Minute,
    RetentionDuration: 24 * time.Hour,
    
    // HIPAA compliance
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     true,
        KeyPassword: deviceEncryptionKey,
    },
    
    // Data integrity
    SyncInterval: time.Second, // Frequent sync for critical data
})

// Batch writes for power efficiency
func collectVitals(db *chronicle.DB, patientID string) {
    batch := make([]chronicle.Point, 0, 60)
    ticker := time.NewTicker(time.Second)
    
    for range ticker.C {
        batch = append(batch, chronicle.Point{
            Metric:    "heart_rate",
            Tags:      map[string]string{"patient_id": patientID},
            Value:     readHeartRate(),
            Timestamp: time.Now().UnixNano(),
        })
        
        if len(batch) >= 60 {
            db.WriteBatch(batch) // Single write per minute
            batch = batch[:0]
        }
    }
}
```

**Results:**
- **AES-256-GCM encryption** meets HIPAA requirements
- **WAL durability** ensures no vital sign data lost
- **30% longer battery life** with batched writes
- **FDA audit passed** with Chronicle's data integrity guarantees

---

## Share Your Story

Using Chronicle in production? We'd love to hear about it!

- [Open a discussion](https://github.com/chronicle-db/chronicle/discussions) to share your experience
- Write a blog post and we'll link to it
- Contribute a case study to this page

Your story helps others understand how Chronicle can solve their challenges.
