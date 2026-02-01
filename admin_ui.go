package chronicle

import (
	"fmt"
	"html/template"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"
)

// AdminUI provides a web-based admin interface for Chronicle.
type AdminUI struct {
	db       *DB
	mux      *http.ServeMux
	startTime time.Time
	mu       sync.RWMutex
	metrics  *adminMetrics
}

type adminMetrics struct {
	Writes       int64
	Reads        int64
	Errors       int64
	LastError    string
	LastErrorAt  time.Time
}

// AdminConfig configures the admin UI.
type AdminConfig struct {
	// Prefix is the URL prefix for admin routes (default: "/admin").
	Prefix string

	// Username for basic auth (optional).
	Username string

	// Password for basic auth (optional).
	Password string
}

// NewAdminUI creates an admin UI instance.
func NewAdminUI(db *DB, config AdminConfig) *AdminUI {
	if config.Prefix == "" {
		config.Prefix = "/admin"
	}

	ui := &AdminUI{
		db:        db,
		mux:       http.NewServeMux(),
		startTime: time.Now(),
		metrics:   &adminMetrics{},
	}

	// Register routes
	ui.mux.HandleFunc(config.Prefix, ui.handleDashboard)
	ui.mux.HandleFunc(config.Prefix+"/", ui.handleDashboard)
	ui.mux.HandleFunc(config.Prefix+"/api/stats", ui.handleAPIStats)
	ui.mux.HandleFunc(config.Prefix+"/api/metrics", ui.handleAPIMetrics)
	ui.mux.HandleFunc(config.Prefix+"/api/series", ui.handleAPISeries)
	ui.mux.HandleFunc(config.Prefix+"/api/query", ui.handleAPIQuery)
	ui.mux.HandleFunc(config.Prefix+"/api/config", ui.handleAPIConfig)
	ui.mux.HandleFunc(config.Prefix+"/api/health", ui.handleAPIHealth)

	return ui
}

// Handler returns the HTTP handler for the admin UI.
func (ui *AdminUI) Handler() http.Handler {
	return ui.mux
}

// ServeHTTP implements http.Handler.
func (ui *AdminUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ui.mux.ServeHTTP(w, r)
}

func (ui *AdminUI) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	
	tmpl, err := template.New("dashboard").Parse(adminDashboardHTML)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := ui.getDashboardData()
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type dashboardData struct {
	Title       string
	Uptime      string
	Version     string
	GoVersion   string
	NumCPU      int
	NumGoroutine int
	MemStats    memStatsData
	DBStats     dbStatsData
	Metrics     []string
	Config      configData
}

type memStatsData struct {
	Alloc      string
	TotalAlloc string
	Sys        string
	NumGC      uint32
}

type dbStatsData struct {
	MetricCount     int
	PartitionCount  int
	BufferSize      int
	Retention       string
}

type configData struct {
	Path              string
	PartitionDuration string
	BufferSize        int
	SyncInterval      string
	Retention         string
}

func (ui *AdminUI) getDashboardData() dashboardData {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := ui.db.Metrics()
	
	partitionCount := 0
	if ui.db.index != nil {
		partitionCount = ui.db.index.Count()
	}

	return dashboardData{
		Title:        "Chronicle Admin",
		Uptime:       time.Since(ui.startTime).Round(time.Second).String(),
		Version:      "1.0.0",
		GoVersion:    runtime.Version(),
		NumCPU:       runtime.NumCPU(),
		NumGoroutine: runtime.NumGoroutine(),
		MemStats: memStatsData{
			Alloc:      formatBytes(m.Alloc),
			TotalAlloc: formatBytes(m.TotalAlloc),
			Sys:        formatBytes(m.Sys),
			NumGC:      m.NumGC,
		},
		DBStats: dbStatsData{
			MetricCount:    len(metrics),
			PartitionCount: partitionCount,
			BufferSize:     ui.db.config.BufferSize,
			Retention:      ui.db.config.RetentionDuration.String(),
		},
		Metrics: metrics,
		Config: configData{
			Path:              ui.db.config.Path,
			PartitionDuration: ui.db.config.PartitionDuration.String(),
			BufferSize:        ui.db.config.BufferSize,
			SyncInterval:      ui.db.config.SyncInterval.String(),
			Retention:         ui.db.config.RetentionDuration.String(),
		},
	}
}

func (ui *AdminUI) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	partitionCount := 0
	if ui.db.index != nil {
		partitionCount = ui.db.index.Count()
	}

	stats := map[string]interface{}{
		"uptime":         time.Since(ui.startTime).Seconds(),
		"version":        "1.0.0",
		"go_version":     runtime.Version(),
		"num_cpu":        runtime.NumCPU(),
		"num_goroutine":  runtime.NumGoroutine(),
		"metric_count":   len(ui.db.Metrics()),
		"partition_count": partitionCount,
		"memory": map[string]uint64{
			"alloc":       m.Alloc,
			"total_alloc": m.TotalAlloc,
			"sys":         m.Sys,
			"num_gc":      uint64(m.NumGC),
		},
	}

	writeJSON(w, stats)
}

func (ui *AdminUI) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ui.db.Metrics()
	sort.Strings(metrics)

	result := make([]map[string]interface{}, len(metrics))
	for i, m := range metrics {
		result[i] = map[string]interface{}{
			"name": m,
		}
	}

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPISeries(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	
	var series []map[string]interface{}
	
	if metric != "" {
		series = append(series, map[string]interface{}{
			"metric": metric,
		})
	} else {
		for _, m := range ui.db.Metrics() {
			series = append(series, map[string]interface{}{
				"metric": m,
			})
		}
	}

	writeJSON(w, series)
}

func (ui *AdminUI) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	parser := &QueryParser{}
	q, err := parser.Parse(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"path":               ui.db.config.Path,
		"partition_duration": ui.db.config.PartitionDuration.String(),
		"buffer_size":        ui.db.config.BufferSize,
		"sync_interval":      ui.db.config.SyncInterval.String(),
		"retention":          ui.db.config.RetentionDuration.String(),
	}

	writeJSON(w, config)
}

func (ui *AdminUI) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"uptime": time.Since(ui.startTime).Seconds(),
	}

	// Check if database is responsive
	if ui.db.closed {
		health["status"] = "unhealthy"
		health["error"] = "database closed"
	}

	writeJSON(w, health)
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

const adminDashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{.Title}}</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #0d1117;
            color: #c9d1d9;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 0;
            border-bottom: 1px solid #30363d;
            margin-bottom: 30px;
        }
        h1 {
            font-size: 24px;
            color: #58a6ff;
        }
        .status {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #3fb950;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
        }
        .card h2 {
            font-size: 14px;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 15px;
        }
        .stat-value {
            font-size: 32px;
            font-weight: 600;
            color: #f0f6fc;
        }
        .stat-label {
            font-size: 14px;
            color: #8b949e;
        }
        .stat-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #30363d;
        }
        .stat-row:last-child {
            border-bottom: none;
        }
        .metrics-list {
            max-height: 300px;
            overflow-y: auto;
        }
        .metric-item {
            padding: 10px;
            background: #0d1117;
            border-radius: 4px;
            margin-bottom: 8px;
            font-family: monospace;
        }
        .config-table {
            width: 100%;
        }
        .config-table td {
            padding: 8px 0;
            border-bottom: 1px solid #30363d;
        }
        .config-table td:first-child {
            color: #8b949e;
        }
        .config-table td:last-child {
            text-align: right;
            font-family: monospace;
        }
        .query-form {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .query-form input {
            flex: 1;
            padding: 10px 15px;
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 6px;
            color: #c9d1d9;
            font-family: monospace;
        }
        .query-form button {
            padding: 10px 20px;
            background: #238636;
            border: none;
            border-radius: 6px;
            color: white;
            cursor: pointer;
            font-weight: 600;
        }
        .query-form button:hover {
            background: #2ea043;
        }
        #queryResult {
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 15px;
            font-family: monospace;
            white-space: pre-wrap;
            max-height: 400px;
            overflow-y: auto;
        }
        .refresh-btn {
            background: #21262d;
            border: 1px solid #30363d;
            border-radius: 6px;
            color: #c9d1d9;
            padding: 8px 16px;
            cursor: pointer;
        }
        .refresh-btn:hover {
            background: #30363d;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 {{.Title}}</h1>
            <div class="status">
                <span class="status-dot"></span>
                <span>Healthy</span>
                <button class="refresh-btn" onclick="location.reload()">↻ Refresh</button>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <h2>System Info</h2>
                <div class="stat-row">
                    <span class="stat-label">Uptime</span>
                    <span>{{.Uptime}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Version</span>
                    <span>{{.Version}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Go Version</span>
                    <span>{{.GoVersion}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">CPUs</span>
                    <span>{{.NumCPU}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Goroutines</span>
                    <span>{{.NumGoroutine}}</span>
                </div>
            </div>

            <div class="card">
                <h2>Memory Usage</h2>
                <div class="stat-row">
                    <span class="stat-label">Allocated</span>
                    <span>{{.MemStats.Alloc}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Total Allocated</span>
                    <span>{{.MemStats.TotalAlloc}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">System</span>
                    <span>{{.MemStats.Sys}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">GC Cycles</span>
                    <span>{{.MemStats.NumGC}}</span>
                </div>
            </div>

            <div class="card">
                <h2>Database Stats</h2>
                <div class="stat-value">{{.DBStats.MetricCount}}</div>
                <div class="stat-label">Total Metrics</div>
                <br>
                <div class="stat-row">
                    <span class="stat-label">Partitions</span>
                    <span>{{.DBStats.PartitionCount}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Buffer Size</span>
                    <span>{{.DBStats.BufferSize}}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Retention</span>
                    <span>{{.DBStats.Retention}}</span>
                </div>
            </div>

            <div class="card">
                <h2>Configuration</h2>
                <table class="config-table">
                    <tr>
                        <td>Path</td>
                        <td>{{.Config.Path}}</td>
                    </tr>
                    <tr>
                        <td>Partition Duration</td>
                        <td>{{.Config.PartitionDuration}}</td>
                    </tr>
                    <tr>
                        <td>Buffer Size</td>
                        <td>{{.Config.BufferSize}}</td>
                    </tr>
                    <tr>
                        <td>Sync Interval</td>
                        <td>{{.Config.SyncInterval}}</td>
                    </tr>
                </table>
            </div>
        </div>

        <div class="card">
            <h2>Query Explorer</h2>
            <form class="query-form" onsubmit="executeQuery(event)">
                <input type="text" id="queryInput" placeholder="SELECT mean(value) FROM cpu WHERE host='server1' GROUP BY time(5m)" />
                <button type="submit">Execute</button>
            </form>
            <div id="queryResult">Enter a query above to see results...</div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <h2>Metrics ({{len .Metrics}})</h2>
            <div class="metrics-list">
                {{range .Metrics}}
                <div class="metric-item">{{.}}</div>
                {{else}}
                <div class="metric-item">No metrics found</div>
                {{end}}
            </div>
        </div>
    </div>

    <script>
        async function executeQuery(e) {
            e.preventDefault();
            const query = document.getElementById('queryInput').value;
            const result = document.getElementById('queryResult');
            
            result.textContent = 'Executing...';
            
            try {
                const response = await fetch('/admin/api/query?q=' + encodeURIComponent(query));
                const data = await response.json();
                result.textContent = JSON.stringify(data, null, 2);
            } catch (error) {
                result.textContent = 'Error: ' + error.message;
            }
        }

        // Auto-refresh stats every 30 seconds
        setInterval(async () => {
            try {
                const response = await fetch('/admin/api/stats');
                const stats = await response.json();
                // Update dynamic values if needed
            } catch (error) {
                console.error('Failed to refresh stats:', error);
            }
        }, 30000);
    </script>
</body>
</html>`
