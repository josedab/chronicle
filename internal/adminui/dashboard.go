package adminui

import (
	"net/http"
	"runtime"
	"time"
)

func (ui *AdminUI) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	tmpl, err := dashboardTemplate()
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
	Title        string
	Uptime       string
	Version      string
	GoVersion    string
	NumCPU       int
	NumGoroutine int
	MemStats     memStatsData
	DBStats      dbStatsData
	Metrics      []string
	Config       configData
	DevMode      bool
}

type memStatsData struct {
	Alloc      string
	TotalAlloc string
	Sys        string
	NumGC      uint32
}

type dbStatsData struct {
	MetricCount    int
	PartitionCount int
	BufferSize     int
	Retention      string
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
	info := ui.db.Info()

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
			PartitionCount: info.PartitionCount,
			BufferSize:     info.BufferSize,
			Retention:      info.RetentionDuration,
		},
		Metrics: metrics,
		Config: configData{
			Path:              info.Path,
			PartitionDuration: info.PartitionDuration,
			BufferSize:        info.BufferSize,
			SyncInterval:      info.WALSyncInterval,
			Retention:         info.RetentionDuration,
		},
		DevMode: ui.devMode,
	}
}
