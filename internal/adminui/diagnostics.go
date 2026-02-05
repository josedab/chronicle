package adminui

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"
)

func (ui *AdminUI) handleAPIDiagnostics(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	info := ui.db.Info()

	// Get disk usage (approximation based on config path)
	var diskUsage int64
	if fileInfo, err := os.Stat(info.Path); err == nil {
		diskUsage = fileInfo.Size()
	}

	diagnostics := map[string]interface{}{
		"system": map[string]interface{}{
			"go_version":   runtime.Version(),
			"os":           runtime.GOOS,
			"arch":         runtime.GOARCH,
			"num_cpu":      runtime.NumCPU(),
			"num_cgo_call": runtime.NumCgoCall(),
		},
		"runtime": map[string]interface{}{
			"goroutines":      runtime.NumGoroutine(),
			"gc_pause_total":  time.Duration(memStats.PauseTotalNs).String(),
			"gc_num":          memStats.NumGC,
			"gc_last":         time.Unix(0, int64(memStats.LastGC)).Format(time.RFC3339),
			"gc_cpu_fraction": fmt.Sprintf("%.4f%%", memStats.GCCPUFraction*100),
		},
		"memory": map[string]interface{}{
			"alloc":         formatBytes(memStats.Alloc),
			"total_alloc":   formatBytes(memStats.TotalAlloc),
			"sys":           formatBytes(memStats.Sys),
			"heap_alloc":    formatBytes(memStats.HeapAlloc),
			"heap_sys":      formatBytes(memStats.HeapSys),
			"heap_idle":     formatBytes(memStats.HeapIdle),
			"heap_inuse":    formatBytes(memStats.HeapInuse),
			"heap_released": formatBytes(memStats.HeapReleased),
			"heap_objects":  memStats.HeapObjects,
			"stack_inuse":   formatBytes(memStats.StackInuse),
			"stack_sys":     formatBytes(memStats.StackSys),
		},
		"database": map[string]interface{}{
			"path":          info.Path,
			"disk_usage":    formatBytes(uint64(diskUsage)),
			"metrics_count": len(ui.db.Metrics()),
			"uptime":        time.Since(ui.startTime).Round(time.Second).String(),
			"buffer_size":   info.BufferSize,
		},
	}

	writeJSON(w, diagnostics)
}
