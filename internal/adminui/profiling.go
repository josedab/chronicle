package adminui

import (
	"net/http"
	"runtime"
	"time"
)

// Phase 11: Performance Profiling API
func (ui *AdminUI) handleAPIProfiling(w http.ResponseWriter, r *http.Request) {
	profileType := r.URL.Query().Get("type")
	if profileType == "" {
		profileType = "summary"
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	switch profileType {
	case "summary":
		writeJSON(w, map[string]interface{}{
			"goroutines":     runtime.NumGoroutine(),
			"cgo_calls":      runtime.NumCgoCall(),
			"cpu_count":      runtime.NumCPU(),
			"gc_runs":        memStats.NumGC,
			"gc_pause_total": time.Duration(memStats.PauseTotalNs).String(),
			"gc_pause_last":  time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256]).String(),
			"heap_alloc":     memStats.HeapAlloc,
			"heap_sys":       memStats.HeapSys,
			"heap_idle":      memStats.HeapIdle,
			"heap_inuse":     memStats.HeapInuse,
			"heap_objects":   memStats.HeapObjects,
			"stack_inuse":    memStats.StackInuse,
			"stack_sys":      memStats.StackSys,
			"mspan_inuse":    memStats.MSpanInuse,
			"mcache_inuse":   memStats.MCacheInuse,
			"other_sys":      memStats.OtherSys,
		})

	case "gc":
		// GC statistics
		gcPauses := make([]int64, 0, 256)
		for i := uint32(0); i < memStats.NumGC && i < 256; i++ {
			gcPauses = append(gcPauses, int64(memStats.PauseNs[i]))
		}
		writeJSON(w, map[string]interface{}{
			"num_gc":          memStats.NumGC,
			"pause_total_ns":  memStats.PauseTotalNs,
			"pause_end":       memStats.PauseEnd[:min(int(memStats.NumGC), 256)],
			"pause_ns":        gcPauses,
			"gc_cpu_fraction": memStats.GCCPUFraction,
			"last_gc":         time.Unix(0, int64(memStats.LastGC)).Format(time.RFC3339),
			"next_gc":         memStats.NextGC,
			"enable_gc":       memStats.EnableGC,
			"debug_gc":        memStats.DebugGC,
		})

	case "memory":
		// Detailed memory breakdown
		writeJSON(w, map[string]interface{}{
			"alloc":         memStats.Alloc,
			"total_alloc":   memStats.TotalAlloc,
			"sys":           memStats.Sys,
			"lookups":       memStats.Lookups,
			"mallocs":       memStats.Mallocs,
			"frees":         memStats.Frees,
			"heap_alloc":    memStats.HeapAlloc,
			"heap_sys":      memStats.HeapSys,
			"heap_idle":     memStats.HeapIdle,
			"heap_inuse":    memStats.HeapInuse,
			"heap_released": memStats.HeapReleased,
			"heap_objects":  memStats.HeapObjects,
			"stack_inuse":   memStats.StackInuse,
			"stack_sys":     memStats.StackSys,
			"mspan_inuse":   memStats.MSpanInuse,
			"mspan_sys":     memStats.MSpanSys,
			"mcache_inuse":  memStats.MCacheInuse,
			"mcache_sys":    memStats.MCacheSys,
			"buck_hash_sys": memStats.BuckHashSys,
			"gc_sys":        memStats.GCSys,
			"other_sys":     memStats.OtherSys,
		})

	case "goroutines":
		// Goroutine info
		writeJSON(w, map[string]interface{}{
			"count":      runtime.NumGoroutine(),
			"gomaxprocs": runtime.GOMAXPROCS(0),
			"num_cpu":    runtime.NumCPU(),
		})

	default:
		http.Error(w, "Invalid profile type. Use: summary, gc, memory, goroutines", http.StatusBadRequest)
	}
}
