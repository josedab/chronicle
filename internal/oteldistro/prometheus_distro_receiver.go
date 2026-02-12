package oteldistro

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

func (r *PrometheusDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	r.ticker = time.NewTicker(15 * time.Second)
	go r.scrapeLoop(ctx)

	r.running = true
	return nil
}

func (r *PrometheusDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ticker != nil {
		r.ticker.Stop()
	}

	r.running = false
	return nil
}

func (r *PrometheusDistroReceiver) scrapeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.ticker.C:
			r.scrapeTargets()
		}
	}
}

func (r *PrometheusDistroReceiver) scrapeTargets() {

	for _, sc := range r.config.ScrapeConfigs {
		for _, static := range sc.StaticConfigs {
			for _, target := range static.Targets {
				r.scrapeTarget(target, sc.JobName, static.Labels)
			}
		}
	}
}

func (r *PrometheusDistroReceiver) scrapeTarget(target, job string, labels map[string]string) {

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", target))
	if err != nil {
		return
	}
	defer resp.Body.Close()

}
