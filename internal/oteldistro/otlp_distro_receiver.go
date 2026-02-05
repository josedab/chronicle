package oteldistro

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func (r *OTLPDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", r.handleMetrics)

	endpoint := "0.0.0.0:4318"
	if r.config.Protocols.HTTP != nil {
		endpoint = r.config.Protocols.HTTP.Endpoint
	}

	r.server = &http.Server{
		Addr:    endpoint,
		Handler: mux,
	}

	go r.server.ListenAndServe()

	r.running = true
	return nil
}

func (r *OTLPDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	if r.server != nil {
		r.server.Shutdown(ctx)
	}

	r.running = false
	return nil
}

func (r *OTLPDistroReceiver) handleMetrics(w http.ResponseWriter, req *http.Request) {
	// Parse and push metrics
	var otlpReq otlpExportRequest
	if err := json.NewDecoder(req.Body).Decode(&otlpReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metrics := r.convertOTLPToMetrics(&otlpReq)
	r.distro.PushMetrics(metrics)

	w.WriteHeader(http.StatusOK)
}

func (r *OTLPDistroReceiver) convertOTLPToMetrics(otlp *otlpExportRequest) *Metrics {
	metrics := &Metrics{}

	for _, rm := range otlp.ResourceMetrics {
		resourceMetrics := ResourceMetrics{
			Resource: Resource{Attributes: make(map[string]interface{})},
		}

		for _, attr := range rm.Resource.Attributes {
			resourceMetrics.Resource.Attributes[attr.Key] = otlpAttrValueToString(attr.Value)
		}

		for _, sm := range rm.ScopeMetrics {
			scopeMetrics := ScopeMetrics{
				Scope: InstrumentationScope{
					Name:    sm.Scope.Name,
					Version: sm.Scope.Version,
				},
			}

			for _, m := range sm.Metrics {
				scopeMetrics.Metrics = append(scopeMetrics.Metrics, Metric{
					Name:        m.Name,
					Description: m.Description,
					Unit:        m.Unit,
					Data:        m,
				})
			}

			resourceMetrics.ScopeMetrics = append(resourceMetrics.ScopeMetrics, scopeMetrics)
		}

		metrics.ResourceMetrics = append(metrics.ResourceMetrics, resourceMetrics)
	}

	return metrics
}

// Local OTLP types for the distro receiver (package-private).

type otlpExportRequest struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

type otlpResourceMetrics struct {
	Resource     otlpResource     `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeMetrics struct {
	Scope   otlpScope    `json:"scope"`
	Metrics []otlpMetric `json:"metrics"`
}

type otlpScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type otlpMetric struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Unit        string `json:"unit"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}

func otlpAttrValueToString(v otlpAnyValue) string {
	if v.StringValue != "" {
		return v.StringValue
	}
	return fmt.Sprintf("%v", v)
}
