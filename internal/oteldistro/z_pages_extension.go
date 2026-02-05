package oteldistro

import (
	"context"
	"net/http"
)

func (e *ZPagesExtension) Start(ctx context.Context, host Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/tracez", e.handleTracez)
	mux.HandleFunc("/debug/rpcz", e.handleRpcz)
	mux.HandleFunc("/debug/servicez", e.handleServicez)

	e.server = &http.Server{
		Addr:    e.config.Endpoint,
		Handler: mux,
	}

	go e.server.ListenAndServe()
	return nil
}

func (e *ZPagesExtension) Shutdown(ctx context.Context) error {
	if e.server != nil {
		return e.server.Shutdown(ctx)
	}
	return nil
}

func (e *ZPagesExtension) handleTracez(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Trace information"))
}

func (e *ZPagesExtension) handleRpcz(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("RPC information"))
}

func (e *ZPagesExtension) handleServicez(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Service information"))
}
