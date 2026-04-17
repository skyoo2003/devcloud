// SPDX-License-Identifier: Apache-2.0

package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// DashboardAPI exposes REST endpoints for the DevCloud dashboard.
type DashboardAPI struct {
	registry     *plugin.Registry
	logCollector *LogCollector
}

// NewDashboardAPI creates a new DashboardAPI.
func NewDashboardAPI(registry *plugin.Registry, logCollector *LogCollector) *DashboardAPI {
	return &DashboardAPI{
		registry:     registry,
		logCollector: logCollector,
	}
}

// Handler returns an http.Handler that serves all /devcloud/api/* routes.
func (d *DashboardAPI) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/devcloud/api/services", d.handleServices)
	mux.HandleFunc("/devcloud/api/services/", d.handleServiceResources)
	mux.HandleFunc("/devcloud/api/metrics", d.handleMetrics)
	mux.HandleFunc("/devcloud/api/metrics/", d.handleServiceMetrics)
	mux.HandleFunc("/devcloud/api/logs", d.handleLogs)

	return mux
}

// writeJSON serialises v as JSON and writes it to w with the appropriate
// Content-Type header.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// serviceInfo is the per-service entry returned by GET /devcloud/api/services.
type serviceInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	ResourceCount int    `json:"resourceCount"`
}

// handleServices handles GET /devcloud/api/services.
func (d *DashboardAPI) handleServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ids := d.registry.ActiveServices()
	ctx := context.Background()
	services := make([]serviceInfo, 0, len(ids))

	for _, id := range ids {
		p, ok := d.registry.Get(id)
		if !ok {
			continue
		}

		resources, _ := p.ListResources(ctx)
		// GetMetrics is called but we only use the resource count from
		// ListResources here; stats are available via /devcloud/api/metrics.
		_, _ = p.GetMetrics(ctx)

		services = append(services, serviceInfo{
			ID:            p.ServiceID(),
			Name:          p.ServiceName(),
			Status:        "active",
			ResourceCount: len(resources),
		})
	}

	writeJSON(w, http.StatusOK, services)
}

// handleServiceResources handles GET /devcloud/api/services/{service}/resources.
func (d *DashboardAPI) handleServiceResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Strip the prefix and extract the service ID.
	// Path: /devcloud/api/services/{service}/resources
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/devcloud/api/services/"), "/")
	serviceID := parts[0]

	p, ok := d.registry.Get(serviceID)
	if !ok {
		http.Error(w, "service not found", http.StatusNotFound)
		return
	}

	resources, err := p.ListResources(context.Background())
	if err != nil {
		http.Error(w, "failed to list resources", http.StatusInternalServerError)
		return
	}
	if resources == nil {
		resources = []plugin.Resource{}
	}

	writeJSON(w, http.StatusOK, resources)
}

// aggregateMetrics is the payload for GET /devcloud/api/metrics.
type aggregateMetrics struct {
	TotalRequests int64 `json:"totalRequests"`
	ErrorCount    int64 `json:"errorCount"`
	Services      int   `json:"services"`
}

// handleMetrics handles GET /devcloud/api/metrics.
func (d *DashboardAPI) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ids := d.registry.ActiveServices()
	ctx := context.Background()

	var totalRequests, errorCount int64
	for _, id := range ids {
		p, ok := d.registry.Get(id)
		if !ok {
			continue
		}
		m, err := p.GetMetrics(ctx)
		if err != nil || m == nil {
			continue
		}
		totalRequests += m.TotalRequests
		errorCount += m.ErrorCount
	}

	writeJSON(w, http.StatusOK, aggregateMetrics{
		TotalRequests: totalRequests,
		ErrorCount:    errorCount,
		Services:      len(ids),
	})
}

// handleServiceMetrics handles GET /devcloud/api/metrics/{service}.
func (d *DashboardAPI) handleServiceMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	serviceID := strings.TrimPrefix(r.URL.Path, "/devcloud/api/metrics/")

	p, ok := d.registry.Get(serviceID)
	if !ok {
		http.Error(w, "service not found", http.StatusNotFound)
		return
	}

	m, err := p.GetMetrics(context.Background())
	if err != nil {
		http.Error(w, "failed to get metrics", http.StatusInternalServerError)
		return
	}
	if m == nil {
		m = &plugin.ServiceMetrics{}
	}

	writeJSON(w, http.StatusOK, m)
}

// handleLogs handles GET /devcloud/api/logs.
func (d *DashboardAPI) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	logs := d.logCollector.Recent(limit)
	writeJSON(w, http.StatusOK, logs)
}
