// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/generated/fidelity"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// API exposes REST endpoints for the DevCloud admin.
type API struct {
	registry     *plugin.Registry
	logCollector *LogCollector
	unrouted     *UnroutedCollector
}

// NewAPI creates a new API. A nil unrouted collector is allowed; the unrouted
// route then reports an empty result rather than 404ing, so a caller reading the
// endpoint cannot mistake "not collecting" for "nothing was asked for" — the
// distinction is visible in maxServiceIds.
func NewAPI(registry *plugin.Registry, logCollector *LogCollector, unrouted *UnroutedCollector) *API {
	return &API{
		registry:     registry,
		logCollector: logCollector,
		unrouted:     unrouted,
	}
}

// Handler returns an http.Handler that serves all /devcloud/api/* routes.
func (d *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/devcloud/api/services", d.handleServices)
	mux.HandleFunc("/devcloud/api/services/", d.handleServiceResources)
	mux.HandleFunc("/devcloud/api/logs", d.handleLogs)
	mux.HandleFunc("/devcloud/api/fidelity", d.handleFidelity)
	mux.HandleFunc("/devcloud/api/unrouted", d.handleUnrouted)

	return mux
}

// handleUnrouted handles GET /devcloud/api/unrouted.
//
// It answers "what did callers ask this DevCloud for that it does not
// register?" — the demand signal the coverage roadmap is gated on.
//
// It is not a complete census of unserved traffic, and docs/coverage.md says so
// where the endpoint is documented. gateway.DetectProtocol classifies anything
// it cannot identify as ("rest-xml", "s3"), and s3 is registered, so an
// unrecognisable request is routed to S3 rather than counted here. What this
// does see is the case that matters: an SDK or CLI call to a real but
// unregistered AWS service signs with that service's own name and misses the
// registry.
func (d *API) handleUnrouted(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, d.unrouted.Snapshot())
}

// fidelityService is one service's entry in GET /devcloud/api/fidelity.
type fidelityService struct {
	ModelBacked bool              `json:"modelBacked"`
	Counts      map[string]int    `json:"counts"`
	Operations  map[string]string `json:"operations,omitempty"`
}

// handleFidelity handles GET /devcloud/api/fidelity[?service=s3].
//
// Without a service filter it returns per-service tier counts only: the full
// manifest is ~7,000 operations, too much to push at a caller whose usual
// question is "how much of this service can I trust?". Naming a service adds
// that service's per-operation tiers.
func (d *API) handleFidelity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if id := r.URL.Query().Get("service"); id != "" {
		svc, ok := fidelity.Services[id]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown service: " + id})
			return
		}
		writeJSON(w, http.StatusOK, map[string]fidelityService{id: fidelitySummary(svc, true)})
		return
	}

	out := make(map[string]fidelityService, len(fidelity.Services))
	for id, svc := range fidelity.Services {
		out[id] = fidelitySummary(svc, false)
	}
	writeJSON(w, http.StatusOK, out)
}

func fidelitySummary(svc fidelity.Service, withOperations bool) fidelityService {
	entry := fidelityService{
		ModelBacked: svc.ModelBacked,
		Counts:      map[string]int{},
	}
	if withOperations {
		entry.Operations = make(map[string]string, len(svc.Operations))
	}
	for op, tier := range svc.Operations {
		entry.Counts[string(tier)]++
		if withOperations {
			entry.Operations[op] = string(tier)
		}
	}
	return entry
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
func (d *API) handleServices(w http.ResponseWriter, r *http.Request) {
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
func (d *API) handleServiceResources(w http.ResponseWriter, r *http.Request) {
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

// handleLogs handles GET /devcloud/api/logs.
func (d *API) handleLogs(w http.ResponseWriter, r *http.Request) {
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
