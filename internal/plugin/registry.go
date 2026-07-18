// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type PluginFactory func() ServicePlugin

type Registry struct {
	mu        sync.RWMutex
	factories map[string]PluginFactory
	active    map[string]ServicePlugin
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]PluginFactory),
		active:    make(map[string]ServicePlugin),
	}
}

// DefaultRegistry is the global service registry used by auto-registration.
// Services register themselves via init() functions.
var DefaultRegistry = NewRegistry()

func (r *Registry) Register(serviceID string, factory PluginFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[serviceID] = factory
}

func (r *Registry) Init(serviceID string, cfg PluginConfig) (ServicePlugin, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	factory, ok := r.factories[serviceID]
	if !ok {
		return nil, fmt.Errorf("unknown service: %s", serviceID)
	}

	p := factory()
	if err := p.Init(cfg); err != nil {
		return nil, fmt.Errorf("init %s: %w", serviceID, err)
	}

	r.active[serviceID] = p
	return p, nil
}

func (r *Registry) Get(serviceID string) (ServicePlugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.active[serviceID]
	return p, ok
}

// RegisteredServices returns the sorted IDs of every service factory that has
// been registered, whether or not it has been initialized. Useful for
// enumerating the full plugin surface (e.g. conformance checks).
func (r *Registry) RegisteredServices() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.factories))
	for id := range r.factories {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Construct builds a fresh plugin instance for serviceID via its factory,
// without calling Init. Returns nil,false if the service is not registered.
// Only the metadata methods (ServiceID/ServiceName/Protocol) are guaranteed
// usable on the returned value; anything requiring state needs Init first.
func (r *Registry) Construct(serviceID string) (ServicePlugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[serviceID]
	if !ok {
		return nil, false
	}
	return f(), true
}

func (r *Registry) ActiveServices() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.active))
	for id := range r.active {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *Registry) ShutdownAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for id, p := range r.active {
		if err := p.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}
