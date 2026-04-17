// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type PluginFactory func(cfg PluginConfig) ServicePlugin

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

	p := factory(cfg)
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
