// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPlugin struct{}

func (m *mockPlugin) ServiceID() string                  { return "s3" }
func (m *mockPlugin) ServiceName() string                { return "Amazon S3" }
func (m *mockPlugin) Protocol() ProtocolType             { return ProtocolRESTXML }
func (m *mockPlugin) Init(cfg PluginConfig) error        { return nil }
func (m *mockPlugin) Shutdown(ctx context.Context) error { return nil }
func (m *mockPlugin) HandleRequest(ctx context.Context, op string, req *http.Request) (*Response, error) {
	return &Response{StatusCode: 200, Body: []byte("ok")}, nil
}
func (m *mockPlugin) ListResources(ctx context.Context) ([]Resource, error)   { return nil, nil }
func (m *mockPlugin) GetMetrics(ctx context.Context) (*ServiceMetrics, error) { return nil, nil }

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	reg.Register("s3", func(cfg PluginConfig) ServicePlugin { return &mockPlugin{} })

	plugin, err := reg.Init("s3", PluginConfig{})
	require.NoError(t, err)
	assert.Equal(t, "s3", plugin.ServiceID())
}

func TestRegistryGetUnknown(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Init("nonexistent", PluginConfig{})
	assert.Error(t, err)
}

func TestRegistryList(t *testing.T) {
	reg := NewRegistry()
	reg.Register("s3", func(cfg PluginConfig) ServicePlugin { return &mockPlugin{} })
	_, err := reg.Init("s3", PluginConfig{})
	require.NoError(t, err)
	assert.Equal(t, []string{"s3"}, reg.ActiveServices())
}
