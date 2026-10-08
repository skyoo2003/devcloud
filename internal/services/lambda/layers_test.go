// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestLambdaLayers(t *testing.T) {
	tmpDir := t.TempDir()
	p := &LambdaProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. PublishLayerVersion
	contentZip := base64.StdEncoding.EncodeToString([]byte("fake zip content"))
	bodyPublish := map[string]any{
		"Content": map[string]any{
			"ZipFile": contentZip,
		},
		"Description":        "My test layer",
		"CompatibleRuntimes": []string{"python3.12"},
		"LicenseInfo":        "Apache-2.0",
	}
	bodyBytes, _ := json.Marshal(bodyPublish)
	reqPublish := httptest.NewRequest(http.MethodPost, "/2018-10-31/layers/my-layer/versions", bytes.NewReader(bodyBytes))
	resp, err := p.HandleRequest(ctx, "PublishLayerVersion", reqPublish)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var pubOut map[string]any
	err = json.Unmarshal(resp.Body, &pubOut)
	require.NoError(t, err)
	assert.Equal(t, float64(1), pubOut["Version"])
	assert.Equal(t, "My test layer", pubOut["Description"])
	assert.Contains(t, pubOut["LayerVersionArn"], "my-layer:1")

	// 2. AddLayerVersionPermission
	bodyPerm := map[string]any{
		"Action":      "lambda:GetLayerVersion",
		"Principal":   "*",
		"StatementId": "public-access",
	}
	permBytes, _ := json.Marshal(bodyPerm)
	reqPerm := httptest.NewRequest(http.MethodPost, "/2018-10-31/layers/my-layer/versions/1/policy", bytes.NewReader(permBytes))
	resp, err = p.HandleRequest(ctx, "AddLayerVersionPermission", reqPerm)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// 3. RemoveLayerVersionPermission
	reqRemove := httptest.NewRequest(http.MethodDelete, "/2018-10-31/layers/my-layer/versions/1/policy/public-access", nil)
	resp, err = p.HandleRequest(ctx, "RemoveLayerVersionPermission", reqRemove)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}
