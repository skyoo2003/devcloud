// SPDX-License-Identifier: Apache-2.0

package azureappconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestAzureAppConfig_Lifecycle(t *testing.T) {
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	ctx := context.Background()

	// 1. Set key-value without label
	putBody := []byte(`{"value":"hello-world","content_type":"text/plain","tags":{"env":"dev"}}`)
	req1 := httptest.NewRequest(http.MethodPut, "http://myconf.azconfig.io/kv/app:greeting", bytes.NewReader(putBody))
	resp1, err := p.HandleRequest(ctx, "", req1)
	if err != nil {
		t.Fatalf("HandleRequest failed: %v", err)
	}
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp1.StatusCode, string(resp1.Body))
	}

	var kv1 KeyValue
	if err := json.Unmarshal(resp1.Body, &kv1); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if kv1.Key != "app:greeting" || kv1.Value != "hello-world" || kv1.Label != nil {
		t.Fatalf("unexpected kv: %+v", kv1)
	}
	if resp1.Headers["ETag"] == "" {
		t.Fatalf("expected ETag header")
	}

	// 2. Set key-value with label
	putBodyProd := []byte(`{"value":"hello-prod"}`)
	reqProd := httptest.NewRequest(http.MethodPut, "http://myconf.azconfig.io/kv/app:greeting?label=prod", bytes.NewReader(putBodyProd))
	respProd, err := p.HandleRequest(ctx, "", reqProd)
	if err != nil {
		t.Fatalf("Set with label failed: %v", err)
	}
	if respProd.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", respProd.StatusCode, string(respProd.Body))
	}
	var kvProd KeyValue
	if err := json.Unmarshal(respProd.Body, &kvProd); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if kvProd.Label == nil || *kvProd.Label != "prod" || kvProd.Value != "hello-prod" {
		t.Fatalf("unexpected kvProd: %+v", kvProd)
	}

	// 3. Get key-value without label
	req2 := httptest.NewRequest(http.MethodGet, "http://myconf.azconfig.io/kv/app:greeting", nil)
	resp2, err := p.HandleRequest(ctx, "", req2)
	if err != nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got status=%d err=%v", resp2.StatusCode, err)
	}
	var kv2 KeyValue
	_ = json.Unmarshal(resp2.Body, &kv2)
	if kv2.Value != "hello-world" {
		t.Fatalf("expected hello-world, got %s", kv2.Value)
	}

	// 4. Get key-value with label
	reqProdGet := httptest.NewRequest(http.MethodGet, "http://myconf.azconfig.io/kv/app:greeting?label=prod", nil)
	respProdGet, err := p.HandleRequest(ctx, "", reqProdGet)
	if err != nil || respProdGet.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got status=%d err=%v", respProdGet.StatusCode, err)
	}
	var kvProdGet KeyValue
	_ = json.Unmarshal(respProdGet.Body, &kvProdGet)
	if kvProdGet.Value != "hello-prod" {
		t.Fatalf("expected hello-prod, got %s", kvProdGet.Value)
	}

	// 5. List key-values
	reqList := httptest.NewRequest(http.MethodGet, "http://myconf.azconfig.io/kv", nil)
	respList, err := p.HandleRequest(ctx, "", reqList)
	if err != nil || respList.StatusCode != http.StatusOK {
		t.Fatalf("List failed: %v", err)
	}
	var listRes struct {
		Items []KeyValue `json:"items"`
	}
	if err := json.Unmarshal(respList.Body, &listRes); err != nil {
		t.Fatalf("unmarshal list failed: %v", err)
	}
	if len(listRes.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(listRes.Items))
	}

	// 6. Delete key-value
	reqDel := httptest.NewRequest(http.MethodDelete, "http://myconf.azconfig.io/kv/app:greeting", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	if err != nil || respDel.StatusCode != http.StatusOK {
		t.Fatalf("Delete failed: %v", err)
	}

	// 7. Get deleted key-value returns 404
	respGet404, _ := p.HandleRequest(ctx, "", req2)
	if respGet404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", respGet404.StatusCode)
	}

	// 8. Delete again returns 204
	respDel2, _ := p.HandleRequest(ctx, "", reqDel)
	if respDel2.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", respDel2.StatusCode)
	}
}
