// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

type OpenAPISource struct{}

func (OpenAPISource) Name() string { return "openapi" }

func (OpenAPISource) Detect(filename string, data []byte) bool {
	if !strings.EqualFold(filepath.Ext(filename), ".json") {
		return false
	}
	var probe struct {
		OpenAPI string `json:"openapi"`
	}
	return json.Unmarshal(data, &probe) == nil && probe.OpenAPI != ""
}

func (OpenAPISource) Parse(data []byte) (*ir.Model, error) {
	return ParseOpenAPIJSON(data)
}
