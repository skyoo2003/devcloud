// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"encoding/json"
	"fmt"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// ParseOpenAPIJSON provides a proof-of-concept OpenAPI parser.
// In a full implementation, this would read the paths, components/schemas,
// and translate them into ir.Model, ir.Operation, and ir.Shape.
func ParseOpenAPIJSON(data []byte) (*ir.Model, error) {
	var doc struct {
		Info struct {
			Title string `json:"title"`
		} `json:"info"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI json: %v", err)
	}

	// This is a stub returning a minimal model for proof-of-concept.
	// We hardcode the azure Blob service ID to satisfy the pipeline until
	// full mapping is implemented.
	model := &ir.Model{
		Provider:    "azure",
		ServiceName: doc.Info.Title,
		ServiceID:   "blob",
		Protocol:    "rest-json",
		Operations: []ir.Operation{
			{
				Name:       "CreateContainer",
				InputName:  "CreateContainerInput",
				OutputName: "CreateContainerOutput",
			},
		},
		Shapes: map[string]*ir.Shape{
			"CreateContainerInput":  {Name: "CreateContainerInput", Type: ir.ShapeStructure},
			"CreateContainerOutput": {Name: "CreateContainerOutput", Type: ir.ShapeStructure},
		},
	}

	return model, nil
}
