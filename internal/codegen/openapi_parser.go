// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

type openAPIDoc struct {
	OpenAPI string `json:"openapi"`
	Info    struct {
		Title       string `json:"title"`
		Version     string `json:"version"`
		Description string `json:"description"`
		ServiceID   string `json:"x-service-id"`
		Provider    string `json:"x-provider"`
	} `json:"info"`
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		Schemas map[string]openAPISchema `json:"schemas"`
	} `json:"components"`
}

type openAPIOperation struct {
	OperationID string                     `json:"operationId"`
	Description string                     `json:"description"`
	Parameters  []openAPIParam             `json:"parameters"`
	Responses   map[string]json.RawMessage `json:"responses"`
	HTTPUri     string                     `json:"x-http-uri"`
}

type openAPIParam struct {
	Name     string         `json:"name"`
	In       string         `json:"in"`
	Required bool           `json:"required"`
	Schema   *openAPISchema `json:"schema"`
}

type openAPISchema struct {
	Type        string                     `json:"type"`
	Enum        []string                   `json:"enum"`
	ErrorStatus int                        `json:"x-error-status"`
	Properties  map[string]openAPIProperty `json:"properties"`
}

type openAPIProperty struct {
	Type string `json:"type"`
}

// ParseOpenAPIJSON translates an OpenAPI 3.0/3.1 JSON model into the provider-neutral IR.
func ParseOpenAPIJSON(data []byte) (*ir.Model, error) {
	var doc openAPIDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI json: %w", err)
	}

	provider := doc.Info.Provider
	if provider == "" {
		provider = "azure"
	}

	serviceID := doc.Info.ServiceID
	if serviceID == "" {
		titleLower := strings.ToLower(doc.Info.Title)
		if strings.Contains(titleLower, "blob") {
			serviceID = "blob"
		} else {
			serviceID = strings.ReplaceAll(titleLower, " ", "")
		}
	}

	model := &ir.Model{
		Provider:    provider,
		ServiceName: doc.Info.Title,
		ServiceID:   serviceID,
		Protocol:    "rest-xml",
		Operations:  nil,
		Shapes:      make(map[string]*ir.Shape),
	}

	httpMethods := map[string]bool{
		"get":     true,
		"put":     true,
		"post":    true,
		"delete":  true,
		"head":    true,
		"patch":   true,
		"options": true,
	}

	// Parse paths and operations
	pathKeys := make([]string, 0, len(doc.Paths))
	for p := range doc.Paths {
		pathKeys = append(pathKeys, p)
	}
	sort.Strings(pathKeys)

	for _, pathKey := range pathKeys {
		methods := doc.Paths[pathKey]
		methodKeys := make([]string, 0, len(methods))
		for m := range methods {
			if httpMethods[strings.ToLower(m)] {
				methodKeys = append(methodKeys, m)
			}
		}
		sort.Strings(methodKeys)

		for _, method := range methodKeys {
			rawOp := methods[method]
			var op openAPIOperation
			if err := json.Unmarshal(rawOp, &op); err != nil {
				return nil, fmt.Errorf("failed to parse operation for %s %s: %w", method, pathKey, err)
			}

			opName := op.OperationID
			if opName == "" {
				opName = fmt.Sprintf("%s%s", strings.Title(method), sanitizeName(pathKey))
			}

			uri := op.HTTPUri
			if uri == "" {
				uri = pathKey
				if !strings.Contains(uri, "?") {
					var queryConstraints []string
					for _, param := range op.Parameters {
						if param.In == "query" && param.Required {
							if param.Schema != nil && len(param.Schema.Enum) > 0 {
								queryConstraints = append(queryConstraints, fmt.Sprintf("%s=%s", param.Name, param.Schema.Enum[0]))
							} else {
								queryConstraints = append(queryConstraints, param.Name)
							}
						}
					}
					if len(queryConstraints) > 0 {
						uri = uri + "?" + strings.Join(queryConstraints, "&")
					}
				}
			}

			inputName := opName + "Input"
			outputName := opName + "Output"

			irOp := ir.Operation{
				Name:       opName,
				InputName:  inputName,
				OutputName: outputName,
				HTTPMethod: strings.ToUpper(method),
				HTTPUri:    uri,
			}

			model.Operations = append(model.Operations, irOp)

			// Ensure input and output shapes exist
			if _, exists := model.Shapes[inputName]; !exists {
				model.Shapes[inputName] = &ir.Shape{
					Name: inputName,
					Type: ir.ShapeStructure,
				}
			}
			if _, exists := model.Shapes[outputName]; !exists {
				model.Shapes[outputName] = &ir.Shape{
					Name: outputName,
					Type: ir.ShapeStructure,
				}
			}
		}
	}

	// Parse component schemas
	for name, schema := range doc.Components.Schemas {
		shape := &ir.Shape{
			Name: name,
			Type: ir.ShapeStructure,
		}

		if schema.ErrorStatus > 0 {
			shape.ErrorTrait = &ir.ErrorTrait{
				Type:       "client",
				HTTPStatus: schema.ErrorStatus,
			}
		}

		for propName, prop := range schema.Properties {
			goType := "string"
			switch prop.Type {
			case "integer":
				goType = "int"
			case "boolean":
				goType = "bool"
			}
			shape.Members = append(shape.Members, ir.Member{
				Name:       propName,
				TargetName: propName,
				GoType:     goType,
			})
		}

		model.Shapes[name] = shape
	}

	// Sort operations:
	// Routes with more query constraints must be ordered before routes with fewer
	// query constraints so httproute.Match resolves the more specific route first.
	sort.Slice(model.Operations, func(i, j int) bool {
		qI := queryParamsCount(model.Operations[i].HTTPUri)
		qJ := queryParamsCount(model.Operations[j].HTTPUri)
		if qI != qJ {
			return qI > qJ
		}
		return model.Operations[i].Name < model.Operations[j].Name
	})

	return model, nil
}

func queryParamsCount(uri string) int {
	parts := strings.Split(uri, "?")
	if len(parts) < 2 || parts[1] == "" {
		return 0
	}
	return strings.Count(parts[1], "&") + 1
}

func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
