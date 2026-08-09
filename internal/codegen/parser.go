// SPDX-License-Identifier: Apache-2.0

// internal/codegen/parser.go
package codegen

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Raw JSON types for unmarshaling
type rawModel struct {
	Smithy string                     `json:"smithy"`
	Shapes map[string]json.RawMessage `json:"shapes"`
}

type rawShape struct {
	Type       string                     `json:"type"`
	Operations []rawTarget                `json:"operations"`
	Input      *rawTarget                 `json:"input"`
	Output     *rawTarget                 `json:"output"`
	Errors     []rawTarget                `json:"errors"`
	Members    map[string]rawMember       `json:"members"`
	Member     *rawMember                 `json:"member"`
	Key        *rawMember                 `json:"key"`
	Value      *rawMember                 `json:"value"`
	Version    string                     `json:"version"`
	Traits     map[string]json.RawMessage `json:"traits"`

	// Resource bindings. Services and resources both nest resources, and a
	// resource binds operations through `operations`, `collectionOperations` and
	// the lifecycle slots below.
	Resources            []rawTarget `json:"resources"`
	CollectionOperations []rawTarget `json:"collectionOperations"`
	Create               *rawTarget  `json:"create"`
	Put                  *rawTarget  `json:"put"`
	Read                 *rawTarget  `json:"read"`
	Update               *rawTarget  `json:"update"`
	Delete               *rawTarget  `json:"delete"`
	List                 *rawTarget  `json:"list"`
}

type rawTarget struct {
	Target string `json:"target"`
}

type rawMember struct {
	Target string                     `json:"target"`
	Traits map[string]json.RawMessage `json:"traits"`
}

type rawHTTPTrait struct {
	Method string `json:"method"`
	URI    string `json:"uri"`
}

func ParseSmithyJSON(data []byte) (*SmithyModel, error) {
	var raw rawModel
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal smithy json: %w", err)
	}

	parsed := make(map[string]*rawShape)
	for name, rawData := range raw.Shapes {
		var s rawShape
		if err := json.Unmarshal(rawData, &s); err != nil {
			return nil, fmt.Errorf("unmarshal shape %s: %w", name, err)
		}
		parsed[name] = &s
	}

	var serviceFQN string
	var serviceShape *rawShape
	for name, s := range parsed {
		if s.Type == "service" {
			serviceFQN = name
			serviceShape = s
			break
		}
	}
	if serviceShape == nil {
		return nil, fmt.Errorf("no service shape found")
	}

	model := &SmithyModel{
		ServiceName: shortName(serviceFQN),
		ServiceID:   detectServiceID(serviceFQN),
		Protocol:    detectProtocol(serviceShape),
		Shapes:      make(map[string]*Shape),
	}

	// Parse operations
	for _, opFQN := range collectOperationTargets(parsed, serviceShape) {
		opShape, ok := parsed[opFQN]
		if !ok {
			continue
		}
		op := Operation{Name: shortName(opFQN)}
		if opShape.Input != nil {
			if opShape.Input.Target == "smithy.api#Unit" {
				op.InputName = "SmithyUnit"
			} else {
				op.InputName = shortName(opShape.Input.Target)
			}
		}
		if opShape.Output != nil {
			if opShape.Output.Target == "smithy.api#Unit" {
				op.OutputName = "SmithyUnit"
			} else {
				op.OutputName = shortName(opShape.Output.Target)
			}
		}
		for _, errRef := range opShape.Errors {
			op.Errors = append(op.Errors, shortName(errRef.Target))
		}
		if httpTrait, ok := opShape.Traits["smithy.api#http"]; ok {
			var ht rawHTTPTrait
			_ = json.Unmarshal(httpTrait, &ht) //nolint:errcheck
			op.HTTPMethod = ht.Method
			op.HTTPUri = ht.URI
		}
		model.Operations = append(model.Operations, op)
	}
	sort.Slice(model.Operations, func(i, j int) bool {
		return model.Operations[i].Name < model.Operations[j].Name
	})

	// Parse shapes
	for fqn, rs := range parsed {
		name := shortName(fqn)
		if strings.HasPrefix(fqn, "smithy.api#") {
			continue
		}
		if rs.Type == "service" || rs.Type == "operation" || rs.Type == "resource" {
			continue
		}

		shape := &Shape{Name: name, Type: ShapeType(rs.Type)}

		if rs.Type == "structure" {
			for memberName, member := range rs.Members {
				m := Member{
					Name:       memberName,
					TargetName: shortName(member.Target),
					GoType:     smithyToGoType(member.Target),
				}
				if member.Traits != nil {
					if _, ok := member.Traits["smithy.api#required"]; ok {
						m.Required = true
					}
					if _, ok := member.Traits["smithy.api#httpLabel"]; ok {
						m.HTTPLabel = true
					}
					if _, ok := member.Traits["smithy.api#httpPayload"]; ok {
						m.HTTPPayload = true
					}
					if hdr, ok := member.Traits["smithy.api#httpHeader"]; ok {
						var headerName string
						_ = json.Unmarshal(hdr, &headerName) //nolint:errcheck
						m.HTTPHeader = headerName
					}
					if q, ok := member.Traits["smithy.api#httpQuery"]; ok {
						var queryName string
						_ = json.Unmarshal(q, &queryName) //nolint:errcheck
						m.HTTPQuery = queryName
					}
				}
				shape.Members = append(shape.Members, m)
			}
			sort.Slice(shape.Members, func(i, j int) bool {
				return shape.Members[i].Name < shape.Members[j].Name
			})

			if errType, ok := rs.Traits["smithy.api#error"]; ok {
				var et string
				_ = json.Unmarshal(errType, &et) //nolint:errcheck
				shape.ErrorTrait = &ErrorTrait{Type: et}
				if httpErr, ok := rs.Traits["smithy.api#httpError"]; ok {
					var status int
					_ = json.Unmarshal(httpErr, &status) //nolint:errcheck
					shape.ErrorTrait.HTTPStatus = status
				}
			}
		}

		if rs.Type == "list" && rs.Member != nil {
			shape.ListMember = &MemberRef{
				TargetName: shortName(rs.Member.Target),
				GoType:     smithyToGoType(rs.Member.Target),
			}
		}

		if rs.Type == "map" {
			if rs.Key != nil {
				shape.KeyMember = &MemberRef{TargetName: shortName(rs.Key.Target), GoType: smithyToGoType(rs.Key.Target)}
			}
			if rs.Value != nil {
				shape.ValueMember = &MemberRef{TargetName: shortName(rs.Value.Target), GoType: smithyToGoType(rs.Value.Target)}
			}
		}

		model.Shapes[name] = shape
	}

	// Add a synthetic SmithyUnit shape if any operation references smithy.api#Unit
	// as its input or output type. We use SmithyUnit to avoid collisions with
	// model-defined shapes also named Unit.
	for _, op := range model.Operations {
		if op.OutputName == "SmithyUnit" || op.InputName == "SmithyUnit" {
			if _, exists := model.Shapes["SmithyUnit"]; !exists {
				model.Shapes["SmithyUnit"] = &Shape{Name: "SmithyUnit", Type: ShapeStructure}
			}
			break
		}
	}

	// Resolve simple shape references to base Go types. This turns member
	// GoTypes like "ReturnConsumedCapacity" (a Smithy string shape) into
	// "string" so that generated code compiles without type alias definitions.
	resolveSimpleType(model.Shapes)

	return model, nil
}

// resolveSimpleType walks all structure members and list/map refs, replacing
// GoType values that reference simple shapes (string, enum, integer, etc.)
// with their underlying Go primitive type.
func resolveSimpleType(shapes map[string]*Shape) {
	resolve := func(goType string) string {
		s, ok := shapes[goType]
		if !ok {
			return goType
		}
		switch s.Type {
		case ShapeString, ShapeEnum:
			return "string"
		case ShapeInteger, ShapeIntEnum:
			return "int32"
		case ShapeLong:
			return "int64"
		case ShapeBoolean:
			return "bool"
		case ShapeBlob:
			return "[]byte"
		case ShapeTimestamp:
			return "time.Time"
		case ShapeDouble:
			return "float64"
		case ShapeFloat:
			return "float32"
		case ShapeUnion, ShapeDocument:
			return "interface{}"
		default:
			return goType
		}
	}

	for _, shape := range shapes {
		for i := range shape.Members {
			shape.Members[i].GoType = resolve(shape.Members[i].GoType)
		}
		if shape.ListMember != nil {
			shape.ListMember.GoType = resolve(shape.ListMember.GoType)
		}
		if shape.KeyMember != nil {
			shape.KeyMember.GoType = resolve(shape.KeyMember.GoType)
		}
		if shape.ValueMember != nil {
			shape.ValueMember.GoType = resolve(shape.ValueMember.GoType)
		}
	}
}

// collectOperationTargets returns the FQN of every operation the service binds,
// walking both the service's own operations list and every resource reachable
// from it. Smithy lets a resource carry operations through `operations`,
// `collectionOperations` and the lifecycle slots, so a model built around
// resources (lambda, ecs, bedrock) exposes only a fraction of its API when the
// service list is read alone. Resources are visited once, so a cyclic resource
// graph terminates.
func collectOperationTargets(shapes map[string]*rawShape, service *rawShape) []string {
	var targets []string
	seen := make(map[string]bool)
	add := func(refs ...*rawTarget) {
		for _, ref := range refs {
			if ref == nil || ref.Target == "" || seen[ref.Target] {
				continue
			}
			seen[ref.Target] = true
			targets = append(targets, ref.Target)
		}
	}
	addAll := func(refs []rawTarget) {
		for i := range refs {
			add(&refs[i])
		}
	}

	addAll(service.Operations)

	visited := make(map[string]bool)
	queue := append([]rawTarget(nil), service.Resources...)
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]
		if visited[ref.Target] {
			continue
		}
		visited[ref.Target] = true

		res, ok := shapes[ref.Target]
		if !ok || res.Type != "resource" {
			continue
		}
		addAll(res.Operations)
		addAll(res.CollectionOperations)
		add(res.Create, res.Put, res.Read, res.Update, res.Delete, res.List)
		queue = append(queue, res.Resources...)
	}
	return targets
}

func shortName(fqn string) string {
	parts := strings.Split(fqn, "#")
	if len(parts) == 2 {
		return parts[1]
	}
	return fqn
}

func detectServiceID(fqn string) string {
	parts := strings.Split(fqn, "#")
	if len(parts) == 2 {
		nsParts := strings.Split(parts[0], ".")
		return nsParts[len(nsParts)-1]
	}
	return strings.ToLower(fqn)
}

func detectProtocol(s *rawShape) string {
	if s.Traits == nil {
		return ""
	}
	if _, ok := s.Traits["aws.protocols#restXml"]; ok {
		return "rest-xml"
	}
	if _, ok := s.Traits["aws.protocols#awsJson1_0"]; ok {
		return "json-1.0"
	}
	if _, ok := s.Traits["aws.protocols#awsJson1_1"]; ok {
		return "json-1.1"
	}
	if _, ok := s.Traits["aws.protocols#awsQuery"]; ok {
		return "query"
	}
	if _, ok := s.Traits["aws.protocols#restJson1"]; ok {
		return "rest-json"
	}
	return ""
}

func smithyToGoType(target string) string {
	switch target {
	case "smithy.api#String":
		return "string"
	case "smithy.api#Integer":
		return "int32"
	case "smithy.api#Long":
		return "int64"
	case "smithy.api#Boolean":
		return "bool"
	case "smithy.api#Blob":
		return "[]byte"
	case "smithy.api#Timestamp":
		return "time.Time"
	case "smithy.api#Double":
		return "float64"
	case "smithy.api#Float":
		return "float32"
	case "smithy.api#Unit":
		return "struct{}"
	case "smithy.api#Document":
		return "interface{}"
	default:
		return shortName(target)
	}
}
