// SPDX-License-Identifier: Apache-2.0

package cloudformation

// engine.go implements real resource provisioning for CloudFormation stacks.
//
// Given a parsed template and resolved parameters, the engine:
//   1. Builds a dependency graph from Fn::Ref / Fn::GetAtt references.
//   2. Topologically sorts the logical resources.
//   3. For each resource, evaluates intrinsic functions and dispatches the
//      request to the matching DevCloud service plugin (S3, DynamoDB, SQS,
//      SNS, Lambda, IAM) using plugin.DefaultRegistry.Get(name).
//   4. Persists the resulting physical IDs / ARNs / attributes in the
//      stack_resources table so Fn::Ref and Fn::GetAtt evaluations work for
//      later resources in the same stack.
//   5. On failure, rolls back previously-provisioned resources in reverse
//      order by dispatching Delete* operations.

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Template parsing
// ---------------------------------------------------------------------------

// Template is a decoded CloudFormation template.  Fields preserve raw interface{}
// values so intrinsic-function markers (maps with a single Fn:: key) survive
// decoding and can be evaluated lazily.
type Template struct {
	FormatVersion string                    `json:"AWSTemplateFormatVersion,omitempty"`
	Description   string                    `json:"Description,omitempty"`
	Parameters    map[string]map[string]any `json:"Parameters,omitempty"`
	Resources     map[string]ResourceSpec   `json:"Resources"`
	Outputs       map[string]map[string]any `json:"Outputs,omitempty"`
	Conditions    map[string]any            `json:"Conditions,omitempty"`
}

// ResourceSpec is a single entry from the template's Resources section.
type ResourceSpec struct {
	Type       string                 `json:"Type"`
	Properties map[string]any         `json:"Properties,omitempty"`
	DependsOn  any                    `json:"DependsOn,omitempty"`
	Metadata   map[string]any         `json:"Metadata,omitempty"`
	Extra      map[string]interface{} `json:"-"`
}

// ParseTemplate accepts JSON or YAML and returns a normalized Template.
// YAML is converted to its canonical map[string]any form, then round-tripped
// through JSON so the strongly-typed Template struct can be populated while
// still preserving the generic intrinsic-function maps inside Properties.
func ParseTemplate(body string) (*Template, error) {
	s := strings.TrimSpace(body)
	if s == "" {
		return &Template{Resources: map[string]ResourceSpec{}}, nil
	}

	// Try JSON first — it is a strict subset of YAML but json.Unmarshal is
	// stricter about trailing garbage and gives better errors.
	if strings.HasPrefix(s, "{") {
		return parseJSONTemplate([]byte(s))
	}
	return parseYAMLTemplate([]byte(s))
}

func parseJSONTemplate(data []byte) (*Template, error) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("template: invalid JSON: %w", err)
	}
	return buildTemplate(raw)
}

func parseYAMLTemplate(data []byte) (*Template, error) {
	var root any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("template: invalid YAML: %w", err)
	}
	// yaml.v3 produces map[string]any only for top-level documents that
	// already use string keys — coerce to keep the downstream logic simple.
	m := convertYAMLMap(root)
	raw, ok := m.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("template: YAML root must be a mapping")
	}
	return buildTemplate(raw)
}

// convertYAMLMap walks a yaml.v3 decoded tree and normalises
// map[interface{}]interface{} into map[string]any so JSON/CFN handling can
// share one code path.
func convertYAMLMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, vv := range t {
			t[k] = convertYAMLMap(vv)
		}
		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[fmt.Sprint(k)] = convertYAMLMap(vv)
		}
		return out
	case []any:
		for i, vv := range t {
			t[i] = convertYAMLMap(vv)
		}
		return t
	default:
		return v
	}
}

func buildTemplate(raw map[string]any) (*Template, error) {
	t := &Template{
		Resources: map[string]ResourceSpec{},
	}
	if v, ok := raw["AWSTemplateFormatVersion"].(string); ok {
		t.FormatVersion = v
	}
	if v, ok := raw["Description"].(string); ok {
		t.Description = v
	}
	if v, ok := raw["Parameters"].(map[string]any); ok {
		t.Parameters = make(map[string]map[string]any, len(v))
		for k, vv := range v {
			if m, ok := vv.(map[string]any); ok {
				t.Parameters[k] = m
			}
		}
	}
	if v, ok := raw["Outputs"].(map[string]any); ok {
		t.Outputs = make(map[string]map[string]any, len(v))
		for k, vv := range v {
			if m, ok := vv.(map[string]any); ok {
				t.Outputs[k] = m
			}
		}
	}
	if v, ok := raw["Conditions"].(map[string]any); ok {
		t.Conditions = v
	}
	if v, ok := raw["Resources"].(map[string]any); ok {
		for logical, vv := range v {
			m, ok := vv.(map[string]any)
			if !ok {
				continue
			}
			var spec ResourceSpec
			if tv, ok := m["Type"].(string); ok {
				spec.Type = tv
			}
			if pv, ok := m["Properties"].(map[string]any); ok {
				spec.Properties = pv
			} else {
				spec.Properties = map[string]any{}
			}
			if dv, ok := m["DependsOn"]; ok {
				spec.DependsOn = dv
			}
			if mv, ok := m["Metadata"].(map[string]any); ok {
				spec.Metadata = mv
			}
			t.Resources[logical] = spec
		}
	}
	return t, nil
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// Engine provisions stacks by dispatching to other service plugins.
type Engine struct {
	store    *Store
	registry *plugin.Registry
	port     int // HTTP port used for fallback URL construction (e.g., SQS queue URLs).
}

// NewEngine builds a provisioning engine using the supplied store, plugin
// registry (defaults to plugin.DefaultRegistry when nil), and the configured
// HTTP server port used when building fallback response URLs. Pass 0 to
// accept the default (4747).
func NewEngine(store *Store, reg *plugin.Registry, port int) *Engine {
	if reg == nil {
		reg = plugin.DefaultRegistry
	}
	if port <= 0 {
		port = 4747
	}
	return &Engine{store: store, registry: reg, port: port}
}

// provisionResult captures the outcome of a single resource provision step so
// the rollback path can undo it.
type provisionResult struct {
	LogicalID  string
	Type       string
	PhysicalID string
	ARN        string
	Attributes map[string]string
}

// ProvisionStack walks the template's Resources in dependency order and
// attempts to create each resource via the matching DevCloud service plugin.
// On failure the stack enters ROLLBACK_IN_PROGRESS and previously-provisioned
// resources are deleted in reverse order.
func (e *Engine) ProvisionStack(ctx context.Context, stackName string, tmpl *Template, params map[string]string) error {
	order, err := topoSort(tmpl)
	if err != nil {
		return err
	}

	// Seed the attribute cache from any existing stack_resources rows so
	// UpdateStack re-provisioning can reference previously-created resources.
	provisioned := make([]provisionResult, 0, len(order))
	cache := newAttrCache(stackName)
	if existing, err := e.store.ListStackResources(stackName); err == nil {
		for _, r := range existing {
			attrs := map[string]string{}
			if r.Attributes != "" {
				_ = json.Unmarshal([]byte(r.Attributes), &attrs)
			}
			cache.set(r.LogicalID, r.PhysicalID, r.ARN, attrs)
		}
	}

	for _, logicalID := range order {
		spec := tmpl.Resources[logicalID]

		// Mark the row CREATE_IN_PROGRESS before dispatching.
		_ = e.store.UpsertStackResourceFull(stackName, logicalID, "", spec.Type, "CREATE_IN_PROGRESS", "", "{}")

		resolvedProps, err := resolveValue(spec.Properties, cache, params)
		if err != nil {
			e.rollback(ctx, stackName, provisioned, fmt.Errorf("resolve %s: %w", logicalID, err))
			return err
		}
		propsMap, _ := resolvedProps.(map[string]any)

		res, err := e.provisionResource(ctx, stackName, logicalID, spec.Type, propsMap)
		if err != nil {
			_ = e.store.UpsertStackResourceFull(stackName, logicalID, "", spec.Type, "CREATE_FAILED", "", "{}")
			e.rollback(ctx, stackName, provisioned, fmt.Errorf("%s: %w", logicalID, err))
			return err
		}

		// Persist the successful provision.
		attrJSON, _ := json.Marshal(res.Attributes)
		_ = e.store.UpsertStackResourceFull(
			stackName, logicalID, res.PhysicalID, spec.Type,
			"CREATE_COMPLETE", res.ARN, string(attrJSON),
		)
		cache.set(logicalID, res.PhysicalID, res.ARN, res.Attributes)
		provisioned = append(provisioned, *res)
	}
	return nil
}

// rollback marks the stack ROLLBACK_IN_PROGRESS and attempts to delete
// already-provisioned resources in reverse order.
func (e *Engine) rollback(ctx context.Context, stackName string, provisioned []provisionResult, cause error) {
	_ = e.store.SetStackStatus(stackName, "ROLLBACK_IN_PROGRESS")
	for i := len(provisioned) - 1; i >= 0; i-- {
		r := provisioned[i]
		_ = e.deleteResource(ctx, r.Type, r.PhysicalID, r.ARN, r.Attributes)
		_ = e.store.DeleteStackResource(stackName, r.LogicalID)
	}
	_ = e.store.SetStackStatus(stackName, "ROLLBACK_COMPLETE")
}

// TearDownStack deletes all live resources attached to a stack and updates
// their statuses as the teardown progresses.  It is invoked from
// handleDeleteStack before the stack row itself is removed.
func (e *Engine) TearDownStack(ctx context.Context, stackName string) {
	resources, err := e.store.ListStackResources(stackName)
	if err != nil {
		return
	}
	// Rough reverse-creation order: the ListStackResources result is sorted by
	// logical ID so we do not have the original dependency order, but
	// destroying in reverse sorted order is typically safe for simple stacks.
	for i := len(resources) - 1; i >= 0; i-- {
		r := resources[i]
		attrs := map[string]string{}
		if r.Attributes != "" {
			_ = json.Unmarshal([]byte(r.Attributes), &attrs)
		}
		_ = e.deleteResource(ctx, r.Type, r.PhysicalID, r.ARN, attrs)
	}
}

// ---------------------------------------------------------------------------
// Dependency analysis
// ---------------------------------------------------------------------------

// topoSort returns the resource logical IDs in dependency order
// (dependents after their dependencies).
func topoSort(tmpl *Template) ([]string, error) {
	// Build adjacency.
	deps := make(map[string]map[string]struct{}, len(tmpl.Resources))
	for id, spec := range tmpl.Resources {
		deps[id] = map[string]struct{}{}
		collectRefs(spec.Properties, deps[id])
		// DependsOn (string or []string)
		switch v := spec.DependsOn.(type) {
		case string:
			if v != "" {
				deps[id][v] = struct{}{}
			}
		case []any:
			for _, x := range v {
				if s, ok := x.(string); ok && s != "" {
					deps[id][s] = struct{}{}
				}
			}
		}
		// Drop self- and non-existent references.
		for dep := range deps[id] {
			if dep == id {
				delete(deps[id], dep)
				continue
			}
			if _, ok := tmpl.Resources[dep]; !ok {
				delete(deps[id], dep)
			}
		}
	}

	// Kahn's algorithm with deterministic ordering.
	remaining := make(map[string]int, len(deps))
	for id, d := range deps {
		remaining[id] = len(d)
	}

	var order []string
	for len(remaining) > 0 {
		ready := make([]string, 0)
		for id, count := range remaining {
			if count == 0 {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("circular dependency in template resources")
		}
		sort.Strings(ready)
		for _, id := range ready {
			delete(remaining, id)
			order = append(order, id)
			for other, d := range deps {
				if _, ok := d[id]; ok {
					remaining[other]--
				}
			}
		}
	}
	return order, nil
}

// collectRefs walks a value tree and records any Ref / Fn::GetAtt targets.
func collectRefs(v any, out map[string]struct{}) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			if target, ok := t["Ref"].(string); ok {
				out[target] = struct{}{}
				return
			}
			if arr, ok := t["Fn::GetAtt"]; ok {
				switch a := arr.(type) {
				case []any:
					if len(a) > 0 {
						if s, ok := a[0].(string); ok {
							out[s] = struct{}{}
						}
					}
				case string:
					if idx := strings.Index(a, "."); idx > 0 {
						out[a[:idx]] = struct{}{}
					}
				}
				return
			}
		}
		for _, vv := range t {
			collectRefs(vv, out)
		}
	case []any:
		for _, vv := range t {
			collectRefs(vv, out)
		}
	}
}

// ---------------------------------------------------------------------------
// Intrinsic function evaluation
// ---------------------------------------------------------------------------

// attrCache records physical IDs, ARNs, and attributes that previously-
// provisioned resources in this stack have already published.
type attrCache struct {
	stackName string
	physical  map[string]string
	arns      map[string]string
	attrs     map[string]map[string]string
}

func newAttrCache(stackName string) *attrCache {
	return &attrCache{
		stackName: stackName,
		physical:  map[string]string{},
		arns:      map[string]string{},
		attrs:     map[string]map[string]string{},
	}
}

func (c *attrCache) set(logical, physical, arn string, attrs map[string]string) {
	c.physical[logical] = physical
	c.arns[logical] = arn
	copyAttrs := make(map[string]string, len(attrs)+2)
	for k, v := range attrs {
		copyAttrs[k] = v
	}
	if arn != "" {
		copyAttrs["Arn"] = arn
	}
	c.attrs[logical] = copyAttrs
}

// ref returns the value Fn::Ref should produce — CloudFormation usually
// returns the resource's logical "Ref" value: Name for S3 buckets, ARN for
// SNS topics/Lambda/IAM, QueueUrl for SQS, etc.  We approximate that by
// returning the physical ID (falling back to ARN).
func (c *attrCache) ref(logical string) (string, bool) {
	if v, ok := c.physical[logical]; ok && v != "" {
		return v, true
	}
	if v, ok := c.arns[logical]; ok && v != "" {
		return v, true
	}
	return "", false
}

func (c *attrCache) getAtt(logical, attr string) (string, bool) {
	m, ok := c.attrs[logical]
	if !ok {
		return "", false
	}
	v, ok := m[attr]
	return v, ok
}

// resolveValue recursively walks a template value and replaces any intrinsic
// function markers with their concrete string/primitive results.  Returns the
// transformed structure.
func resolveValue(v any, cache *attrCache, params map[string]string) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			for k, inner := range t {
				switch k {
				case "Ref":
					name, _ := inner.(string)
					if p, ok := params[name]; ok {
						return p, nil
					}
					if pseudo, ok := pseudoParameter(name, cache.stackName); ok {
						return pseudo, nil
					}
					if ref, ok := cache.ref(name); ok {
						return ref, nil
					}
					return "", fmt.Errorf("unresolved Ref: %s", name)

				case "Fn::GetAtt":
					logical, attr, err := parseGetAtt(inner)
					if err != nil {
						return nil, err
					}
					if val, ok := cache.getAtt(logical, attr); ok {
						return val, nil
					}
					// Fall back to the logical resource's ARN/physical ID
					// when the attribute is not specifically known.
					if attr == "Arn" {
						if v, ok := cache.arns[logical]; ok {
							return v, nil
						}
					}
					return "", fmt.Errorf("unresolved GetAtt %s.%s", logical, attr)

				case "Fn::Sub":
					return resolveSub(inner, cache, params)

				case "Fn::Join":
					return resolveJoin(inner, cache, params)

				case "Fn::Select":
					return resolveSelect(inner, cache, params)

				case "Fn::Split":
					return resolveSplit(inner, cache, params)
				}
			}
		}
		// Regular map — resolve each child.
		out := make(map[string]any, len(t))
		for k, vv := range t {
			r, err := resolveValue(vv, cache, params)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			r, err := resolveValue(vv, cache, params)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	default:
		return v, nil
	}
}

// pseudoParameter returns the value for CloudFormation pseudo-parameters such
// as AWS::AccountId.  Unknown pseudo-parameters fall through to the Ref cache.
func pseudoParameter(name, stackName string) (string, bool) {
	switch name {
	case "AWS::AccountId":
		return shared.DefaultAccountID, true
	case "AWS::Region":
		return shared.DefaultRegion, true
	case "AWS::StackName":
		return stackName, true
	case "AWS::NoValue":
		return "", true
	case "AWS::Partition":
		return "aws", true
	case "AWS::URLSuffix":
		return "amazonaws.com", true
	}
	return "", false
}

func parseGetAtt(v any) (string, string, error) {
	switch t := v.(type) {
	case string:
		parts := strings.SplitN(t, ".", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid Fn::GetAtt string: %q", t)
		}
		return parts[0], parts[1], nil
	case []any:
		if len(t) < 2 {
			return "", "", fmt.Errorf("Fn::GetAtt expects [LogicalId, Attribute]")
		}
		a, _ := t[0].(string)
		b, _ := t[1].(string)
		return a, b, nil
	}
	return "", "", fmt.Errorf("unsupported Fn::GetAtt value type")
}

// resolveSub handles both ["${Var}", {map}] and "${Var}" forms.
func resolveSub(v any, cache *attrCache, params map[string]string) (string, error) {
	var template string
	overrides := map[string]string{}

	switch t := v.(type) {
	case string:
		template = t
	case []any:
		if len(t) == 0 {
			return "", fmt.Errorf("Fn::Sub array form requires at least one element")
		}
		s, ok := t[0].(string)
		if !ok {
			return "", fmt.Errorf("Fn::Sub first element must be a string")
		}
		template = s
		if len(t) > 1 {
			if m, ok := t[1].(map[string]any); ok {
				for k, vv := range m {
					r, err := resolveValue(vv, cache, params)
					if err != nil {
						return "", err
					}
					overrides[k] = toString(r)
				}
			}
		}
	default:
		return "", fmt.Errorf("unsupported Fn::Sub value")
	}

	// Replace ${Var} markers.  We walk char-by-char so nested braces do not
	// confuse the parser.
	var buf bytes.Buffer
	for i := 0; i < len(template); {
		if i+1 < len(template) && template[i] == '$' && template[i+1] == '{' {
			end := strings.Index(template[i+2:], "}")
			if end == -1 {
				buf.WriteByte(template[i])
				i++
				continue
			}
			varRef := template[i+2 : i+2+end]
			// Either Name or Name.Attr
			if v, ok := overrides[varRef]; ok {
				buf.WriteString(v)
			} else if v, ok := params[varRef]; ok {
				buf.WriteString(v)
			} else if v, ok := pseudoParameter(varRef, cache.stackName); ok {
				buf.WriteString(v)
			} else if dot := strings.Index(varRef, "."); dot > 0 {
				logical := varRef[:dot]
				attr := varRef[dot+1:]
				if v, ok := cache.getAtt(logical, attr); ok {
					buf.WriteString(v)
				} else if attr == "Arn" {
					if a, ok := cache.arns[logical]; ok {
						buf.WriteString(a)
					}
				}
			} else if v, ok := cache.ref(varRef); ok {
				buf.WriteString(v)
			} else {
				// Leave the token in place so users can at least see which
				// variable failed to resolve.
				buf.WriteString("${" + varRef + "}")
			}
			i += 2 + end + 1
			continue
		}
		buf.WriteByte(template[i])
		i++
	}
	return buf.String(), nil
}

func resolveJoin(v any, cache *attrCache, params map[string]string) (string, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return "", fmt.Errorf("Fn::Join expects [separator, [values]]")
	}
	sep, _ := arr[0].(string)
	items, ok := arr[1].([]any)
	if !ok {
		return "", fmt.Errorf("Fn::Join values must be a list")
	}
	parts := make([]string, 0, len(items))
	for _, it := range items {
		r, err := resolveValue(it, cache, params)
		if err != nil {
			return "", err
		}
		parts = append(parts, toString(r))
	}
	return strings.Join(parts, sep), nil
}

func resolveSelect(v any, cache *attrCache, params map[string]string) (any, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return nil, fmt.Errorf("Fn::Select expects [index, [values]]")
	}
	idx := 0
	switch n := arr[0].(type) {
	case int:
		idx = n
	case float64:
		idx = int(n)
	case string:
		fmt.Sscanf(n, "%d", &idx)
	}
	items, ok := arr[1].([]any)
	if !ok {
		return nil, fmt.Errorf("Fn::Select values must be a list")
	}
	if idx < 0 || idx >= len(items) {
		return nil, fmt.Errorf("Fn::Select index out of range")
	}
	return resolveValue(items[idx], cache, params)
}

func resolveSplit(v any, cache *attrCache, params map[string]string) ([]any, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return nil, fmt.Errorf("Fn::Split expects [separator, value]")
	}
	sep, _ := arr[0].(string)
	val, err := resolveValue(arr[1], cache, params)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(toString(val), sep)
	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out, nil
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return fmt.Sprintf("%g", t)
	case int:
		return fmt.Sprintf("%d", t)
	case int64:
		return fmt.Sprintf("%d", t)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// ---------------------------------------------------------------------------
// Per-resource-type dispatchers
// ---------------------------------------------------------------------------

// provisionResource looks up the handler for the given CFN resource type and
// dispatches to the corresponding DevCloud service plugin.  Unsupported types
// (or types whose sibling service is not registered in the current process)
// fall through to a simulated provision that records a synthetic physical ID
// — this keeps CFN templates working in unit tests that only register the
// CloudFormation plugin, while still doing real provisioning in the full
// server.
func (e *Engine) provisionResource(ctx context.Context, stackName, logicalID, resType string, props map[string]any) (*provisionResult, error) {
	var (
		res *provisionResult
		err error
	)
	switch resType {
	case "AWS::S3::Bucket":
		res, err = e.provisionS3Bucket(ctx, logicalID, props)
	case "AWS::DynamoDB::Table":
		res, err = e.provisionDynamoDBTable(ctx, logicalID, props)
	case "AWS::Lambda::Function":
		res, err = e.provisionLambdaFunction(ctx, logicalID, props)
	case "AWS::SQS::Queue":
		res, err = e.provisionSQSQueue(ctx, stackName, logicalID, props)
	case "AWS::SNS::Topic":
		res, err = e.provisionSNSTopic(ctx, logicalID, props)
	case "AWS::IAM::Role":
		res, err = e.provisionIAMRole(ctx, logicalID, props)
	default:
		return e.simulateResource(logicalID, resType, props), nil
	}
	if err != nil {
		// When the sibling service isn't loaded (common in unit tests that
		// only initialise the CloudFormation plugin), fall back to a
		// simulated provision so the stack still reaches CREATE_COMPLETE.
		if errors.Is(err, errServiceNotRegistered) {
			return e.simulateResource(logicalID, resType, props), nil
		}
		return nil, err
	}
	return res, nil
}

// simulateResource returns a CREATE_COMPLETE placeholder for resources the
// engine cannot really create (missing sibling plugin or unknown type).
func (e *Engine) simulateResource(logicalID, resType string, props map[string]any) *provisionResult {
	physicalID := logicalID + "-" + shared.GenerateID("", 8)
	if props != nil {
		if name, ok := props["BucketName"].(string); ok && name != "" {
			physicalID = name
		} else if name, ok := props["QueueName"].(string); ok && name != "" {
			physicalID = name
		} else if name, ok := props["TopicName"].(string); ok && name != "" {
			physicalID = name
		} else if name, ok := props["TableName"].(string); ok && name != "" {
			physicalID = name
		} else if name, ok := props["FunctionName"].(string); ok && name != "" {
			physicalID = name
		} else if name, ok := props["RoleName"].(string); ok && name != "" {
			physicalID = name
		}
	}
	arn := shared.BuildARN("cloudformation", "resource", physicalID)
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       resType,
		PhysicalID: physicalID,
		ARN:        arn,
		Attributes: map[string]string{"Arn": arn},
	}
}

// deleteResource fires the matching Delete* API for each supported resource
// type.  Errors are swallowed — rollback/delete is best-effort.
func (e *Engine) deleteResource(ctx context.Context, resType, physicalID, arn string, attrs map[string]string) error {
	switch resType {
	case "AWS::S3::Bucket":
		return e.deleteS3Bucket(ctx, physicalID)
	case "AWS::DynamoDB::Table":
		return e.deleteDynamoDBTable(ctx, physicalID)
	case "AWS::Lambda::Function":
		return e.deleteLambdaFunction(ctx, physicalID)
	case "AWS::SQS::Queue":
		queueURL := attrs["QueueUrl"]
		if queueURL == "" {
			queueURL = physicalID
		}
		return e.deleteSQSQueue(ctx, queueURL)
	case "AWS::SNS::Topic":
		return e.deleteSNSTopic(ctx, arn)
	case "AWS::IAM::Role":
		return e.deleteIAMRole(ctx, physicalID)
	}
	return nil
}

// errServiceNotRegistered is returned when the engine cannot find the sibling
// service for a given resource type.  Callers treat this as a non-fatal
// "simulate-only" provision so DevCloud tests that exercise the CFN service
// in isolation still work.
var errServiceNotRegistered = fmt.Errorf("sibling service not registered")

// call invokes a service plugin's HandleRequest using a synthetic
// httptest.NewRequest so the plugin sees a fully-formed HTTP request.  The
// `op` argument is the operation name (for protocols that use the Action
// header or X-Amz-Target); when the plugin re-reads it from the body that
// still works because we write the action there as well for Query-protocol
// services.
func (e *Engine) call(ctx context.Context, service, op, method, path, contentType string, target string, body []byte) (*plugin.Response, error) {
	svc, ok := e.registry.Get(service)
	if !ok {
		return nil, errServiceNotRegistered
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}
	return svc.HandleRequest(ctx, op, req)
}

func isSuccess(resp *plugin.Response) bool {
	return resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300
}

// ---- S3 ----

func (e *Engine) provisionS3Bucket(ctx context.Context, logicalID string, props map[string]any) (*provisionResult, error) {
	name, _ := props["BucketName"].(string)
	if name == "" {
		name = strings.ToLower(logicalID) + "-" + shared.GenerateID("", 8)
	}
	resp, err := e.call(ctx, "s3", "", http.MethodPut, "/"+name, "application/xml", "", nil)
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("s3 CreateBucket failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	arn := fmt.Sprintf("arn:aws:s3:::%s", name)
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::S3::Bucket",
		PhysicalID: name,
		ARN:        arn,
		Attributes: map[string]string{
			"Arn":                arn,
			"DomainName":         name + ".s3.amazonaws.com",
			"WebsiteURL":         "http://" + name + ".s3-website-us-east-1.amazonaws.com",
			"RegionalDomainName": name + ".s3.us-east-1.amazonaws.com",
		},
	}, nil
}

func (e *Engine) deleteS3Bucket(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}
	_, err := e.call(ctx, "s3", "", http.MethodDelete, "/"+name, "application/xml", "", nil)
	return err
}

// ---- DynamoDB ----

func (e *Engine) provisionDynamoDBTable(ctx context.Context, logicalID string, props map[string]any) (*provisionResult, error) {
	// CFN properties map almost 1:1 to the DynamoDB JSON body.
	body, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	resp, err := e.call(ctx, "dynamodb", "CreateTable", http.MethodPost, "/", "application/x-amz-json-1.0", "DynamoDB_20120810.CreateTable", body)
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("dynamodb CreateTable failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	tableName, _ := props["TableName"].(string)
	if tableName == "" {
		tableName = logicalID + "-" + shared.GenerateID("", 8)
	}
	arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", shared.DefaultRegion, shared.DefaultAccountID, tableName)
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::DynamoDB::Table",
		PhysicalID: tableName,
		ARN:        arn,
		Attributes: map[string]string{"Arn": arn, "StreamArn": ""},
	}, nil
}

func (e *Engine) deleteDynamoDBTable(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]string{"TableName": name})
	_, err := e.call(ctx, "dynamodb", "DeleteTable", http.MethodPost, "/", "application/x-amz-json-1.0", "DynamoDB_20120810.DeleteTable", body)
	return err
}

// ---- Lambda ----

func (e *Engine) provisionLambdaFunction(ctx context.Context, logicalID string, props map[string]any) (*provisionResult, error) {
	body, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	resp, err := e.call(ctx, "lambda", "", http.MethodPost, "/2015-03-31/functions", "application/json", "", body)
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("lambda CreateFunction failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	name, _ := props["FunctionName"].(string)
	if name == "" {
		name = logicalID
	}
	arn := fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", shared.DefaultRegion, shared.DefaultAccountID, name)
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::Lambda::Function",
		PhysicalID: name,
		ARN:        arn,
		Attributes: map[string]string{"Arn": arn},
	}, nil
}

func (e *Engine) deleteLambdaFunction(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}
	_, err := e.call(ctx, "lambda", "", http.MethodDelete, "/2015-03-31/functions/"+name, "application/json", "", nil)
	return err
}

// ---- SQS ----

func (e *Engine) provisionSQSQueue(ctx context.Context, stackName, logicalID string, props map[string]any) (*provisionResult, error) {
	name, _ := props["QueueName"].(string)
	if name == "" {
		// Default CFN queue name: <StackName>-<LogicalId>-<ShortId>
		name = stackName + "-" + logicalID + "-" + shared.GenerateID("", 8)
	}
	form := url.Values{}
	form.Set("Action", "CreateQueue")
	form.Set("QueueName", name)
	// Pass through queue attributes (VisibilityTimeout, DelaySeconds, ...)
	i := 1
	for k, v := range props {
		if k == "QueueName" || k == "Tags" {
			continue
		}
		form.Set(fmt.Sprintf("Attribute.%d.Name", i), k)
		form.Set(fmt.Sprintf("Attribute.%d.Value", i), toString(v))
		i++
	}
	body := []byte(form.Encode())
	resp, err := e.call(ctx, "sqs", "CreateQueue", http.MethodPost, "/", "application/x-www-form-urlencoded", "", body)
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("sqs CreateQueue failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	// Extract the URL from the XML response to keep PhysicalId consistent with SQS.
	queueURL := extractXMLField(resp.Body, "QueueUrl")
	if queueURL == "" {
		queueURL = fmt.Sprintf("http://localhost:%d/%s/%s", e.port, shared.DefaultAccountID, name)
	}
	arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", shared.DefaultRegion, shared.DefaultAccountID, name)
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::SQS::Queue",
		PhysicalID: queueURL,
		ARN:        arn,
		Attributes: map[string]string{
			"Arn":       arn,
			"QueueName": name,
			"QueueUrl":  queueURL,
		},
	}, nil
}

func (e *Engine) deleteSQSQueue(ctx context.Context, queueURL string) error {
	if queueURL == "" {
		return nil
	}
	form := url.Values{}
	form.Set("Action", "DeleteQueue")
	form.Set("QueueUrl", queueURL)
	_, err := e.call(ctx, "sqs", "DeleteQueue", http.MethodPost, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
	return err
}

// ---- SNS ----

func (e *Engine) provisionSNSTopic(ctx context.Context, logicalID string, props map[string]any) (*provisionResult, error) {
	name, _ := props["TopicName"].(string)
	if name == "" {
		name = logicalID
	}
	form := url.Values{}
	form.Set("Action", "CreateTopic")
	form.Set("Name", name)
	resp, err := e.call(ctx, "sns", "CreateTopic", http.MethodPost, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("sns CreateTopic failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	arn := extractXMLField(resp.Body, "TopicArn")
	if arn == "" {
		arn = fmt.Sprintf("arn:aws:sns:%s:%s:%s", shared.DefaultRegion, shared.DefaultAccountID, name)
	}
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::SNS::Topic",
		PhysicalID: arn, // SNS Ref returns the ARN
		ARN:        arn,
		Attributes: map[string]string{
			"Arn":       arn,
			"TopicName": name,
			"TopicArn":  arn,
		},
	}, nil
}

func (e *Engine) deleteSNSTopic(ctx context.Context, arn string) error {
	if arn == "" {
		return nil
	}
	form := url.Values{}
	form.Set("Action", "DeleteTopic")
	form.Set("TopicArn", arn)
	_, err := e.call(ctx, "sns", "DeleteTopic", http.MethodPost, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
	return err
}

// ---- IAM ----

func (e *Engine) provisionIAMRole(ctx context.Context, logicalID string, props map[string]any) (*provisionResult, error) {
	roleName, _ := props["RoleName"].(string)
	if roleName == "" {
		roleName = logicalID + "-" + shared.GenerateID("", 8)
	}
	form := url.Values{}
	form.Set("Action", "CreateRole")
	form.Set("RoleName", roleName)
	if doc, ok := props["AssumeRolePolicyDocument"]; ok {
		switch d := doc.(type) {
		case string:
			form.Set("AssumeRolePolicyDocument", d)
		default:
			b, _ := json.Marshal(d)
			form.Set("AssumeRolePolicyDocument", string(b))
		}
	}
	resp, err := e.call(ctx, "iam", "CreateRole", http.MethodPost, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
	if err != nil {
		return nil, err
	}
	if !isSuccess(resp) {
		return nil, fmt.Errorf("iam CreateRole failed: status=%d body=%s", resp.StatusCode, string(resp.Body))
	}
	arn := extractXMLField(resp.Body, "Arn")
	if arn == "" {
		arn = fmt.Sprintf("arn:aws:iam::%s:role/%s", shared.DefaultAccountID, roleName)
	}
	return &provisionResult{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Role",
		PhysicalID: roleName,
		ARN:        arn,
		Attributes: map[string]string{
			"Arn":      arn,
			"RoleId":   extractXMLField(resp.Body, "RoleId"),
			"RoleName": roleName,
		},
	}, nil
}

func (e *Engine) deleteIAMRole(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}
	form := url.Values{}
	form.Set("Action", "DeleteRole")
	form.Set("RoleName", name)
	_, err := e.call(ctx, "iam", "DeleteRole", http.MethodPost, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
	return err
}

// extractXMLField does a minimal XML tag lookup — enough to pluck a single
// scalar value out of a response body without dragging in full XML decoding.
func extractXMLField(body []byte, tag string) string {
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			return ""
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == tag {
			var v string
			if err := dec.DecodeElement(&v, &se); err == nil {
				return v
			}
			return ""
		}
	}
}
