// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroupstaggingapi/provider.go
package resourcegroupstaggingapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "resourcegroupstaggingapi" }
func (p *Provider) ServiceName() string           { return "ResourceGroupsTaggingAPI_20170126" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "resourcegroupstaggingapi"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	case "GetResources":
		return p.getResources(params)
	case "GetTagKeys":
		return p.getTagKeys(params)
	case "GetTagValues":
		return p.getTagValues(params)
	case "TagResources":
		return p.tagResources(params)
	case "UntagResources":
		return p.untagResources(params)
	case "GetComplianceSummary":
		return p.getComplianceSummary(params)
	case "StartReportCreation":
		return p.startReportCreation(params)
	case "DescribeReportCreation":
		return p.describeReportCreation(params)
	case "ListRequiredTags":
		return p.listRequiredTags(params)
	case "ListTagOptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TagOptions":      []any{},
			"PaginationToken": "",
		})
	case "GetTagOption":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TagOption": map[string]any{
				"Id":     params["Id"],
				"Active": true,
			},
		})
	case "CreateTagOption":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TagOption": map[string]any{
				"Id":     shared.GenerateID("tag-opt-", 16),
				"Key":    params["Key"],
				"Value":  params["Value"],
				"Active": true,
			},
		})
	case "DeleteTagOption":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateTagOption":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TagOption": map[string]any{
				"Id":     params["Id"],
				"Active": true,
			},
		})
	case "AssociateTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DisassociateTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListTagAssociations":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TagAssociations": []any{},
			"PaginationToken": "",
		})
	case "GetComplianceDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ComplianceDetails": map[string]any{},
		})
	case "ListTaggedResourcesByType":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceTagMappingList": []any{},
		})
	case "GetResourceTags":
		return p.getResources(params)
	case "SearchResourcesByTags":
		return p.getResources(params)
	case "ListResourcesWithTagFilter":
		return p.getResources(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) getResources(params map[string]any) (*plugin.Response, error) {
	// Parse TagFilters
	var tagFilters []TagFilter
	if tf, ok := params["TagFilters"].([]any); ok {
		for _, item := range tf {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			f := TagFilter{}
			if k, ok := m["Key"].(string); ok {
				f.Key = k
			}
			if vals, ok := m["Values"].([]any); ok {
				for _, v := range vals {
					if s, ok := v.(string); ok {
						f.Values = append(f.Values, s)
					}
				}
			}
			tagFilters = append(tagFilters, f)
		}
	}

	// Parse ResourceTypeFilters
	var resourceTypeFilters []string
	if rtf, ok := params["ResourceTypeFilters"].([]any); ok {
		for _, item := range rtf {
			if s, ok := item.(string); ok {
				resourceTypeFilters = append(resourceTypeFilters, s)
			}
		}
	}

	mappings, err := p.store.GetResources(tagFilters, resourceTypeFilters, shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}

	list := make([]map[string]any, 0, len(mappings))
	for _, m := range mappings {
		tagList := make([]map[string]any, 0, len(m.Tags))
		for _, t := range m.Tags {
			tagList = append(tagList, map[string]any{
				"Key":   t.TagKey,
				"Value": t.TagValue,
			})
		}
		list = append(list, map[string]any{
			"ResourceARN": m.ResourceARN,
			"Tags":        tagList,
		})
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceTagMappingList": list,
		"PaginationToken":        "",
	})
}

func (p *Provider) getTagKeys(_ map[string]any) (*plugin.Response, error) {
	keys, err := p.store.GetTagKeys(shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}
	if keys == nil {
		keys = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TagKeys":         keys,
		"PaginationToken": "",
	})
}

func (p *Provider) getTagValues(params map[string]any) (*plugin.Response, error) {
	key, _ := params["Key"].(string)
	vals, err := p.store.GetTagValues(key, shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}
	if vals == nil {
		vals = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TagValues":       vals,
		"PaginationToken": "",
	})
}

func (p *Provider) tagResources(params map[string]any) (*plugin.Response, error) {
	arns, ok := params["ResourceARNList"].([]any)
	if !ok {
		return shared.JSONError("ValidationException", "ResourceARNList is required", http.StatusBadRequest), nil
	}
	tagsRaw, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string, len(tagsRaw))
	for k, v := range tagsRaw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}

	failed := map[string]any{}
	for _, arnAny := range arns {
		arn, ok := arnAny.(string)
		if !ok {
			continue
		}
		if err := p.store.PutTags(arn, tags, shared.DefaultAccountID); err != nil {
			failed[arn] = map[string]any{
				"StatusCode":   http.StatusInternalServerError,
				"ErrorCode":    "InternalServiceException",
				"ErrorMessage": err.Error(),
			}
		}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FailedResourcesMap": failed,
	})
}

func (p *Provider) untagResources(params map[string]any) (*plugin.Response, error) {
	arns, ok := params["ResourceARNList"].([]any)
	if !ok {
		return shared.JSONError("ValidationException", "ResourceARNList is required", http.StatusBadRequest), nil
	}
	tagKeysRaw, _ := params["TagKeys"].([]any)
	tagKeys := make([]string, 0, len(tagKeysRaw))
	for _, k := range tagKeysRaw {
		if s, ok := k.(string); ok {
			tagKeys = append(tagKeys, s)
		}
	}

	failed := map[string]any{}
	for _, arnAny := range arns {
		arn, ok := arnAny.(string)
		if !ok {
			continue
		}
		if err := p.store.DeleteTags(arn, tagKeys); err != nil {
			failed[arn] = map[string]any{
				"StatusCode":   http.StatusInternalServerError,
				"ErrorCode":    "InternalServiceException",
				"ErrorMessage": err.Error(),
			}
		}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FailedResourcesMap": failed,
	})
}

func (p *Provider) getComplianceSummary(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SummaryList":     []any{},
		"PaginationToken": "",
	})
}

func (p *Provider) startReportCreation(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeReportCreation(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Status":       "COMPLETE",
		"S3Location":   "",
		"ErrorMessage": "",
	})
}

func (p *Provider) listRequiredTags(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ComplianceDetails": []any{},
		"PaginationToken":   "",
	})
}
