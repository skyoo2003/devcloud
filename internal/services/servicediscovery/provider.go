// SPDX-License-Identifier: Apache-2.0

// internal/services/servicediscovery/provider.go
package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "servicediscovery" }
func (p *Provider) ServiceName() string           { return "Route53AutoNaming_v20170314" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "servicediscovery"))
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
	case "CreateHttpNamespace":
		return p.createHttpNamespace(params)
	case "CreatePrivateDnsNamespace":
		return p.createPrivateDnsNamespace(params)
	case "CreatePublicDnsNamespace":
		return p.createPublicDnsNamespace(params)
	case "GetNamespace":
		return p.getNamespace(params)
	case "ListNamespaces":
		return p.listNamespaces(params)
	case "DeleteNamespace":
		return p.deleteNamespace(params)
	case "UpdateHttpNamespace":
		return p.updateHttpNamespace(params)
	case "UpdatePrivateDnsNamespace":
		return p.updatePrivateDnsNamespace(params)
	case "UpdatePublicDnsNamespace":
		return p.updatePublicDnsNamespace(params)
	case "CreateService":
		return p.createService(params)
	case "GetService":
		return p.getService(params)
	case "ListServices":
		return p.listServices(params)
	case "UpdateService":
		return p.updateService(params)
	case "DeleteService":
		return p.deleteService(params)
	case "RegisterInstance":
		return p.registerInstance(params)
	case "DeregisterInstance":
		return p.deregisterInstance(params)
	case "GetInstance":
		return p.getInstance(params)
	case "ListInstances":
		return p.listInstances(params)
	case "DiscoverInstances":
		return p.discoverInstances(params)
	case "DiscoverInstancesRevision":
		return p.discoverInstancesRevision(params)
	case "GetInstancesHealthStatus":
		return p.getInstancesHealthStatus(params)
	case "UpdateInstanceCustomHealthStatus":
		return p.updateInstanceCustomHealthStatus(params)
	case "GetOperation":
		return p.getOperation(params)
	case "ListOperations":
		return p.listOperations(params)
	case "GetServiceAttributes":
		return p.getServiceAttributes(params)
	case "UpdateServiceAttributes":
		return p.updateServiceAttributes(params)
	case "DeleteServiceAttributes":
		return p.deleteServiceAttributes(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	namespaces, err := p.store.ListNamespaces()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(namespaces))
	for _, ns := range namespaces {
		res = append(res, plugin.Resource{Type: "namespace", ID: ns.ID, Name: ns.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// Namespace operations

func (p *Provider) createHttpNamespace(params map[string]any) (*plugin.Response, error) {
	return p.createNamespace(params, "HTTP")
}

func (p *Provider) createPrivateDnsNamespace(params map[string]any) (*plugin.Response, error) {
	return p.createNamespace(params, "DNS_PRIVATE")
}

func (p *Provider) createPublicDnsNamespace(params map[string]any) (*plugin.Response, error) {
	return p.createNamespace(params, "DNS")
}

func (p *Provider) createNamespace(params map[string]any, nsType string) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	id := shared.GenerateUUID()
	arn := shared.BuildARN("servicediscovery", "namespace", id)
	config := "{}"
	opID := shared.GenerateUUID()

	ns, err := p.store.CreateNamespace(id, arn, name, nsType, description, config)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("NamespaceAlreadyExists", "namespace already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	// Handle tags
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(ns.ARN, parseTags(rawTags)) //nolint:errcheck
	}

	targets := map[string]string{"NAMESPACE": id}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "CREATE_NAMESPACE", string(targetsJSON))

	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) getNamespace(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	ns, err := p.store.GetNamespace(id)
	if err != nil {
		return shared.JSONError("NamespaceNotFound", "namespace not found", http.StatusBadRequest), nil
	}
	count, _ := p.store.ServiceCount(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Namespace": namespaceToMap(ns, count),
	})
}

func (p *Provider) listNamespaces(_ map[string]any) (*plugin.Response, error) {
	namespaces, err := p.store.ListNamespaces()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(namespaces))
	for _, ns := range namespaces {
		count, _ := p.store.ServiceCount(ns.ID)
		list = append(list, namespaceToMap(&ns, count))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Namespaces": list,
		"NextToken":  "",
	})
}

func (p *Provider) deleteNamespace(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	ns, err := p.store.GetNamespace(id)
	if err != nil {
		return shared.JSONError("NamespaceNotFound", "namespace not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(ns.ARN) //nolint:errcheck
	if err := p.store.DeleteNamespace(id); err != nil {
		return shared.JSONError("NamespaceNotFound", "namespace not found", http.StatusBadRequest), nil
	}
	opID := shared.GenerateUUID()
	targets := map[string]string{"NAMESPACE": id}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "DELETE_NAMESPACE", string(targetsJSON))
	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) updateHttpNamespace(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	description := ""
	if ns, ok := params["Namespace"].(map[string]any); ok {
		description, _ = ns["Description"].(string)
	}
	if err := p.store.UpdateNamespace(id, description); err != nil {
		return shared.JSONError("NamespaceNotFound", "namespace not found", http.StatusBadRequest), nil
	}
	opID := shared.GenerateUUID()
	targets := map[string]string{"NAMESPACE": id}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "UPDATE_NAMESPACE", string(targetsJSON))
	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) updatePrivateDnsNamespace(params map[string]any) (*plugin.Response, error) {
	return p.updateHttpNamespace(params)
}

func (p *Provider) updatePublicDnsNamespace(params map[string]any) (*plugin.Response, error) {
	return p.updateHttpNamespace(params)
}

// Service operations

func (p *Provider) createService(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	namespaceID, _ := params["NamespaceId"].(string)

	dnsConfig := "{}"
	if dc, ok := params["DnsConfig"]; ok {
		b, _ := json.Marshal(dc)
		dnsConfig = string(b)
	}
	healthConfig := "{}"
	if hc, ok := params["HealthCheckConfig"]; ok {
		b, _ := json.Marshal(hc)
		healthConfig = string(b)
	}

	id := shared.GenerateUUID()
	arn := shared.BuildARN("servicediscovery", "service", id)

	svc, err := p.store.CreateService(id, arn, name, namespaceID, description, dnsConfig, healthConfig)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ServiceAlreadyExists", "service already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(svc.ARN, parseTags(rawTags)) //nolint:errcheck
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Service": serviceToMap(svc, 0),
	})
}

func (p *Provider) getService(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(id)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	count, _ := p.store.InstanceCount(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Service": serviceToMap(svc, count),
	})
}

func (p *Provider) listServices(_ map[string]any) (*plugin.Response, error) {
	services, err := p.store.ListServices("")
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(services))
	for _, svc := range services {
		count, _ := p.store.InstanceCount(svc.ID)
		list = append(list, serviceToMap(&svc, count))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Services":  list,
		"NextToken": "",
	})
}

func (p *Provider) updateService(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(id)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	description := svc.Description
	dnsConfig := svc.DnsConfig
	healthConfig := svc.HealthConfig

	if change, ok := params["Service"].(map[string]any); ok {
		if d, ok := change["Description"].(string); ok {
			description = d
		}
		if dc, ok := change["DnsConfig"]; ok {
			b, _ := json.Marshal(dc)
			dnsConfig = string(b)
		}
		if hc, ok := change["HealthCheckConfig"]; ok {
			b, _ := json.Marshal(hc)
			healthConfig = string(b)
		}
	}

	if err := p.store.UpdateService(id, description, dnsConfig, healthConfig); err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}

	opID := shared.GenerateUUID()
	targets := map[string]string{"SERVICE": id}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "UPDATE_SERVICE", string(targetsJSON))
	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) deleteService(params map[string]any) (*plugin.Response, error) {
	id, _ := params["Id"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(id)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(svc.ARN) //nolint:errcheck
	if err := p.store.DeleteService(id); err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// Instance operations

func (p *Provider) registerInstance(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	instanceID, _ := params["InstanceId"].(string)
	if instanceID == "" {
		return shared.JSONError("ValidationException", "InstanceId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetService(serviceID); err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}

	attrs := "{}"
	if a, ok := params["Attributes"]; ok {
		b, _ := json.Marshal(a)
		attrs = string(b)
	}

	if _, err := p.store.RegisterInstance(instanceID, serviceID, attrs); err != nil {
		return nil, err
	}

	opID := shared.GenerateUUID()
	targets := map[string]string{"INSTANCE": instanceID, "SERVICE": serviceID}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "REGISTER_INSTANCE", string(targetsJSON))
	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) deregisterInstance(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	instanceID, _ := params["InstanceId"].(string)
	if serviceID == "" || instanceID == "" {
		return shared.JSONError("ValidationException", "ServiceId and InstanceId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterInstance(instanceID, serviceID); err != nil {
		return shared.JSONError("InstanceNotFound", "instance not found", http.StatusBadRequest), nil
	}
	opID := shared.GenerateUUID()
	targets := map[string]string{"INSTANCE": instanceID, "SERVICE": serviceID}
	targetsJSON, _ := json.Marshal(targets)
	_, _ = p.store.CreateOperation(opID, "DEREGISTER_INSTANCE", string(targetsJSON))
	return shared.JSONResponse(http.StatusOK, map[string]any{"OperationId": opID})
}

func (p *Provider) getInstance(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	instanceID, _ := params["InstanceId"].(string)
	if serviceID == "" || instanceID == "" {
		return shared.JSONError("ValidationException", "ServiceId and InstanceId are required", http.StatusBadRequest), nil
	}
	inst, err := p.store.GetInstance(instanceID, serviceID)
	if err != nil {
		return shared.JSONError("InstanceNotFound", "instance not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Instance": instanceToMap(inst),
	})
}

func (p *Provider) listInstances(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	instances, err := p.store.ListInstances(serviceID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(instances))
	for _, inst := range instances {
		list = append(list, instanceSummaryToMap(&inst))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Instances": list,
		"NextToken": "",
	})
}

func (p *Provider) discoverInstances(params map[string]any) (*plugin.Response, error) {
	namespaceName, _ := params["NamespaceName"].(string)
	serviceName, _ := params["ServiceName"].(string)
	if namespaceName == "" || serviceName == "" {
		return shared.JSONError("ValidationException", "NamespaceName and ServiceName are required", http.StatusBadRequest), nil
	}

	// Find all services with matching name
	services, err := p.store.ListServices("")
	if err != nil {
		return nil, err
	}

	var matchedServiceID string
	for _, svc := range services {
		if svc.Name == serviceName {
			// Check namespace name matches
			ns, err := p.store.GetNamespace(svc.NamespaceID)
			if err == nil && ns.Name == namespaceName {
				matchedServiceID = svc.ID
				break
			}
		}
	}

	var instances []map[string]any
	if matchedServiceID != "" {
		insts, err := p.store.ListInstances(matchedServiceID)
		if err != nil {
			return nil, err
		}
		for _, inst := range insts {
			var attrs map[string]string
			_ = json.Unmarshal([]byte(inst.Attributes), &attrs)
			instances = append(instances, map[string]any{
				"InstanceId":    inst.ID,
				"NamespaceName": namespaceName,
				"ServiceName":   serviceName,
				"HealthStatus":  "HEALTHY",
				"Attributes":    attrs,
			})
		}
	}
	if instances == nil {
		instances = []map[string]any{}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Instances":         instances,
		"InstancesRevision": time.Now().Unix(),
	})
}

func (p *Provider) discoverInstancesRevision(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"InstancesRevision": time.Now().Unix(),
	})
}

func (p *Provider) getInstancesHealthStatus(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	instances, err := p.store.ListInstances(serviceID)
	if err != nil {
		return nil, err
	}
	status := make(map[string]string)
	for _, inst := range instances {
		status[inst.ID] = "HEALTHY"
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Status":    status,
		"NextToken": "",
	})
}

func (p *Provider) updateInstanceCustomHealthStatus(params map[string]any) (*plugin.Response, error) {
	// just acknowledge
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// Operation operations

func (p *Provider) getOperation(params map[string]any) (*plugin.Response, error) {
	opID, _ := params["OperationId"].(string)
	if opID == "" {
		return shared.JSONError("ValidationException", "OperationId is required", http.StatusBadRequest), nil
	}
	op, err := p.store.GetOperation(opID)
	if err != nil {
		return shared.JSONError("OperationNotFound", "operation not found", http.StatusBadRequest), nil
	}
	var targets map[string]string
	_ = json.Unmarshal([]byte(op.Targets), &targets)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Operation": map[string]any{
			"Id":         op.ID,
			"Type":       op.Type,
			"Status":     op.Status,
			"Targets":    targets,
			"CreateDate": op.CreatedAt,
			"UpdateDate": op.CreatedAt,
		},
	})
}

func (p *Provider) listOperations(_ map[string]any) (*plugin.Response, error) {
	ops, err := p.store.ListOperations()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(ops))
	for _, op := range ops {
		list = append(list, map[string]any{
			"Id":     op.ID,
			"Status": op.Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Operations": list,
		"NextToken":  "",
	})
}

// Service attributes (stored in tag store using special prefix)

func (p *Provider) getServiceAttributes(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(serviceID)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags("svc-attrs:" + svc.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServiceAttributes": map[string]any{
			"Attributes":    tags,
			"ServiceArn":    svc.ARN,
			"ResourceOwner": shared.DefaultAccountID,
		},
	})
}

func (p *Provider) updateServiceAttributes(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(serviceID)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	attrs, _ := params["Attributes"].(map[string]any)
	attrMap := make(map[string]string)
	for k, v := range attrs {
		if vs, ok := v.(string); ok {
			attrMap[k] = vs
		}
	}
	_ = p.store.tags.AddTags("svc-attrs:"+svc.ARN, attrMap) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteServiceAttributes(params map[string]any) (*plugin.Response, error) {
	serviceID, _ := params["ServiceId"].(string)
	if serviceID == "" {
		return shared.JSONError("ValidationException", "ServiceId is required", http.StatusBadRequest), nil
	}
	svc, err := p.store.GetService(serviceID)
	if err != nil {
		return shared.JSONError("ServiceNotFound", "service not found", http.StatusBadRequest), nil
	}
	keys, _ := params["Attributes"].([]any)
	strKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if s, ok := k.(string); ok {
			strKeys = append(strKeys, s)
		}
	}
	p.store.tags.RemoveTags("svc-attrs:"+svc.ARN, strKeys) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// Tag operations

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags": tagList,
	})
}

// Helpers

func namespaceToMap(ns *Namespace, serviceCount int32) map[string]any {
	return map[string]any{
		"Id":           ns.ID,
		"Arn":          ns.ARN,
		"Name":         ns.Name,
		"Type":         ns.Type,
		"Description":  ns.Description,
		"ServiceCount": serviceCount,
		"CreateDate":   ns.CreatedAt,
	}
}

func serviceToMap(svc *Service, instanceCount int32) map[string]any {
	var dnsConfig any
	var healthConfig any
	_ = json.Unmarshal([]byte(svc.DnsConfig), &dnsConfig)
	_ = json.Unmarshal([]byte(svc.HealthConfig), &healthConfig)
	return map[string]any{
		"Id":                svc.ID,
		"Arn":               svc.ARN,
		"Name":              svc.Name,
		"NamespaceId":       svc.NamespaceID,
		"Description":       svc.Description,
		"DnsConfig":         dnsConfig,
		"HealthCheckConfig": healthConfig,
		"InstanceCount":     instanceCount,
		"CreateDate":        svc.CreatedAt,
	}
}

func instanceToMap(inst *Instance) map[string]any {
	var attrs map[string]string
	_ = json.Unmarshal([]byte(inst.Attributes), &attrs)
	return map[string]any{
		"Id":         inst.ID,
		"Attributes": attrs,
	}
}

func instanceSummaryToMap(inst *Instance) map[string]any {
	var attrs map[string]string
	_ = json.Unmarshal([]byte(inst.Attributes), &attrs)
	return map[string]any{
		"Id":         inst.ID,
		"Attributes": attrs,
	}
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
