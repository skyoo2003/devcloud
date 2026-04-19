// SPDX-License-Identifier: Apache-2.0

// internal/services/iotwireless/provider.go
package iotwireless

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the IoT Wireless service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "iotwireless" }
func (p *Provider) ServiceName() string           { return "iotwireless" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "iotwireless"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}
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

	path := req.URL.Path

	switch op {
	// Destinations
	case "CreateDestination":
		return p.createDestination(params)
	case "GetDestination":
		name := extractPathParam(path, "destinations")
		return p.getDestination(name)
	case "ListDestinations":
		return p.listDestinations()
	case "UpdateDestination":
		name := extractPathParam(path, "destinations")
		return p.updateDestination(name, params)
	case "DeleteDestination":
		name := extractPathParam(path, "destinations")
		return p.deleteDestination(name)

	// Device Profiles
	case "CreateDeviceProfile":
		return p.createDeviceProfile(params)
	case "GetDeviceProfile":
		id := extractPathParam(path, "device-profiles")
		return p.getDeviceProfile(id)
	case "ListDeviceProfiles":
		return p.listDeviceProfiles()
	case "DeleteDeviceProfile":
		id := extractPathParam(path, "device-profiles")
		return p.deleteDeviceProfile(id)

	// Service Profiles
	case "CreateServiceProfile":
		return p.createServiceProfile(params)
	case "GetServiceProfile":
		id := extractPathParam(path, "service-profiles")
		return p.getServiceProfile(id)
	case "ListServiceProfiles":
		return p.listServiceProfiles()
	case "DeleteServiceProfile":
		id := extractPathParam(path, "service-profiles")
		return p.deleteServiceProfile(id)

	// Wireless Devices
	case "CreateWirelessDevice":
		return p.createWirelessDevice(params)
	case "GetWirelessDevice":
		id := extractPathParam(path, "wireless-devices")
		return p.getWirelessDevice(id)
	case "ListWirelessDevices":
		return p.listWirelessDevices()
	case "UpdateWirelessDevice":
		id := extractPathParam(path, "wireless-devices")
		return p.updateWirelessDevice(id, params)
	case "DeleteWirelessDevice":
		id := extractPathParam(path, "wireless-devices")
		return p.deleteWirelessDevice(id)

	// Wireless Gateways
	case "CreateWirelessGateway":
		return p.createWirelessGateway(params)
	case "GetWirelessGateway":
		id := extractPathParam(path, "wireless-gateways")
		return p.getWirelessGateway(id)
	case "ListWirelessGateways":
		return p.listWirelessGateways()
	case "UpdateWirelessGateway":
		id := extractPathParam(path, "wireless-gateways")
		return p.updateWirelessGateway(id, params)
	case "DeleteWirelessGateway":
		id := extractPathParam(path, "wireless-gateways")
		return p.deleteWirelessGateway(id)

	// Fuota Tasks
	case "CreateFuotaTask":
		return p.createFuotaTask(params)
	case "GetFuotaTask":
		id := extractPathParam(path, "fuota-tasks")
		return p.getFuotaTask(id)
	case "ListFuotaTasks":
		return p.listFuotaTasks()
	case "UpdateFuotaTask":
		id := extractPathParam(path, "fuota-tasks")
		return p.updateFuotaTask(id, params)
	case "DeleteFuotaTask":
		id := extractPathParam(path, "fuota-tasks")
		return p.deleteFuotaTask(id)
	case "StartFuotaTask":
		id := extractPathParam(path, "fuota-tasks")
		return p.startFuotaTask(id)

	// Multicast Groups
	case "CreateMulticastGroup":
		return p.createMulticastGroup(params)
	case "GetMulticastGroup":
		id := extractPathParam(path, "multicast-groups")
		return p.getMulticastGroup(id)
	case "ListMulticastGroups":
		return p.listMulticastGroups()
	case "UpdateMulticastGroup":
		id := extractPathParam(path, "multicast-groups")
		return p.updateMulticastGroup(id, params)
	case "DeleteMulticastGroup":
		id := extractPathParam(path, "multicast-groups")
		return p.deleteMulticastGroup(id)

	// Network Analyzer Configurations
	case "CreateNetworkAnalyzerConfiguration":
		return p.createNetworkAnalyzerConfig(params)
	case "GetNetworkAnalyzerConfiguration":
		name := extractPathParam(path, "network-analyzer-configurations")
		return p.getNetworkAnalyzerConfig(name)
	case "ListNetworkAnalyzerConfigurations":
		return p.listNetworkAnalyzerConfigs()
	case "UpdateNetworkAnalyzerConfiguration":
		name := extractPathParam(path, "network-analyzer-configurations")
		return p.updateNetworkAnalyzerConfig(name, params)
	case "DeleteNetworkAnalyzerConfiguration":
		name := extractPathParam(path, "network-analyzer-configurations")
		return p.deleteNetworkAnalyzerConfig(name)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Stub operations - return success/empty
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	wds, err := p.store.ListWirelessDevices()
	if err != nil {
		return nil, err
	}
	wgs, err := p.store.ListWirelessGateways()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(wds)+len(wgs))
	for _, wd := range wds {
		res = append(res, plugin.Resource{Type: "wireless-device", ID: wd.ID, Name: wd.Name})
	}
	for _, wg := range wgs {
		res = append(res, plugin.Resource{Type: "wireless-gateway", ID: wg.ID, Name: wg.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Destination operations ---

func (p *Provider) createDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iotwireless", "dest", name)
	d := &Destination{
		Name:           name,
		ARN:            arn,
		Expression:     strParam(params, "Expression"),
		ExpressionType: strParamDefault(params, "ExpressionType", "RuleName"),
		RoleARN:        strParam(params, "RoleArn"),
		Description:    strParam(params, "Description"),
	}
	if err := p.store.CreateDestination(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "destination already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Name": name})
}

func (p *Provider) getDestination(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDestination(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "destination not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(d.ARN)
	return shared.JSONResponse(http.StatusOK, destinationToMap(d, tags))
}

func (p *Provider) listDestinations() (*plugin.Response, error) {
	destinations, err := p.store.ListDestinations()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(destinations))
	for i := range destinations {
		tags, _ := p.store.tags.ListTags(destinations[i].ARN)
		result = append(result, destinationToMap(&destinations[i], tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DestinationList": result})
}

func (p *Provider) updateDestination(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDestination(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "destination not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteDestination(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	d, err := p.store.DeleteDestination(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "destination not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(d.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Device Profile operations ---

func (p *Provider) createDeviceProfile(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "device-profile", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	dp := &DeviceProfile{
		ID:     id,
		ARN:    arn,
		Name:   strParam(params, "Name"),
		Config: configJSON,
	}
	if err := p.store.CreateDeviceProfile(dp); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getDeviceProfile(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	dp, err := p.store.GetDeviceProfile(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "device profile not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(dp.ARN)
	return shared.JSONResponse(http.StatusOK, deviceProfileToMap(dp, tags))
}

func (p *Provider) listDeviceProfiles() (*plugin.Response, error) {
	profiles, err := p.store.ListDeviceProfiles()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(profiles))
	for i := range profiles {
		result = append(result, map[string]any{"Arn": profiles[i].ARN, "Id": profiles[i].ID, "Name": profiles[i].Name})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DeviceProfileList": result})
}

func (p *Provider) deleteDeviceProfile(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	dp, err := p.store.DeleteDeviceProfile(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "device profile not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(dp.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Service Profile operations ---

func (p *Provider) createServiceProfile(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "service-profile", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	sp := &ServiceProfile{
		ID:     id,
		ARN:    arn,
		Name:   strParam(params, "Name"),
		Config: configJSON,
	}
	if err := p.store.CreateServiceProfile(sp); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getServiceProfile(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	sp, err := p.store.GetServiceProfile(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "service profile not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(sp.ARN)
	return shared.JSONResponse(http.StatusOK, serviceProfileToMap(sp, tags))
}

func (p *Provider) listServiceProfiles() (*plugin.Response, error) {
	profiles, err := p.store.ListServiceProfiles()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(profiles))
	for i := range profiles {
		result = append(result, map[string]any{"Arn": profiles[i].ARN, "Id": profiles[i].ID, "Name": profiles[i].Name})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceProfileList": result})
}

func (p *Provider) deleteServiceProfile(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	sp, err := p.store.DeleteServiceProfile(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "service profile not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(sp.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Wireless Device operations ---

func (p *Provider) createWirelessDevice(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "device", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	wd := &WirelessDevice{
		ID:          id,
		ARN:         arn,
		Name:        strParam(params, "Name"),
		Type:        strParamDefault(params, "Type", "LoRaWAN"),
		Destination: strParam(params, "DestinationName"),
		Description: strParam(params, "Description"),
		Config:      configJSON,
	}
	if err := p.store.CreateWirelessDevice(wd); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getWirelessDevice(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	wd, err := p.store.GetWirelessDevice(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless device not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(wd.ARN)
	return shared.JSONResponse(http.StatusOK, wirelessDeviceToMap(wd, tags))
}

func (p *Provider) listWirelessDevices() (*plugin.Response, error) {
	devices, err := p.store.ListWirelessDevices()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(devices))
	for i := range devices {
		result = append(result, map[string]any{
			"Arn":             devices[i].ARN,
			"Id":              devices[i].ID,
			"Name":            devices[i].Name,
			"Type":            devices[i].Type,
			"DestinationName": devices[i].Destination,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"WirelessDeviceList": result})
}

func (p *Provider) updateWirelessDevice(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateWirelessDevice(id, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless device not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteWirelessDevice(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	wd, err := p.store.DeleteWirelessDevice(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless device not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(wd.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Wireless Gateway operations ---

func (p *Provider) createWirelessGateway(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "gateway", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	wg := &WirelessGateway{
		ID:          id,
		ARN:         arn,
		Name:        strParam(params, "Name"),
		Description: strParam(params, "Description"),
		Config:      configJSON,
	}
	if err := p.store.CreateWirelessGateway(wg); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getWirelessGateway(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	wg, err := p.store.GetWirelessGateway(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless gateway not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(wg.ARN)
	return shared.JSONResponse(http.StatusOK, wirelessGatewayToMap(wg, tags))
}

func (p *Provider) listWirelessGateways() (*plugin.Response, error) {
	gateways, err := p.store.ListWirelessGateways()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(gateways))
	for i := range gateways {
		result = append(result, map[string]any{
			"Arn":  gateways[i].ARN,
			"Id":   gateways[i].ID,
			"Name": gateways[i].Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"WirelessGatewayList": result})
}

func (p *Provider) updateWirelessGateway(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateWirelessGateway(id, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless gateway not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteWirelessGateway(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	wg, err := p.store.DeleteWirelessGateway(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "wireless gateway not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(wg.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Fuota Task operations ---

func (p *Provider) createFuotaTask(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "fuota-task", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	ft := &FuotaTask{
		ID:     id,
		ARN:    arn,
		Name:   strParam(params, "Name"),
		Status: "Pending",
		Config: configJSON,
	}
	if err := p.store.CreateFuotaTask(ft); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getFuotaTask(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	ft, err := p.store.GetFuotaTask(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "fuota task not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(ft.ARN)
	return shared.JSONResponse(http.StatusOK, fuotaTaskToMap(ft, tags))
}

func (p *Provider) listFuotaTasks() (*plugin.Response, error) {
	tasks, err := p.store.ListFuotaTasks()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(tasks))
	for i := range tasks {
		result = append(result, map[string]any{
			"Arn":    tasks[i].ARN,
			"Id":     tasks[i].ID,
			"Name":   tasks[i].Name,
			"Status": tasks[i].Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FuotaTaskList": result})
}

func (p *Provider) updateFuotaTask(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateFuotaTask(id, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fuota task not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteFuotaTask(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	ft, err := p.store.DeleteFuotaTask(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "fuota task not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(ft.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) startFuotaTask(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateFuotaTask(id, map[string]any{"Status": "FuotaSession_Waiting"}); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fuota task not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Multicast Group operations ---

func (p *Provider) createMulticastGroup(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	arn := shared.BuildARN("iotwireless", "multicast-group", id)
	configJSON := "{}"
	if v, ok := params["LoRaWAN"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	mg := &MulticastGroup{
		ID:     id,
		ARN:    arn,
		Name:   strParam(params, "Name"),
		Status: "Active",
		Config: configJSON,
	}
	if err := p.store.CreateMulticastGroup(mg); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Id": id})
}

func (p *Provider) getMulticastGroup(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	mg, err := p.store.GetMulticastGroup(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "multicast group not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(mg.ARN)
	return shared.JSONResponse(http.StatusOK, multicastGroupToMap(mg, tags))
}

func (p *Provider) listMulticastGroups() (*plugin.Response, error) {
	groups, err := p.store.ListMulticastGroups()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(groups))
	for i := range groups {
		result = append(result, map[string]any{
			"Arn":    groups[i].ARN,
			"Id":     groups[i].ID,
			"Name":   groups[i].Name,
			"Status": groups[i].Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MulticastGroupList": result})
}

func (p *Provider) updateMulticastGroup(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateMulticastGroup(id, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "multicast group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteMulticastGroup(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "Id is required", http.StatusBadRequest), nil
	}
	mg, err := p.store.DeleteMulticastGroup(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "multicast group not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(mg.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Network Analyzer Configuration operations ---

func (p *Provider) createNetworkAnalyzerConfig(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iotwireless", "network-analyzer-configuration", name)
	configJSON := "{}"
	if v, ok := params["TraceContent"]; ok {
		b, _ := json.Marshal(v)
		configJSON = string(b)
	}
	nac := &NetworkAnalyzerConfig{
		Name:   name,
		ARN:    arn,
		Config: configJSON,
	}
	if err := p.store.CreateNetworkAnalyzerConfig(nac); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "network analyzer configuration already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{"Arn": arn, "Name": name})
}

func (p *Provider) getNetworkAnalyzerConfig(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	nac, err := p.store.GetNetworkAnalyzerConfig(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "network analyzer configuration not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(nac.ARN)
	return shared.JSONResponse(http.StatusOK, networkAnalyzerConfigToMap(nac, tags))
}

func (p *Provider) listNetworkAnalyzerConfigs() (*plugin.Response, error) {
	configs, err := p.store.ListNetworkAnalyzerConfigs()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(configs))
	for i := range configs {
		result = append(result, map[string]any{"Arn": configs[i].ARN, "Name": configs[i].Name})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NetworkAnalyzerConfigurationList": result})
}

func (p *Provider) updateNetworkAnalyzerConfig(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateNetworkAnalyzerConfig(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "network analyzer configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) deleteNetworkAnalyzerConfig(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	nac, err := p.store.DeleteNetworkAnalyzerConfig(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "network analyzer configuration not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(nac.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Tags operations ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn query parameter is required", http.StatusBadRequest), nil
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn query parameter is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	p.store.tags.RemoveTags(arn, keys) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn query parameter is required", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(arn)
	tagList := make([]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// --- Serialization helpers ---

func destinationToMap(d *Destination, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":            d.ARN,
		"Name":           d.Name,
		"Expression":     d.Expression,
		"ExpressionType": d.ExpressionType,
		"RoleArn":        d.RoleARN,
		"Description":    d.Description,
		"Tags":           tagsMapToList(tags),
	}
}

func deviceProfileToMap(dp *DeviceProfile, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":  dp.ARN,
		"Id":   dp.ID,
		"Name": dp.Name,
		"Tags": tagsMapToList(tags),
	}
}

func serviceProfileToMap(sp *ServiceProfile, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":  sp.ARN,
		"Id":   sp.ID,
		"Name": sp.Name,
		"Tags": tagsMapToList(tags),
	}
}

func wirelessDeviceToMap(wd *WirelessDevice, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":             wd.ARN,
		"Id":              wd.ID,
		"Name":            wd.Name,
		"Type":            wd.Type,
		"DestinationName": wd.Destination,
		"ThingArn":        wd.ThingARN,
		"Description":     wd.Description,
		"Tags":            tagsMapToList(tags),
	}
}

func wirelessGatewayToMap(wg *WirelessGateway, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":         wg.ARN,
		"Id":          wg.ID,
		"Name":        wg.Name,
		"Description": wg.Description,
		"ThingArn":    wg.ThingARN,
		"Tags":        tagsMapToList(tags),
	}
}

func fuotaTaskToMap(ft *FuotaTask, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":    ft.ARN,
		"Id":     ft.ID,
		"Name":   ft.Name,
		"Status": ft.Status,
		"Tags":   tagsMapToList(tags),
	}
}

func multicastGroupToMap(mg *MulticastGroup, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":    mg.ARN,
		"Id":     mg.ID,
		"Name":   mg.Name,
		"Status": mg.Status,
		"Tags":   tagsMapToList(tags),
	}
}

func networkAnalyzerConfigToMap(nac *NetworkAnalyzerConfig, tags map[string]string) map[string]any {
	return map[string]any{
		"Arn":  nac.ARN,
		"Name": nac.Name,
		"Tags": tagsMapToList(tags),
	}
}

// --- Utility functions ---

// resolveOp maps HTTP method+path to an IoT Wireless operation name.
func resolveOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	type crudOps struct {
		create, get, list, update, delete string
	}
	resources := map[string]crudOps{
		"destinations":                    {"CreateDestination", "GetDestination", "ListDestinations", "UpdateDestination", "DeleteDestination"},
		"device-profiles":                 {"CreateDeviceProfile", "GetDeviceProfile", "ListDeviceProfiles", "", "DeleteDeviceProfile"},
		"service-profiles":                {"CreateServiceProfile", "GetServiceProfile", "ListServiceProfiles", "", "DeleteServiceProfile"},
		"wireless-devices":                {"CreateWirelessDevice", "GetWirelessDevice", "ListWirelessDevices", "UpdateWirelessDevice", "DeleteWirelessDevice"},
		"wireless-gateways":               {"CreateWirelessGateway", "GetWirelessGateway", "ListWirelessGateways", "UpdateWirelessGateway", "DeleteWirelessGateway"},
		"fuota-tasks":                     {"CreateFuotaTask", "GetFuotaTask", "ListFuotaTasks", "UpdateFuotaTask", "DeleteFuotaTask"},
		"multicast-groups":                {"CreateMulticastGroup", "GetMulticastGroup", "ListMulticastGroups", "UpdateMulticastGroup", "DeleteMulticastGroup"},
		"network-analyzer-configurations": {"CreateNetworkAnalyzerConfiguration", "GetNetworkAnalyzerConfiguration", "ListNetworkAnalyzerConfigurations", "UpdateNetworkAnalyzerConfiguration", "DeleteNetworkAnalyzerConfiguration"},
	}

	first := segs[0]

	if ops, ok := resources[first]; ok {
		if n == 1 {
			switch method {
			case "POST":
				return ops.create
			case "GET":
				return ops.list
			}
		}
		if n == 2 {
			switch method {
			case "GET":
				return ops.get
			case "PATCH":
				return ops.update
			case "DELETE":
				return ops.delete
			}
		}
		// Special case: POST /fuota-tasks/{id}/start
		if first == "fuota-tasks" && n == 3 && segs[2] == "start" && method == "PUT" {
			return "StartFuotaTask"
		}
	}

	if first == "tags" && n >= 2 {
		switch method {
		case "PUT":
			return "TagResource"
		case "DELETE":
			return "UntagResource"
		case "GET":
			return "ListTagsForResource"
		}
	}

	return ""
}

func extractPathParam(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func strParamDefault(params map[string]any, key, def string) string {
	v, _ := params[key].(string)
	if v == "" {
		return def
	}
	return v
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func tagsListToMap(list []any) map[string]string {
	m := make(map[string]string, len(list))
	for _, item := range list {
		if t, ok := item.(map[string]any); ok {
			k, _ := t["Key"].(string)
			v, _ := t["Value"].(string)
			if k != "" {
				m[k] = v
			}
		}
	}
	return m
}

func tagsMapToList(tags map[string]string) []any {
	list := make([]any, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]any{"Key": k, "Value": v})
	}
	return list
}
