// SPDX-License-Identifier: Apache-2.0

// internal/services/mq/provider.go
package mq

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

// Provider implements the Amazon MQ service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "mq" }
func (p *Provider) ServiceName() string           { return "mq" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "mq"))
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

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// Broker
	case "CreateBroker":
		return p.createBroker(params)
	case "DescribeBroker":
		id := extractPathParam(req.URL.Path, "brokers")
		return p.describeBroker(id)
	case "ListBrokers":
		return p.listBrokers()
	case "DeleteBroker":
		id := extractPathParam(req.URL.Path, "brokers")
		return p.deleteBroker(id)
	case "UpdateBroker":
		id := extractPathParam(req.URL.Path, "brokers")
		return p.updateBroker(id, params)
	case "RebootBroker":
		id := extractPathParam(req.URL.Path, "brokers")
		return p.rebootBroker(id)
	case "Promote":
		id := extractPathParam(req.URL.Path, "brokers")
		return p.promote(id)

	// Configuration
	case "CreateConfiguration":
		return p.createConfiguration(params)
	case "DescribeConfiguration":
		id := extractPathParam(req.URL.Path, "configurations")
		return p.describeConfiguration(id)
	case "DescribeConfigurationRevision":
		id := extractPathParam(req.URL.Path, "configurations")
		return p.describeConfigurationRevision(id)
	case "ListConfigurations":
		return p.listConfigurations()
	case "ListConfigurationRevisions":
		id := extractPathParam(req.URL.Path, "configurations")
		return p.listConfigurationRevisions(id)
	case "UpdateConfiguration":
		id := extractPathParam(req.URL.Path, "configurations")
		return p.updateConfiguration(id, params)
	case "DeleteConfiguration":
		id := extractPathParam(req.URL.Path, "configurations")
		return p.deleteConfiguration(id)

	// User
	case "CreateUser":
		brokerID := extractPathParam(req.URL.Path, "brokers")
		username := extractPathParam(req.URL.Path, "users")
		return p.createUser(brokerID, username, params)
	case "DescribeUser":
		brokerID := extractPathParam(req.URL.Path, "brokers")
		username := extractPathParam(req.URL.Path, "users")
		return p.describeUser(brokerID, username)
	case "ListUsers":
		brokerID := extractPathParam(req.URL.Path, "brokers")
		return p.listUsers(brokerID)
	case "UpdateUser":
		brokerID := extractPathParam(req.URL.Path, "brokers")
		username := extractPathParam(req.URL.Path, "users")
		return p.updateUser(brokerID, username, params)
	case "DeleteUser":
		brokerID := extractPathParam(req.URL.Path, "brokers")
		username := extractPathParam(req.URL.Path, "users")
		return p.deleteUser(brokerID, username)

	// Tags
	case "CreateTags":
		return p.createTags(req, params)
	case "DeleteTags":
		return p.deleteTags(req)
	case "ListTags":
		return p.listTags(req)

	// Static
	case "DescribeBrokerEngineTypes":
		return p.describeBrokerEngineTypes()
	case "DescribeBrokerInstanceOptions":
		return p.describeBrokerInstanceOptions()

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.TrimPrefix(path, "/v1")
	p = strings.Trim(p, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /v1/tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "CreateTags"
		case http.MethodGet:
			return "ListTags"
		case http.MethodDelete:
			return "DeleteTags"
		}

	// Broker engine types / instance options
	case n >= 2 && seg[0] == "broker-engine-types":
		return "DescribeBrokerEngineTypes"
	case n >= 2 && seg[0] == "broker-instance-options":
		return "DescribeBrokerInstanceOptions"

	// Brokers
	case n >= 1 && seg[0] == "brokers":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateBroker"
			case http.MethodGet:
				return "ListBrokers"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "DescribeBroker"
			case http.MethodPut:
				return "UpdateBroker"
			case http.MethodDelete:
				return "DeleteBroker"
			}
		}
		// /brokers/{id}/reboot
		if n == 3 && seg[2] == "reboot" && method == http.MethodPost {
			return "RebootBroker"
		}
		// /brokers/{id}/promote
		if n == 3 && seg[2] == "promote" && method == http.MethodPost {
			return "Promote"
		}
		// /brokers/{id}/users
		if n >= 3 && seg[2] == "users" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateUser"
				case http.MethodGet:
					return "ListUsers"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribeUser"
				case http.MethodPut:
					return "UpdateUser"
				case http.MethodDelete:
					return "DeleteUser"
				}
			}
		}

	// Configurations
	case n >= 1 && seg[0] == "configurations":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateConfiguration"
			case http.MethodGet:
				return "ListConfigurations"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "DescribeConfiguration"
			case http.MethodPut:
				return "UpdateConfiguration"
			case http.MethodDelete:
				return "DeleteConfiguration"
			}
		}
		// /configurations/{id}/revisions/{rev}
		if n >= 3 && seg[2] == "revisions" {
			if n == 3 && method == http.MethodGet {
				return "ListConfigurationRevisions"
			}
			if n == 4 && method == http.MethodGet {
				return "DescribeConfigurationRevision"
			}
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	brokers, err := p.store.ListBrokers()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(brokers))
	for _, b := range brokers {
		res = append(res, plugin.Resource{Type: "mq-broker", ID: b.ID, Name: b.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Broker ----

func (p *Provider) createBroker(params map[string]any) (*plugin.Response, error) {
	name, _ := params["brokerName"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "brokerName is required", http.StatusBadRequest), nil
	}

	engineType := "ACTIVEMQ"
	if v, ok := params["engineType"].(string); ok && v != "" {
		engineType = v
	}
	engineVersion := "5.17.6"
	if v, ok := params["engineVersion"].(string); ok && v != "" {
		engineVersion = v
	}
	hostInstance := "mq.m5.large"
	if v, ok := params["hostInstanceType"].(string); ok && v != "" {
		hostInstance = v
	}
	deploymentMode := "SINGLE_INSTANCE"
	if v, ok := params["deploymentMode"].(string); ok && v != "" {
		deploymentMode = v
	}
	publicly := false
	if v, ok := params["publiclyAccessible"].(bool); ok {
		publicly = v
	}
	autoMinor := true
	if v, ok := params["autoMinorVersionUpgrade"].(bool); ok {
		autoMinor = v
	}

	id := shared.GenerateID("b-", 26)
	arn := shared.BuildARN("mq", "broker", id)
	consoleURL := fmt.Sprintf("https://%s.mq.%s.amazonaws.com:8162", id, shared.DefaultRegion)
	endpoints := []string{
		fmt.Sprintf("ssl://%s.mq.%s.amazonaws.com:61617", id, shared.DefaultRegion),
	}

	b := &Broker{
		ID:                 id,
		ARN:                arn,
		Name:               name,
		EngineType:         engineType,
		EngineVersion:      engineVersion,
		HostInstance:       hostInstance,
		DeploymentMode:     deploymentMode,
		Status:             "RUNNING",
		PubliclyAccessible: publicly,
		AutoMinorUpgrade:   autoMinor,
		ConsoleURL:         consoleURL,
		Endpoints:          endpoints,
		Config:             "{}",
	}

	if err := p.store.CreateBroker(b); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "broker already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	// Handle tags
	if rawTags, ok := params["tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(arn, tags)
	}

	// Handle initial users
	if rawUsers, ok := params["users"].([]any); ok {
		for _, ru := range rawUsers {
			if um, ok := ru.(map[string]any); ok {
				uname, _ := um["username"].(string)
				pw, _ := um["password"].(string)
				if uname != "" {
					u := &BrokerUser{
						BrokerID: id,
						Username: uname,
						Password: pw,
						Groups:   []string{},
					}
					p.store.CreateUser(u)
				}
			}
		}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId":  id,
		"brokerArn": arn,
	})
}

func (p *Provider) describeBroker(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBroker(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(b.ARN)
	users, _ := p.store.ListUsers(id)
	userSummaries := make([]map[string]any, 0, len(users))
	for _, u := range users {
		userSummaries = append(userSummaries, map[string]any{
			"username":      u.Username,
			"pendingChange": "",
		})
	}
	return shared.JSONResponse(http.StatusOK, brokerToMap(b, tags, userSummaries))
}

func (p *Provider) listBrokers() (*plugin.Response, error) {
	brokers, err := p.store.ListBrokers()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(brokers))
	for _, b := range brokers {
		summaries = append(summaries, map[string]any{
			"brokerId":         b.ID,
			"brokerArn":        b.ARN,
			"brokerName":       b.Name,
			"brokerState":      b.Status,
			"deploymentMode":   b.DeploymentMode,
			"engineType":       b.EngineType,
			"hostInstanceType": b.HostInstance,
			"created":          b.CreatedAt.Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerSummaries": summaries,
	})
}

func (p *Provider) updateBroker(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBroker(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateBroker(id, params); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId": b.ID,
	})
}

func (p *Provider) deleteBroker(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBroker(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(b.ARN)
	if err := p.store.DeleteBroker(id); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId": b.ID,
	})
}

func (p *Provider) rebootBroker(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBroker(id); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) promote(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBroker(id); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId": id,
	})
}

// ---- Configuration ----

func (p *Provider) createConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	engineType := "ACTIVEMQ"
	if v, ok := params["engineType"].(string); ok && v != "" {
		engineType = v
	}
	engineVersion := "5.17.6"
	if v, ok := params["engineVersion"].(string); ok && v != "" {
		engineVersion = v
	}

	id := shared.GenerateID("c-", 26)
	arn := shared.BuildARN("mq", "configuration", id)

	c := &MQConfiguration{
		ID:            id,
		ARN:           arn,
		Name:          name,
		EngineType:    engineType,
		EngineVersion: engineVersion,
		Data:          "",
		Revision:      1,
	}

	if err := p.store.CreateConfiguration(c); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "configuration already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"id":      id,
		"arn":     arn,
		"name":    name,
		"created": c.CreatedAt.Format(time.RFC3339),
		"latestRevision": map[string]any{
			"revision": 1,
			"created":  c.CreatedAt.Format(time.RFC3339),
		},
	})
}

func (p *Provider) describeConfiguration(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "configuration id is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetConfiguration(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(c.ARN)
	return shared.JSONResponse(http.StatusOK, configurationToMap(c, tags))
}

func (p *Provider) describeConfigurationRevision(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "configuration id is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetConfiguration(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"configurationId": c.ID,
		"data":            c.Data,
		"created":         c.CreatedAt.Format(time.RFC3339),
		"description":     "",
	})
}

func (p *Provider) listConfigurations() (*plugin.Response, error) {
	cfgs, err := p.store.ListConfigurations()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(cfgs))
	for _, c := range cfgs {
		tags, _ := p.store.tags.ListTags(c.ARN)
		items = append(items, configurationToMap(&c, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"configurations": items,
	})
}

func (p *Provider) listConfigurationRevisions(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "configuration id is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetConfiguration(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	revisions := make([]map[string]any, 0, c.Revision)
	for i := 1; i <= c.Revision; i++ {
		revisions = append(revisions, map[string]any{
			"revision":    i,
			"created":     c.CreatedAt.Format(time.RFC3339),
			"description": "",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"configurationId": id,
		"revisions":       revisions,
	})
}

func (p *Provider) updateConfiguration(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "configuration id is required", http.StatusBadRequest), nil
	}
	data, _ := params["data"].(string)
	c, err := p.store.UpdateConfiguration(id, data)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"id":   c.ID,
		"arn":  c.ARN,
		"name": c.Name,
		"latestRevision": map[string]any{
			"revision": c.Revision,
			"created":  c.CreatedAt.Format(time.RFC3339),
		},
		"warnings": []any{},
	})
}

func (p *Provider) deleteConfiguration(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("BadRequestException", "configuration id is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetConfiguration(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(c.ARN)
	if err := p.store.DeleteConfiguration(id); err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"configurationId": id,
	})
}

// ---- User ----

func (p *Provider) createUser(brokerID, username string, params map[string]any) (*plugin.Response, error) {
	if brokerID == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	if username == "" {
		if v, ok := params["username"].(string); ok {
			username = v
		}
	}
	if username == "" {
		return shared.JSONError("BadRequestException", "username is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBroker(brokerID); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}

	pw, _ := params["password"].(string)
	consoleAccess := false
	if v, ok := params["consoleAccess"].(bool); ok {
		consoleAccess = v
	}
	groups := []string{}
	if v, ok := params["groups"].([]any); ok {
		for _, g := range v {
			if s, ok := g.(string); ok {
				groups = append(groups, s)
			}
		}
	}

	u := &BrokerUser{
		BrokerID:      brokerID,
		Username:      username,
		Password:      pw,
		ConsoleAccess: consoleAccess,
		Groups:        groups,
	}
	if err := p.store.CreateUser(u); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "user already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeUser(brokerID, username string) (*plugin.Response, error) {
	if brokerID == "" || username == "" {
		return shared.JSONError("BadRequestException", "broker id and username are required", http.StatusBadRequest), nil
	}
	u, err := p.store.GetUser(brokerID, username)
	if err != nil {
		return shared.JSONError("NotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId":      u.BrokerID,
		"username":      u.Username,
		"consoleAccess": u.ConsoleAccess,
		"groups":        u.Groups,
	})
}

func (p *Provider) listUsers(brokerID string) (*plugin.Response, error) {
	if brokerID == "" {
		return shared.JSONError("BadRequestException", "broker id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBroker(brokerID); err != nil {
		return shared.JSONError("NotFoundException", "broker not found", http.StatusNotFound), nil
	}
	users, err := p.store.ListUsers(brokerID)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, map[string]any{
			"username":      u.Username,
			"pendingChange": "",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerId": brokerID,
		"users":    summaries,
	})
}

func (p *Provider) updateUser(brokerID, username string, params map[string]any) (*plugin.Response, error) {
	if brokerID == "" || username == "" {
		return shared.JSONError("BadRequestException", "broker id and username are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateUser(brokerID, username, params); err != nil {
		return shared.JSONError("NotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteUser(brokerID, username string) (*plugin.Response, error) {
	if brokerID == "" || username == "" {
		return shared.JSONError("BadRequestException", "broker id and username are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUser(brokerID, username); err != nil {
		return shared.JSONError("NotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Tags ----

func (p *Provider) createTags(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteTags(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTags(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// ---- Static ----

func (p *Provider) describeBrokerEngineTypes() (*plugin.Response, error) {
	types := []map[string]any{
		{
			"engineType": "ACTIVEMQ",
			"engineVersions": []map[string]any{
				{"name": "5.15.14"},
				{"name": "5.15.16"},
				{"name": "5.16.7"},
				{"name": "5.17.6"},
				{"name": "5.18.3"},
			},
		},
		{
			"engineType": "RABBITMQ",
			"engineVersions": []map[string]any{
				{"name": "3.10.20"},
				{"name": "3.11.20"},
				{"name": "3.12.13"},
			},
		},
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerEngineTypes": types,
	})
}

func (p *Provider) describeBrokerInstanceOptions() (*plugin.Response, error) {
	options := []map[string]any{
		{
			"engineType":       "ACTIVEMQ",
			"hostInstanceType": "mq.m5.large",
			"storageType":      "efs",
			"supportedDeploymentModes": []string{
				"SINGLE_INSTANCE",
				"ACTIVE_STANDBY_MULTI_AZ",
			},
			"supportedEngineVersions": []string{"5.15.14", "5.15.16", "5.16.7", "5.17.6", "5.18.3"},
			"availabilityZones": []map[string]any{
				{"name": "us-east-1a"},
				{"name": "us-east-1b"},
				{"name": "us-east-1c"},
			},
		},
		{
			"engineType":       "RABBITMQ",
			"hostInstanceType": "mq.m5.large",
			"storageType":      "ebs",
			"supportedDeploymentModes": []string{
				"SINGLE_INSTANCE",
				"CLUSTER_MULTI_AZ",
			},
			"supportedEngineVersions": []string{"3.10.20", "3.11.20", "3.12.13"},
			"availabilityZones": []map[string]any{
				{"name": "us-east-1a"},
				{"name": "us-east-1b"},
				{"name": "us-east-1c"},
			},
		},
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"brokerInstanceOptions": options,
	})
}

// ---- Helpers ----

func brokerToMap(b *Broker, tags map[string]string, users []map[string]any) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	if users == nil {
		users = []map[string]any{}
	}
	instances := []map[string]any{
		{
			"consoleURL": b.ConsoleURL,
			"endpoints":  b.Endpoints,
		},
	}
	return map[string]any{
		"brokerId":                b.ID,
		"brokerArn":               b.ARN,
		"brokerName":              b.Name,
		"brokerState":             b.Status,
		"engineType":              b.EngineType,
		"engineVersion":           b.EngineVersion,
		"hostInstanceType":        b.HostInstance,
		"deploymentMode":          b.DeploymentMode,
		"publiclyAccessible":      b.PubliclyAccessible,
		"autoMinorVersionUpgrade": b.AutoMinorUpgrade,
		"brokerInstances":         instances,
		"created":                 b.CreatedAt.Format(time.RFC3339),
		"tags":                    tags,
		"users":                   users,
	}
}

func configurationToMap(c *MQConfiguration, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"id":            c.ID,
		"arn":           c.ARN,
		"name":          c.Name,
		"engineType":    c.EngineType,
		"engineVersion": c.EngineVersion,
		"created":       c.CreatedAt.Format(time.RFC3339),
		"latestRevision": map[string]any{
			"revision": c.Revision,
			"created":  c.CreatedAt.Format(time.RFC3339),
		},
		"tags": tags,
	}
}

func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
