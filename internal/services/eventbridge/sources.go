// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func canonicalError(err error) (*plugin.Response, error) {
	switch {
	case errors.Is(err, ErrSourceNotFound), errors.Is(err, ErrConnectionNotFound), errors.Is(err, ErrBusNotFound), errors.Is(err, ErrPermissionNotFound):
		return ebError("ResourceNotFoundException", err.Error(), 400), nil
	case errors.Is(err, ErrAlreadyExists):
		return ebError("ResourceAlreadyExistsException", err.Error(), 400), nil
	case errors.Is(err, ErrInvalidSourceState):
		return ebError("InvalidStateException", err.Error(), 400), nil
	default:
		return ebError("InternalException", "EventBridge storage operation failed", 500), nil
	}
}

func unitResponse() (*plugin.Response, error) {
	return &plugin.Response{StatusCode: http.StatusOK, ContentType: "application/x-amz-json-1.1"}, nil
}

func listParameters(params map[string]any) (int, string, error) {
	limit := 100
	if value, ok := params["Limit"]; ok {
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number || number < 1 || number > 100 {
			return 0, "", errors.New("limit must be an integer between 1 and 100")
		}
		limit = int(number)
	}
	token := ""
	if value, ok := params["NextToken"]; ok {
		var valid bool
		token, valid = value.(string)
		if !valid || token == "" {
			return 0, "", errInvalidPageToken
		}
	}
	return limit, token, nil
}

func sourceResponse(source PartnerSource, partner bool) map[string]any {
	result := map[string]any{"Arn": source.ARN, "Name": source.Name}
	if !partner {
		result["CreatedBy"] = source.CreatedBy
		result["CreationTime"] = float64(source.CreationTime.UnixNano()) / 1e9
		result["State"] = source.State
	}
	return result
}

func (p *Provider) handleSourceOperation(action string, params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if action == "ListPartnerEventSourceAccounts" {
		name, _ = params["EventSourceName"].(string)
	}
	switch action {
	case "ListEventSources", "ListPartnerEventSources", "ListPartnerEventSourceAccounts":
		limit, token, err := listParameters(params)
		if err != nil {
			return ebError("InvalidParameterException", err.Error(), 400), nil
		}
		prefix := ""
		if value, ok := params["NamePrefix"]; ok {
			var valid bool
			prefix, valid = value.(string)
			if !valid {
				return ebError("InvalidParameterException", "NamePrefix must be a string", 400), nil
			}
		}
		filters := map[string]string{"NamePrefix": prefix, "Account": defaultAccountID}
		field := "EventSources"
		sources := []PartnerSource{}
		if action == "ListPartnerEventSourceAccounts" {
			if !partnerSourceNamePattern.MatchString(name) || len(name) > 256 {
				return ebError("InvalidParameterException", "valid EventSourceName is required", 400), nil
			}
			source, err := p.store.GetPartnerSource(name, defaultAccountID)
			if err != nil {
				return canonicalError(err)
			}
			sources = append(sources, *source)
			filters["EventSourceName"] = name
			field = "PartnerEventSourceAccounts"
		} else {
			all, err := p.store.ListPartnerSources(defaultAccountID)
			if err != nil {
				return canonicalError(err)
			}
			for _, source := range all {
				if strings.HasPrefix(source.Name, prefix) {
					sources = append(sources, source)
				}
			}
			if action == "ListPartnerEventSources" {
				field = "PartnerEventSources"
			}
		}
		page, next, err := pageItems(action, filters, sources, func(source PartnerSource) string { return source.Name + "/" + source.AccountID }, limit, token)
		if err != nil {
			return ebError("InvalidParameterException", err.Error(), 400), nil
		}
		views := []map[string]any{}
		for _, source := range page {
			if action == "ListPartnerEventSourceAccounts" {
				views = append(views, map[string]any{"Account": source.AccountID, "CreationTime": float64(source.CreationTime.UnixNano()) / 1e9, "State": source.State})
			} else {
				views = append(views, sourceResponse(source, action == "ListPartnerEventSources"))
			}
		}
		result := map[string]any{field: views}
		if next != "" {
			result["NextToken"] = next
		}
		return jsonResp(200, result)
	}
	if len(name) > 256 || !partnerSourceNamePattern.MatchString(name) {
		return ebError("InvalidParameterException", "valid partner source Name is required", 400), nil
	}
	switch action {
	case "CreatePartnerEventSource", "DeletePartnerEventSource":
		account, _ := params["Account"].(string)
		if account != defaultAccountID {
			return ebError("InvalidParameterException", "Account must be the local 12-digit account", 400), nil
		}
		if action == "DeletePartnerEventSource" {
			if err := p.store.DeletePartnerSource(name, account); err != nil {
				return canonicalError(err)
			}
			return unitResponse()
		}
		source := PartnerSource{Name: name, AccountID: account, ARN: "arn:aws:events:us-east-1:" + account + ":event-source/" + name, CreatedBy: account, State: "PENDING", CreationTime: time.Now().UTC()}
		if err := p.store.CreatePartnerSource(source); err != nil {
			return canonicalError(err)
		}
		return jsonResp(200, map[string]any{"EventSourceArn": source.ARN})
	case "ActivateEventSource", "DeactivateEventSource":
		state := "ACTIVE"
		if action == "DeactivateEventSource" {
			state = "PENDING"
		}
		if err := p.store.SetPartnerSourceState(name, defaultAccountID, state); err != nil {
			return canonicalError(err)
		}
		return unitResponse()
	case "DescribeEventSource", "DescribePartnerEventSource":
		source, err := p.store.GetPartnerSource(name, defaultAccountID)
		if err != nil {
			return canonicalError(err)
		}
		return jsonResp(200, sourceResponse(*source, action == "DescribePartnerEventSource"))
	}
	return nil, plugin.ErrUnhandledOp
}

func (p *Provider) enqueuePartnerEvent(busName string, entry map[string]any) error {
	rules, err := p.store.MatchingRules(busName, defaultAccountID, entry)
	if err != nil {
		return err
	}
	targets := []Target{}
	for _, rule := range rules {
		found, err := p.store.ListTargetsByRule(rule.Name, busName, defaultAccountID)
		if err != nil {
			return err
		}
		targets = append(targets, found...)
	}
	if len(targets) > 0 && p.serverPort <= 0 {
		return errors.New("local delivery endpoint is unavailable")
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	for _, target := range targets {
		go p.dispatchToTarget(target.ARN, payload)
	}
	return nil
}

func (p *Provider) putPartnerEvents(params map[string]any) (*plugin.Response, error) {
	entries, ok := params["Entries"].([]any)
	if !ok || len(entries) < 1 || len(entries) > 20 {
		return ebError("InvalidParameterException", "Entries must contain between 1 and 20 items", 400), nil
	}
	results := make([]map[string]any, 0, len(entries))
	failed := 0
	for _, value := range entries {
		code, message := "", ""
		entry, ok := value.(map[string]any)
		if !ok {
			code = "InvalidArgument"
			message = "entry must be an object"
		} else {
			sourceName, _ := entry["Source"].(string)
			detailType, _ := entry["DetailType"].(string)
			detail, _ := entry["Detail"].(string)
			if sourceName == "" || detailType == "" || detail == "" {
				code = "InvalidArgument"
				message = "Source, DetailType and Detail are required"
			} else if !json.Valid([]byte(detail)) {
				code = "MalformedDetail"
				message = "Detail must be valid JSON"
			} else {
				source, err := p.store.GetPartnerSource(sourceName, defaultAccountID)
				switch {
				case errors.Is(err, ErrSourceNotFound):
					code = "ResourceNotFoundException"
					message = "partner event source not found"
				case err != nil:
					code = "InternalFailure"
					message = "partner source lookup failed"
				case source.State != "ACTIVE" || source.BusName == "":
					code = "InvalidStateException"
					message = "partner event source is not ACTIVE and bound"
				default:
					if err := p.enqueuePartnerEvent(source.BusName, entry); err != nil {
						code = "InternalFailure"
						message = "partner event could not be admitted"
					}
				}
			}
		}
		if code != "" {
			failed++
			results = append(results, map[string]any{"ErrorCode": code, "ErrorMessage": message})
		} else {
			results = append(results, map[string]any{"EventId": randomID(16)})
		}
	}
	return jsonResp(200, map[string]any{"FailedEntryCount": failed, "Entries": results})
}
