// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

var statementIDPattern = regexp.MustCompile(`^[.\-_A-Za-z0-9]{1,64}$`)
var principalAccountPattern = regexp.MustCompile(`^[0-9]{12}$`)

func permissionBusName(params map[string]any) (string, error) {
	value, ok := params["EventBusName"]
	if !ok {
		return "default", nil
	}
	name, ok := value.(string)
	if !ok || name == "" {
		return "", errInvalidParameter
	}
	if strings.HasPrefix(name, "arn:") {
		parts := strings.SplitN(name, ":", 6)
		if len(parts) != 6 || parts[1] != "aws" || parts[2] != "events" || parts[4] != defaultAccountID || !strings.HasPrefix(parts[5], "event-bus/") {
			return "", errInvalidParameter
		}
		name = strings.TrimPrefix(parts[5], "event-bus/")
	}
	if name == "" {
		return "", errInvalidParameter
	}
	return name, nil
}

func policyStatements(policy map[string]any) ([]any, error) {
	var statements []any
	switch value := policy["Statement"].(type) {
	case map[string]any:
		statements = []any{value}
	case []any:
		statements = value
	default:
		return nil, errInvalidParameter
	}
	if len(statements) == 0 {
		return nil, errInvalidParameter
	}
	seen := map[string]bool{}
	for _, value := range statements {
		statement, ok := value.(map[string]any)
		if !ok {
			return nil, errInvalidParameter
		}
		sid, _ := statement["Sid"].(string)
		effect, _ := statement["Effect"].(string)
		if !statementIDPattern.MatchString(sid) || seen[sid] || (effect != "Allow" && effect != "Deny") {
			return nil, errInvalidParameter
		}
		seen[sid] = true
		switch action := statement["Action"].(type) {
		case string:
			if action == "" {
				return nil, errInvalidParameter
			}
		case []any:
			if len(action) == 0 {
				return nil, errInvalidParameter
			}
			for _, item := range action {
				text, ok := item.(string)
				if !ok || text == "" {
					return nil, errInvalidParameter
				}
			}
		default:
			return nil, errInvalidParameter
		}
		switch principal := statement["Principal"].(type) {
		case string:
			if principal == "" {
				return nil, errInvalidParameter
			}
		case map[string]any:
			if len(principal) == 0 {
				return nil, errInvalidParameter
			}
		default:
			return nil, errInvalidParameter
		}
	}
	return statements, nil
}

func permissionChange(params map[string]any) (func(map[string]any) (map[string]any, error), error) {
	bus, err := permissionBusName(params)
	if err != nil {
		return nil, err
	}
	document := map[string]any{}
	if value, provided := params["Policy"]; provided {
		for _, field := range []string{"StatementId", "Action", "Principal", "Condition"} {
			if _, exists := params[field]; exists {
				return nil, errInvalidParameter
			}
		}
		text, ok := value.(string)
		if !ok || len(text) > 20480 || json.Unmarshal([]byte(text), &document) != nil || document == nil {
			return nil, errInvalidParameter
		}
	} else {
		sid, _ := params["StatementId"].(string)
		action, _ := params["Action"].(string)
		principal, _ := params["Principal"].(string)
		if !statementIDPattern.MatchString(sid) || action != "events:PutEvents" || (principal != "*" && !principalAccountPattern.MatchString(principal)) {
			return nil, errInvalidParameter
		}
		statement := map[string]any{"Sid": sid, "Effect": "Allow", "Action": action, "Principal": map[string]any{"AWS": principal}, "Resource": busARN(bus, defaultAccountID)}
		if value, provided := params["Condition"]; provided {
			condition, ok := value.(map[string]any)
			if !ok {
				return nil, errInvalidParameter
			}
			kind, _ := condition["Type"].(string)
			key, _ := condition["Key"].(string)
			value, _ := condition["Value"].(string)
			if kind == "" || key == "" || value == "" {
				return nil, errInvalidParameter
			}
			statement["Condition"] = map[string]any{kind: map[string]any{key: value}}
		}
		document["Statement"] = []any{statement}
	}
	incoming, err := policyStatements(document)
	if err != nil {
		return nil, err
	}
	return func(current map[string]any) (map[string]any, error) {
		if current == nil {
			current = map[string]any{"Version": "2012-10-17", "Statement": []any{}}
		}
		old, ok := current["Statement"].([]any)
		if !ok {
			return nil, errors.New("stored policy has an invalid statement array")
		}
		result := append([]any{}, old...)
		for _, value := range incoming {
			statement := value.(map[string]any)
			replaced := false
			for i, previous := range result {
				row, ok := previous.(map[string]any)
				if !ok {
					return nil, errors.New("stored policy has an invalid statement")
				}
				if row["Sid"] == statement["Sid"] {
					result[i] = statement
					replaced = true
					break
				}
			}
			if !replaced {
				result = append(result, statement)
			}
		}
		for key, value := range document {
			if key != "Statement" {
				current[key] = value
			}
		}
		current["Statement"] = result
		return current, nil
	}, nil
}

func (p *Provider) handlePermissionOperation(action string, params map[string]any) (*plugin.Response, error) {
	bus, err := permissionBusName(params)
	if err != nil {
		return ebError("InvalidParameterException", "invalid EventBusName", 400), nil
	}
	var change func(map[string]any) (map[string]any, error)
	switch action {
	case "PutPermission":
		change, err = permissionChange(params)
		if err != nil {
			return ebError("InvalidParameterException", "invalid permission parameters", 400), nil
		}
	case "RemovePermission":
		sidValue, sidProvided := params["StatementId"]
		sid, validSid := sidValue.(string)
		all := false
		if value, provided := params["RemoveAllPermissions"]; provided {
			var valid bool
			all, valid = value.(bool)
			if !valid {
				return ebError("InvalidParameterException", "RemoveAllPermissions must be boolean", 400), nil
			}
		}
		if (all && sidProvided) || (!all && (!sidProvided || !validSid || !statementIDPattern.MatchString(sid))) {
			return ebError("InvalidParameterException", "supply one StatementId or RemoveAllPermissions=true", 400), nil
		}
		change = func(policy map[string]any) (map[string]any, error) {
			if all {
				return nil, nil
			}
			if policy == nil {
				return nil, ErrPermissionNotFound
			}
			statements, ok := policy["Statement"].([]any)
			if !ok {
				return nil, errors.New("stored policy has an invalid statement array")
			}
			kept := make([]any, 0, len(statements))
			found := false
			for _, value := range statements {
				statement, ok := value.(map[string]any)
				if !ok {
					return nil, errors.New("stored policy has an invalid statement")
				}
				if statement["Sid"] == sid {
					found = true
				} else {
					kept = append(kept, statement)
				}
			}
			if !found {
				return nil, ErrPermissionNotFound
			}
			if len(kept) == 0 {
				return nil, nil
			}
			policy["Statement"] = kept
			return policy, nil
		}
	default:
		return nil, plugin.ErrUnhandledOp
	}
	if err := p.store.UpdateBusPolicy(bus, defaultAccountID, change); err != nil {
		return canonicalError(err)
	}
	return unitResponse()
}
