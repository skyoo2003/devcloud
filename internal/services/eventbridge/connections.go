// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

var errInvalidParameter = errors.New("invalid parameter")

func validateConnectionAuth(kind string, auth map[string]any) error {
	invalid := errInvalidParameter
	if auth == nil {
		return invalid
	}
	text := func(m map[string]any, key string) bool { value, ok := m[key].(string); return ok && value != "" }
	switch kind {
	case "API_KEY":
		m, ok := auth["ApiKeyAuthParameters"].(map[string]any)
		if !ok || !text(m, "ApiKeyName") || !text(m, "ApiKeyValue") {
			return invalid
		}
	case "BASIC":
		m, ok := auth["BasicAuthParameters"].(map[string]any)
		if !ok || !text(m, "Username") || !text(m, "Password") {
			return invalid
		}
	case "OAUTH_CLIENT_CREDENTIALS":
		m, ok := auth["OAuthParameters"].(map[string]any)
		if !ok {
			return invalid
		}
		client, ok := m["ClientParameters"].(map[string]any)
		if !ok || !text(client, "ClientID") || !text(client, "ClientSecret") {
			return invalid
		}
		endpoint, _ := m["AuthorizationEndpoint"].(string)
		parsed, err := url.Parse(endpoint)
		method, _ := m["HttpMethod"].(string)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || (method != "GET" && method != "POST" && method != "PUT") {
			return invalid
		}
	default:
		return invalid
	}
	return nil
}

// redactAuth returns a detached view and removes secret HTTP parameter values
// as well as the credentials that AWS never returns through DescribeConnection.
func redactAuth(value any) any {
	switch value := value.(type) {
	case map[string]any:
		result := map[string]any{}
		secret, _ := value["IsValueSecret"].(bool)
		for key, item := range value {
			if key == "Password" || key == "ApiKeyValue" || key == "ClientSecret" || (secret && key == "Value") {
				continue
			}
			result[key] = redactAuth(item)
		}
		return result
	case []any:
		result := make([]any, 0, len(value))
		for _, item := range value {
			result = append(result, redactAuth(item))
		}
		return result
	default:
		return value
	}
}

func connectionResponse(c *Connection, describe bool) map[string]any {
	result := map[string]any{"ConnectionArn": c.ARN, "ConnectionState": c.State, "CreationTime": float64(c.CreationTime.UnixNano()) / 1e9, "LastModifiedTime": float64(c.LastModifiedTime.UnixNano()) / 1e9}
	if c.LastAuthorizedTime != nil {
		result["LastAuthorizedTime"] = float64(c.LastAuthorizedTime.UnixNano()) / 1e9
	}
	if describe {
		result["Name"] = c.Name
		result["Description"] = c.Description
		result["AuthorizationType"] = c.AuthorizationType
		if len(c.AuthParameters) > 0 {
			result["AuthParameters"] = redactAuth(c.AuthParameters)
		}
		for _, key := range []string{"KmsKeyIdentifier", "InvocationConnectivityParameters"} {
			if value, ok := c.Metadata[key]; ok {
				result[key] = value
			}
		}
	}
	return result
}

func applyConnectionInput(c *Connection, params map[string]any, creating bool) error {
	if value, ok := params["Description"]; ok {
		description, valid := value.(string)
		if !valid || len(description) > 512 {
			return errInvalidParameter
		}
		c.Description = description
	}
	kindChanged := false
	if value, ok := params["AuthorizationType"]; ok {
		kind, valid := value.(string)
		if !valid || (kind != "API_KEY" && kind != "BASIC" && kind != "OAUTH_CLIENT_CREDENTIALS") {
			return errInvalidParameter
		}
		kindChanged = kind != c.AuthorizationType
		c.AuthorizationType = kind
	} else if creating {
		return errInvalidParameter
	}
	authSupplied := false
	credentialsSupplied := false
	if value, ok := params["AuthParameters"]; ok {
		auth, valid := value.(map[string]any)
		if !valid || len(auth) == 0 {
			return errInvalidParameter
		}
		credentialKey := map[string]string{"API_KEY": "ApiKeyAuthParameters", "BASIC": "BasicAuthParameters", "OAUTH_CLIENT_CREDENTIALS": "OAuthParameters"}[c.AuthorizationType]
		if value, provided := auth[credentialKey]; provided {
			credential, valid := value.(map[string]any)
			if !valid || len(credential) == 0 {
				return errInvalidParameter
			}
			credentialsSupplied = true
		}
		if creating || kindChanged {
			c.AuthParameters = mergeAuthParameters(nil, auth)
		} else {
			c.AuthParameters = mergeAuthParameters(c.AuthParameters, auth)
		}
		authSupplied = true
	} else if creating {
		return errInvalidParameter
	}
	if kindChanged && !credentialsSupplied {
		return errInvalidParameter
	}
	if creating || credentialsSupplied || kindChanged {
		if err := validateConnectionAuth(c.AuthorizationType, c.AuthParameters); err != nil {
			return err
		}
		c.State = "DEAUTHORIZED"
		if c.AuthorizationType != "OAUTH_CLIENT_CREDENTIALS" {
			c.State = "AUTHORIZED"
			now := time.Now().UTC()
			c.LastAuthorizedTime = &now
		}
	}
	if authSupplied && !credentialsSupplied && c.State == "AUTHORIZED" {
		if err := validateConnectionAuth(c.AuthorizationType, c.AuthParameters); err != nil {
			return err
		}
	}
	if c.Metadata == nil {
		c.Metadata = map[string]any{}
	}
	for _, key := range []string{"KmsKeyIdentifier", "InvocationConnectivityParameters"} {
		if value, ok := params[key]; ok {
			if key == "KmsKeyIdentifier" {
				if _, ok := value.(string); !ok {
					return errInvalidParameter
				}
			} else if _, ok := value.(map[string]any); !ok {
				return errInvalidParameter
			}
			c.Metadata[key] = value
		}
	}
	return nil
}

// Structured patches retain fields omitted by UpdateConnection. Lists are
// replaced as collections; credential checks run against the effective state.
func mergeAuthParameters(existing, patch map[string]any) map[string]any {
	result := map[string]any{}
	for key, value := range existing {
		result[key] = value
	}
	for key, value := range patch {
		if nested, ok := value.(map[string]any); ok {
			previous, _ := existing[key].(map[string]any)
			result[key] = mergeAuthParameters(previous, nested)
		} else {
			result[key] = value
		}
	}
	return result
}

func (p *Provider) handleConnectionOperation(action string, params map[string]any) (*plugin.Response, error) {
	if action == "ListConnections" {
		limit, token, err := listParameters(params)
		if err != nil {
			return ebError("InvalidParameterException", err.Error(), 400), nil
		}
		prefix, state := "", ""
		for _, key := range []string{"NamePrefix", "ConnectionState"} {
			if value, ok := params[key]; ok {
				text, valid := value.(string)
				if !valid {
					return ebError("InvalidParameterException", "list filter must be a string", 400), nil
				}
				if key == "NamePrefix" {
					prefix = text
				} else {
					state = text
				}
			}
		}
		all, err := p.store.ListConnections(defaultAccountID)
		if err != nil {
			return canonicalError(err)
		}
		filtered := []Connection{}
		for _, c := range all {
			if strings.HasPrefix(c.Name, prefix) && (state == "" || c.State == state) {
				filtered = append(filtered, c)
			}
		}
		page, next, err := pageItems(action, map[string]string{"NamePrefix": prefix, "ConnectionState": state, "Account": defaultAccountID}, filtered, func(c Connection) string { return c.Name }, limit, token)
		if err != nil {
			return ebError("InvalidParameterException", err.Error(), 400), nil
		}
		views := []map[string]any{}
		for _, c := range page {
			view := connectionResponse(&c, false)
			view["Name"] = c.Name
			view["AuthorizationType"] = c.AuthorizationType
			views = append(views, view)
		}
		result := map[string]any{"Connections": views}
		if next != "" {
			result["NextToken"] = next
		}
		return jsonResp(200, result)
	}
	name, _ := params["Name"].(string)
	if !connectionNamePattern.MatchString(name) {
		return ebError("InvalidParameterException", "valid connection Name is required", 400), nil
	}
	var c *Connection
	var err error
	switch action {
	case "CreateConnection":
		now := time.Now().UTC()
		c = &Connection{Name: name, AccountID: defaultAccountID, ARN: "arn:aws:events:us-east-1:" + defaultAccountID + ":connection/" + name + "/" + randomID(16), CreationTime: now, LastModifiedTime: now}
		if err = applyConnectionInput(c, params, true); err == nil {
			err = p.store.CreateConnection(*c)
		}
	case "DescribeConnection":
		c, err = p.store.GetConnection(name, defaultAccountID)
	case "UpdateConnection":
		c, err = p.store.UpdateConnection(name, defaultAccountID, func(c *Connection) error {
			if err := applyConnectionInput(c, params, false); err != nil {
				return err
			}
			c.LastModifiedTime = time.Now().UTC()
			return nil
		})
	case "DeauthorizeConnection":
		c, err = p.store.UpdateConnection(name, defaultAccountID, func(c *Connection) error {
			c.AuthParameters = nil
			c.State = "DEAUTHORIZED"
			c.LastModifiedTime = time.Now().UTC()
			return nil
		})
	case "DeleteConnection":
		c, err = p.store.DeleteConnection(name, defaultAccountID)
		if c != nil {
			c.State = "DELETING"
			c.LastModifiedTime = time.Now().UTC()
		}
	default:
		return nil, plugin.ErrUnhandledOp
	}
	if err != nil {
		if errors.Is(err, errInvalidParameter) {
			return ebError("InvalidParameterException", "invalid connection parameters", 400), nil
		}
		return canonicalError(err)
	}
	// Encoding through JSON prevents response views from sharing stored nested maps.
	result := connectionResponse(c, action == "DescribeConnection")
	if _, err := json.Marshal(result); err != nil {
		return canonicalError(err)
	}
	return jsonResp(200, result)
}
