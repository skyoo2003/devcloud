// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"encoding/json"
	"encoding/xml"
	"strings"
	"unicode"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func JSONResponse(status int, v any) (*plugin.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.1"}, nil
}

func XMLResponse(status int, v any) (*plugin.Response, error) {
	b, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "text/xml"}, nil
}

func JSONError(code, message string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.1"}
}

type queryXMLErrorResponse struct {
	XMLName xml.Name `xml:"Response"`
	Errors  struct {
		Error struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	} `xml:"Errors"`
}

func QueryXMLError(code, message string, status int) *plugin.Response {
	e := queryXMLErrorResponse{}
	e.Errors.Error.Code = code
	e.Errors.Error.Message = message
	b, _ := xml.Marshal(e)
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "text/xml"}
}

type restXMLErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func RESTXMLError(code, message string, status int) *plugin.Response {
	b, _ := xml.Marshal(restXMLErrorResponse{Code: code, Message: message})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/xml"}
}

// ToCamelCase converts a PascalCase string to camelCase.
// E.g. "ClusterArn" -> "clusterArn", "ARN" -> "arn", "HTTPStatusCode" -> "httpStatusCode".
func ToCamelCase(s string) string {
	if s == "" {
		return s
	}
	// Find the first lowercase letter or end of leading uppercase run
	runes := []rune(s)
	i := 0
	for i < len(runes) && unicode.IsUpper(runes[i]) {
		i++
	}
	if i == 0 {
		return s // already camelCase
	}
	if i == 1 {
		// Single uppercase letter at start
		return strings.ToLower(string(runes[0])) + string(runes[1:])
	}
	// Multiple uppercase: e.g. "ARN" -> "arn", "ARNPrefix" -> "arnPrefix"
	if i == len(runes) {
		return strings.ToLower(s)
	}
	return strings.ToLower(string(runes[:i-1])) + string(runes[i-1:])
}

// CamelCaseKeys recursively converts all map keys from PascalCase to camelCase.
func CamelCaseKeys(v any) any {
	switch val := v.(type) {
	case map[string]any:
		result := make(map[string]any, len(val))
		for k, v := range val {
			result[ToCamelCase(k)] = CamelCaseKeys(v)
		}
		return result
	case []any:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = CamelCaseKeys(item)
		}
		return result
	case []map[string]any:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = CamelCaseKeys(item)
		}
		return result
	default:
		return v
	}
}

// JSONResponseCamel is like JSONResponse but converts all PascalCase keys to camelCase.
// Use for REST-JSON services that use camelCase wire format (kafka, mq, mwaa, etc.).
func JSONResponseCamel(status int, v any) (*plugin.Response, error) {
	return JSONResponse(status, CamelCaseKeys(v))
}

// ToPascalCase converts a camelCase string to PascalCase.
func ToPascalCase(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

// PascalCaseKeys recursively converts all map keys from camelCase to PascalCase.
func PascalCaseKeys(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, val := range m {
		newKey := ToPascalCase(k)
		switch typedVal := val.(type) {
		case map[string]any:
			result[newKey] = PascalCaseKeys(typedVal)
		case []any:
			newSlice := make([]any, len(typedVal))
			for i, item := range typedVal {
				if subMap, ok := item.(map[string]any); ok {
					newSlice[i] = PascalCaseKeys(subMap)
				} else {
					newSlice[i] = item
				}
			}
			result[newKey] = newSlice
		default:
			result[newKey] = val
		}
	}
	return result
}

// AWSError returns a protocol-appropriate error response.
// protocol should be one of: "json-1.0", "json-1.1", "rest-json", "query", "rest-xml".
func AWSError(protocol string, code, message string, status int) *plugin.Response {
	switch protocol {
	case "query":
		return QueryXMLError(code, message, status)
	case "rest-xml":
		return RESTXMLError(code, message, status)
	default:
		return JSONError(code, message, status)
	}
}
