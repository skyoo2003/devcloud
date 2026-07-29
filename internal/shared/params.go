// SPDX-License-Identifier: Apache-2.0

package shared

// StrParam returns params[key] as a string, or "" when the key is absent or
// holds another type. Providers parse request bodies into map[string]any, so
// this is the standard accessor for a string field.
func StrParam(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}

// StrParamDefault returns params[key] as a string, or def when the key is
// absent, holds another type, or holds the empty string.
func StrParamDefault(params map[string]any, key, def string) string {
	if s, _ := params[key].(string); s != "" {
		return s
	}
	return def
}
