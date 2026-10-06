// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func indexedForms(form url.Values, prefix string, max int) ([]url.Values, error) {
	indexed := map[int]url.Values{}
	highest := 0
	for k, v := range form {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(k, prefix)
		index, field, _ := strings.Cut(suffix, ".")
		n, e := strconv.Atoi(index)
		if e != nil || n < 1 || n > max || strconv.Itoa(n) != index || len(v) != 1 {
			return nil, invalidSNS("invalid collection index or repeated field")
		}
		if indexed[n] == nil {
			indexed[n] = url.Values{}
		}
		indexed[n][field] = v
		if n > highest {
			highest = n
		}
	}
	if highest != len(indexed) {
		return nil, invalidSNS("collection indexes must be contiguous")
	}
	result := make([]url.Values, highest)
	for n := 1; n <= highest; n++ {
		result[n-1] = indexed[n]
	}
	return result, nil
}

func queryEntries(form url.Values, prefix string, max int) ([]url.Values, error) {
	return indexedForms(form, prefix+".member.", max)
}
func queryStringMap(form url.Values, prefix string) (map[string]string, error) {
	entries, e := indexedForms(form, prefix+".entry.", 10000)
	if e != nil {
		return nil, e
	}
	m := map[string]string{}
	for _, entry := range entries {
		k := entry.Get("key")
		_, hasValue := entry["value"]
		if k == "" || !hasValue || len(entry) != 2 {
			return nil, invalidSNS("map requires key and value")
		}
		if _, ok := m[k]; ok {
			return nil, invalidSNS("duplicate map key")
		}
		m[k] = entry.Get("value")
	}
	return m, nil
}
func queryStringList(form url.Values, prefix string) ([]string, error) {
	entries, e := indexedForms(form, prefix+".member.", 10000)
	if e != nil {
		return nil, e
	}
	v := make([]string, 0, len(entries))
	for _, entry := range entries {
		if len(entry) != 1 || entry.Get("") == "" {
			return nil, invalidSNS("invalid list member")
		}
		v = append(v, entry.Get(""))
	}
	return v, nil
}

type snsPageToken struct {
	Version             int
	Kind, Context, Last string
}

func pageSNS[T any](kind string, filters map[string]string, items []T, key func(T) string, limit int, token string) ([]T, string, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 100 {
		return nil, "", invalidSNS("MaxResults must be 1–100")
	}
	context, _ := json.Marshal(filters)
	last := ""
	if token != "" {
		raw, e := base64.RawURLEncoding.DecodeString(token)
		var t snsPageToken
		if e != nil || json.Unmarshal(raw, &t) != nil || t.Version != 1 || t.Kind != kind || t.Context != string(context) || t.Last == "" {
			return nil, "", invalidSNS("invalid NextToken")
		}
		last = t.Last
	}
	sorted := append([]T(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return key(sorted[i]) < key(sorted[j]) })
	start := sort.Search(len(sorted), func(i int) bool { return key(sorted[i]) > last })
	end := start + limit
	if end > len(sorted) {
		end = len(sorted)
	}
	next := ""
	if end < len(sorted) {
		raw, _ := json.Marshal(snsPageToken{1, kind, string(context), key(sorted[end-1])})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	return sorted[start:end], next, nil
}

func snsPageLimit(req *http.Request) (int, error) {
	if _, ok := req.Form["MaxResults"]; !ok {
		return 100, nil
	}
	n, e := strconv.Atoi(req.Form.Get("MaxResults"))
	if e != nil || n < 1 || n > 100 {
		return 0, invalidSNS("MaxResults must be 1–100")
	}
	return n, nil
}

type queryAttribute struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

func queryAttributes(m map[string]string) []queryAttribute {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	a := make([]queryAttribute, 0, len(keys))
	for _, k := range keys {
		a = append(a, queryAttribute{k, m[k]})
	}
	return a
}

func snsXML(action string, result any) (*plugin.Response, error) {
	var b bytes.Buffer
	enc := xml.NewEncoder(&b)
	root := xml.StartElement{Name: xml.Name{Local: action + "Response"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: "http://sns.amazonaws.com/doc/2010-03-31/"}}}
	if e := enc.EncodeToken(root); e != nil {
		return nil, e
	}
	if result != nil {
		if e := enc.EncodeElement(result, xml.StartElement{Name: xml.Name{Local: action + "Result"}}); e != nil {
			return nil, e
		}
	}
	metadata := struct {
		RequestID string `xml:"RequestId"`
	}{randomID(16)}
	if e := enc.EncodeElement(metadata, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}); e != nil {
		return nil, e
	}
	if e := enc.EncodeToken(root.End()); e != nil {
		return nil, e
	}
	if e := enc.Flush(); e != nil {
		return nil, e
	}
	return &plugin.Response{StatusCode: 200, ContentType: "text/xml", Body: b.Bytes()}, nil
}
func snsUnit(action string) (*plugin.Response, error) { return snsXML(action, struct{}{}) }

func canonicalSNSError(err error) (*plugin.Response, error) {
	code, status := "InternalError", 500
	switch {
	case errors.Is(err, ErrSNSInvalidParameter) || errors.Is(err, ErrSNSConflict):
		code, status = "InvalidParameter", 400
	case errors.Is(err, ErrMobileApplicationNotFound) || errors.Is(err, ErrMobileEndpointNotFound):
		code, status = "NotFound", 404
	case errors.Is(err, ErrSandboxPhoneNotFound):
		code, status = "ResourceNotFound", 404
	case errors.Is(err, ErrOTPVerification):
		code, status = "Verification", 400
	}
	message := err.Error()
	if status == 500 {
		message = "local SNS storage or delivery failed"
	}
	return snsError(code, message, status), nil
}
