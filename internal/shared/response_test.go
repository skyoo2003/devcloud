// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"encoding/json"
	"encoding/xml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestJSONResponse(t *testing.T) {
	resp, err := JSONResponse(http.StatusOK, map[string]string{"key": "value"})
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.ContentType)
	var m map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	assert.Equal(t, "value", m["key"])
}

func TestXMLResponse(t *testing.T) {
	type item struct {
		XMLName xml.Name `xml:"Item"`
		Name    string   `xml:"Name"`
	}
	resp, err := XMLResponse(http.StatusOK, item{Name: "test"})
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "text/xml", resp.ContentType)
	assert.Contains(t, string(resp.Body), "<Name>test</Name>")
}

func TestJSONError(t *testing.T) {
	resp := JSONError("ValidationException", "bad input", http.StatusBadRequest)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.ContentType)
	var m map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	assert.Equal(t, "ValidationException", m["__type"])
	assert.Equal(t, "bad input", m["message"])
}

func TestQueryXMLError(t *testing.T) {
	resp := QueryXMLError("InvalidAction", "unknown", http.StatusBadRequest)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "text/xml", resp.ContentType)
	assert.Contains(t, string(resp.Body), "<Code>InvalidAction</Code>")
	assert.Contains(t, string(resp.Body), "<Message>unknown</Message>")
}

func TestRESTXMLError(t *testing.T) {
	resp := RESTXMLError("NoSuchBucket", "not found", http.StatusNotFound)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Equal(t, "application/xml", resp.ContentType)
	assert.Contains(t, string(resp.Body), "<Code>NoSuchBucket</Code>")
}

func TestAWSError_JSON11(t *testing.T) {
	resp := AWSError("json-1.1", "ThrottlingException", "too fast", http.StatusTooManyRequests)
	assert.Equal(t, 429, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.ContentType)
	var m map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	assert.Equal(t, "ThrottlingException", m["__type"])
}

func TestAWSError_JSON10(t *testing.T) {
	resp := AWSError("json-1.0", "ValidationException", "bad", http.StatusBadRequest)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.ContentType)
}

func TestAWSError_Query(t *testing.T) {
	resp := AWSError("query", "InvalidAction", "nope", http.StatusBadRequest)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "text/xml", resp.ContentType)
	assert.Contains(t, string(resp.Body), "<Code>InvalidAction</Code>")
}

func TestAWSError_RESTXML(t *testing.T) {
	resp := AWSError("rest-xml", "NoSuchKey", "missing", http.StatusNotFound)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Equal(t, "application/xml", resp.ContentType)
	assert.Contains(t, string(resp.Body), "<Code>NoSuchKey</Code>")
}

func TestAWSError_RESTJSON(t *testing.T) {
	resp := AWSError("rest-json", "NotFoundException", "gone", http.StatusNotFound)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.ContentType)
}
