// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"encoding/json"
	"encoding/xml"

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
