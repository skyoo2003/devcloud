// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/auth"
	generated "github.com/skyoo2003/devcloud/internal/generated/blob"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Provider implements the Azure Blob Storage service.
type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *FileStore
}

func (p *Provider) ServiceID() string             { return "blob" }
func (p *Provider) ServiceName() string           { return "Azure Blob Storage" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTXML }
func (p *Provider) Provider() string              { return "azure" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/blob"
	}
	p.store = NewFileStore(p.dataDir)
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func randomRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (p *Provider) reply(req *http.Request, status int, headers map[string]string, body []byte) *plugin.Response {
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["x-ms-request-id"] = randomRequestID()
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version
	headers["Date"] = time.Now().UTC().Format(http.TimeFormat)

	ct := headers["Content-Type"]
	if ct == "" {
		ct = "application/octet-stream"
	}
	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        body,
		ContentType: ct,
	}
}

func (p *Provider) errorReply(req *http.Request, status int, code, message string) *plugin.Response {
	headers := map[string]string{
		"x-ms-error-code": code,
		"Content-Type":    "application/xml",
		"x-ms-request-id": randomRequestID(),
		"Date":            time.Now().UTC().Format(http.TimeFormat),
	}
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version

	body := fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Error><Code>%s</Code><Message>%s\nRequestId:%s\nTime:%s</Message></Error>",
		code, message, headers["x-ms-request-id"], time.Now().UTC().Format(time.RFC3339Nano))

	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        []byte(body),
		ContentType: "application/xml",
	}
}

func (p *Provider) HandleRequest(ctx context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	account := "devstoreaccount1"
	if id, ok := auth.FromContext(ctx); ok && id.AccessKeyID != "" {
		account = id.AccessKeyID
	}

	// Normalize URI: strip leading account name prefix if present
	uri := req.URL.RequestURI()
	for _, accPrefix := range []string{"/" + account, "/devstoreaccount1"} {
		if strings.HasPrefix(uri, accPrefix+"/") {
			uri = uri[len(accPrefix):]
			break
		} else if strings.HasPrefix(uri, accPrefix+"?") {
			uri = "/" + uri[len(accPrefix):]
			break
		} else if uri == accPrefix {
			uri = "/"
			break
		}
	}

	op, params := generated.MatchOperation(req.Method, uri)
	if op == "" {
		return nil, plugin.ErrUnhandledOp
	}

	switch op {
	case "CreateContainer":
		container := params["container"]
		info, err := p.store.CreateContainer(account, container)
		if errors.Is(err, ErrContainerAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "ContainerAlreadyExists", "The specified container already exists."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"ETag":          info.ETag,
			"Last-Modified": info.CreatedAt.Format(http.TimeFormat),
		}
		return p.reply(req, http.StatusCreated, headers, nil), nil

	case "DeleteContainer":
		container := params["container"]
		err := p.store.DeleteContainer(account, container)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		return p.reply(req, http.StatusAccepted, nil, nil), nil

	case "GetContainerProperties":
		container := params["container"]
		info, err := p.store.GetContainer(account, container)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"ETag":                                info.ETag,
			"Last-Modified":                       info.CreatedAt.Format(http.TimeFormat),
			"x-ms-lease-status":                   "unlocked",
			"x-ms-lease-state":                    "available",
			"x-ms-has-immutability-policy":        "false",
			"x-ms-has-legal-hold":                 "false",
			"x-ms-default-encryption-scope":       "$account-encryption-key",
			"x-ms-deny-encryption-scope-override": "false",
		}
		return p.reply(req, http.StatusOK, headers, nil), nil

	case "ListContainers":
		containers, err := p.store.ListContainers(account)
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		res := XMLContainerEnumerationResults{
			ServiceEndpoint: fmt.Sprintf("http://%s/%s/", req.Host, account),
			Containers: XMLContainersWrapper{
				Container: make([]XMLContainerItem, 0, len(containers)),
			},
		}
		for _, c := range containers {
			res.Containers.Container = append(res.Containers.Container, XMLContainerItem{
				Name: c.Name,
				Properties: XMLContainerProperties{
					LastModified:                c.CreatedAt.Format(http.TimeFormat),
					Etag:                        c.ETag,
					LeaseStatus:                 "unlocked",
					LeaseState:                  "available",
					DefaultEncryptionScope:      "$account-encryption-key",
					DenyEncryptionScopeOverride: "false",
					HasImmutabilityPolicy:       "false",
					HasLegalHold:                "false",
				},
			})
		}
		xmlBytes, err := xml.MarshalIndent(res, "", "  ")
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		body := append([]byte(xml.Header), xmlBytes...)
		headers := map[string]string{
			"Content-Type": "application/xml",
		}
		return p.reply(req, http.StatusOK, headers, body), nil

	case "PutBlob":
		container := params["container"]
		blob := params["blob"]
		contentType := req.Header.Get("Content-Type")
		info, err := p.store.PutBlob(account, container, blob, contentType, req.Body)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"ETag":                          info.ETag,
			"Last-Modified":                 info.LastModified.Format(http.TimeFormat),
			"Content-MD5":                   info.MD5,
			"x-ms-request-server-encrypted": "true",
		}
		return p.reply(req, http.StatusCreated, headers, nil), nil

	case "GetBlob":
		container := params["container"]
		blob := params["blob"]
		info, data, err := p.store.GetBlob(account, container, blob)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if errors.Is(err, ErrBlobNotFound) {
			return p.errorReply(req, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"Content-Type":          info.ContentType,
			"ETag":                  info.ETag,
			"Last-Modified":         info.LastModified.Format(http.TimeFormat),
			"Content-MD5":           info.MD5,
			"x-ms-blob-type":        "BlockBlob",
			"x-ms-lease-status":     "unlocked",
			"x-ms-lease-state":      "available",
			"x-ms-server-encrypted": "true",
			"x-ms-access-tier":      "Hot",
			"x-ms-creation-time":    info.CreatedAt.Format(http.TimeFormat),
			"Accept-Ranges":         "bytes",
		}

		rangeHeader := req.Header.Get("x-ms-range")
		if rangeHeader == "" {
			rangeHeader = req.Header.Get("Range")
		}

		if rangeHeader != "" && info.Size > 0 {
			start := int64(0)
			end := info.Size - 1
			trimmed := strings.TrimPrefix(rangeHeader, "bytes=")
			parts := strings.Split(trimmed, "-")
			if len(parts) >= 1 && parts[0] != "" {
				_, _ = fmt.Sscanf(parts[0], "%d", &start)
			}
			if len(parts) >= 2 && parts[1] != "" {
				_, _ = fmt.Sscanf(parts[1], "%d", &end)
			}
			if start < 0 {
				start = 0
			}
			if end >= info.Size {
				end = info.Size - 1
			}
			if start > end {
				start = end
			}
			slicedData := data[start : end+1]
			headers["Content-Range"] = fmt.Sprintf("bytes %d-%d/%d", start, end, info.Size)
			headers["Content-Length"] = fmt.Sprintf("%d", len(slicedData))
			return p.reply(req, http.StatusPartialContent, headers, slicedData), nil
		}

		headers["Content-Length"] = fmt.Sprintf("%d", info.Size)
		if info.Size > 0 {
			headers["Content-Range"] = fmt.Sprintf("bytes 0-%d/%d", info.Size-1, info.Size)
		} else {
			headers["Content-Range"] = "bytes */0"
		}
		return p.reply(req, http.StatusOK, headers, data), nil

	case "GetBlobProperties":
		container := params["container"]
		blob := params["blob"]
		info, err := p.store.GetBlobProperties(account, container, blob)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if errors.Is(err, ErrBlobNotFound) {
			return p.errorReply(req, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"Content-Type":          info.ContentType,
			"Content-Length":        fmt.Sprintf("%d", info.Size),
			"ETag":                  info.ETag,
			"Last-Modified":         info.LastModified.Format(http.TimeFormat),
			"Content-MD5":           info.MD5,
			"x-ms-blob-type":        "BlockBlob",
			"x-ms-lease-status":     "unlocked",
			"x-ms-lease-state":      "available",
			"x-ms-server-encrypted": "true",
			"x-ms-access-tier":      "Hot",
			"x-ms-creation-time":    info.CreatedAt.Format(http.TimeFormat),
		}
		return p.reply(req, http.StatusOK, headers, nil), nil

	case "DeleteBlob":
		container := params["container"]
		blob := params["blob"]
		err := p.store.DeleteBlob(account, container, blob)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if errors.Is(err, ErrBlobNotFound) {
			return p.errorReply(req, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		headers := map[string]string{
			"x-ms-delete-type-permanent": "true",
		}
		return p.reply(req, http.StatusAccepted, headers, nil), nil

	case "ListBlobs":
		container := params["container"]
		blobs, err := p.store.ListBlobs(account, container)
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist."), nil
		}
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		res := XMLBlobEnumerationResults{
			ServiceEndpoint: fmt.Sprintf("http://%s/%s/", req.Host, account),
			ContainerName:   container,
			Blobs: XMLBlobsWrapper{
				Blob: make([]XMLBlobItem, 0, len(blobs)),
			},
		}
		for _, b := range blobs {
			res.Blobs.Blob = append(res.Blobs.Blob, XMLBlobItem{
				Name: b.Name,
				Properties: XMLBlobProperties{
					CreationTime:       b.CreatedAt.Format(http.TimeFormat),
					LastModified:       b.LastModified.Format(http.TimeFormat),
					Etag:               b.ETag,
					ContentLength:      b.Size,
					ContentType:        b.ContentType,
					ContentMD5:         b.MD5,
					BlobType:           "BlockBlob",
					AccessTier:         "Hot",
					AccessTierInferred: "true",
					LeaseStatus:        "unlocked",
					LeaseState:         "available",
					ServerEncrypted:    "true",
				},
			})
		}
		xmlBytes, err := xml.MarshalIndent(res, "", "  ")
		if err != nil {
			return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
		}
		body := append([]byte(xml.Header), xmlBytes...)
		headers := map[string]string{
			"Content-Type": "application/xml",
		}
		return p.reply(req, http.StatusOK, headers, body), nil
	}

	return nil, plugin.ErrUnhandledOp
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	account := "devstoreaccount1"
	containers, err := p.store.ListContainers(account)
	if err != nil {
		return []plugin.Resource{}, nil
	}
	res := make([]plugin.Resource, 0, len(containers))
	for _, c := range containers {
		res = append(res, plugin.Resource{
			Type: "container",
			ID:   c.Name,
			Name: c.Name,
		})
	}
	return res, nil
}

type XMLContainerEnumerationResults struct {
	XMLName         xml.Name             `xml:"EnumerationResults"`
	ServiceEndpoint string               `xml:"ServiceEndpoint,attr"`
	Containers      XMLContainersWrapper `xml:"Containers"`
	NextMarker      string               `xml:"NextMarker"`
}

type XMLContainersWrapper struct {
	Container []XMLContainerItem `xml:"Container"`
}

type XMLContainerItem struct {
	Name       string                 `xml:"Name"`
	Properties XMLContainerProperties `xml:"Properties"`
}

type XMLContainerProperties struct {
	LastModified                string `xml:"Last-Modified"`
	Etag                        string `xml:"Etag"`
	LeaseStatus                 string `xml:"LeaseStatus"`
	LeaseState                  string `xml:"LeaseState"`
	DefaultEncryptionScope      string `xml:"DefaultEncryptionScope"`
	DenyEncryptionScopeOverride string `xml:"DenyEncryptionScopeOverride"`
	HasImmutabilityPolicy       string `xml:"HasImmutabilityPolicy"`
	HasLegalHold                string `xml:"HasLegalHold"`
}

type XMLBlobEnumerationResults struct {
	XMLName         xml.Name        `xml:"EnumerationResults"`
	ServiceEndpoint string          `xml:"ServiceEndpoint,attr"`
	ContainerName   string          `xml:"ContainerName,attr"`
	Blobs           XMLBlobsWrapper `xml:"Blobs"`
	NextMarker      string          `xml:"NextMarker"`
}

type XMLBlobsWrapper struct {
	Blob []XMLBlobItem `xml:"Blob"`
}

type XMLBlobItem struct {
	Name       string            `xml:"Name"`
	Properties XMLBlobProperties `xml:"Properties"`
}

type XMLBlobProperties struct {
	CreationTime       string `xml:"Creation-Time"`
	LastModified       string `xml:"Last-Modified"`
	Etag               string `xml:"Etag"`
	ContentLength      int64  `xml:"Content-Length"`
	ContentType        string `xml:"Content-Type"`
	ContentEncoding    string `xml:"Content-Encoding"`
	ContentLanguage    string `xml:"Content-Language"`
	ContentCRC64       string `xml:"Content-CRC64"`
	ContentMD5         string `xml:"Content-MD5"`
	CacheControl       string `xml:"Cache-Control"`
	ContentDisposition string `xml:"Content-Disposition"`
	BlobType           string `xml:"BlobType"`
	AccessTier         string `xml:"AccessTier"`
	AccessTierInferred string `xml:"AccessTierInferred"`
	LeaseStatus        string `xml:"LeaseStatus"`
	LeaseState         string `xml:"LeaseState"`
	ServerEncrypted    string `xml:"ServerEncrypted"`
}

func init() {
	plugin.DefaultRegistry.Register("blob", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
