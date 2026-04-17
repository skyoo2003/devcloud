// SPDX-License-Identifier: Apache-2.0

// internal/services/firehose/provider.go
package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "firehose" }
func (p *Provider) ServiceName() string           { return "Firehose_20150804" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "firehose"))
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

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	case "CreateDeliveryStream":
		return p.createDeliveryStream(params)
	case "DeleteDeliveryStream":
		return p.deleteDeliveryStream(params)
	case "DescribeDeliveryStream":
		return p.describeDeliveryStream(params)
	case "ListDeliveryStreams":
		return p.listDeliveryStreams(params)
	case "PutRecord":
		return p.putRecord(params)
	case "PutRecordBatch":
		return p.putRecordBatch(params)
	case "UpdateDestination":
		return p.updateDestination(params)
	case "TagDeliveryStream":
		return p.tagDeliveryStream(params)
	case "UntagDeliveryStream":
		return p.untagDeliveryStream(params)
	case "ListTagsForDeliveryStream":
		return p.listTagsForDeliveryStream(params)
	case "StartDeliveryStreamEncryption":
		return p.startEncryption(params)
	case "StopDeliveryStreamEncryption":
		return p.stopEncryption(params)
	// Kinesis source
	case "AddKinesisSource":
		return p.addKinesisSource(params)
	case "GetKinesisSources":
		return p.getKinesisSources(params)
	case "DisableKinesisStreamSource":
		return p.disableKinesisSource(params)
	// Redshift target
	case "PutRedshiftDestination":
		return p.putRedshiftDestination(params)
	case "GetRedshiftDestination":
		return p.getRedshiftDestination(params)
	case "UpdateRedshiftDestination":
		return p.putRedshiftDestination(params)
	// Elasticsearch target
	case "PutElasticsearchDestination":
		return p.putElasticsearchDestination(params)
	case "UpdateElasticsearchDestination":
		return p.putElasticsearchDestination(params)
	// HTTP endpoint
	case "PutHttpEndpointDestination":
		return p.putHTTPEndpointDestination(params)
	case "UpdateHttpEndpointDestination":
		return p.putHTTPEndpointDestination(params)
	// Splunk
	case "PutSplunkDestination":
		return p.putSplunkDestination(params)
	case "UpdateSplunkDestination":
		return p.putSplunkDestination(params)
	// S3 destination
	case "PutS3Destination":
		return p.putS3Destination(params)
	case "UpdateS3Destination":
		return p.putS3Destination(params)
	// ExtendedS3
	case "PutExtendedS3Destination":
		return p.putS3Destination(params)
	case "UpdateExtendedS3Destination":
		return p.putS3Destination(params)
	// Lifecycle
	case "StartDeliveryStream":
		return p.startDeliveryStream(params)
	case "StopDeliveryStream":
		return p.stopDeliveryStream(params)
	case "PauseDeliveryStream":
		return p.stopDeliveryStream(params)
	case "ResumeDeliveryStream":
		return p.startDeliveryStream(params)
	// Metrics / Logs
	case "GetDeliveryStreamMetrics":
		return p.getDeliveryStreamMetrics(params)
	case "GetDeliveryStreamLogs":
		return p.getDeliveryStreamLogs(params)
	case "DescribeDeliveryStreamHealth":
		return p.describeDeliveryStreamHealth(params)
	// Monitoring
	case "EnableLogging":
		return p.enableLogging(params)
	case "DisableLogging":
		return p.disableLogging(params)
	case "GetLoggingConfiguration":
		return p.getLoggingConfiguration(params)
	// Record stats / status
	case "GetRecordStatistics":
		return p.getRecordStatistics(params)
	case "ListRecords":
		return p.listRecords(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	streams, err := p.store.ListStreams("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(streams))
	for _, s := range streams {
		res = append(res, plugin.Resource{Type: "delivery-stream", ID: s.Name, Name: s.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	streamType, _ := params["DeliveryStreamType"].(string)
	if streamType == "" {
		streamType = "DirectPut"
	}
	destJSON := "{}"
	if dest, ok := params["ExtendedS3DestinationConfiguration"]; ok {
		b, _ := json.Marshal(dest)
		destJSON = string(b)
	}
	arn := shared.BuildARN("firehose", "deliverystream", name)
	st, err := p.store.CreateStream(name, arn, streamType, destJSON)
	if err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("ResourceInUseException", "stream already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags if provided
	if rawTags, ok := params["Tags"].([]any); ok {
		tags := parseTags(rawTags)
		p.store.tags.AddTags(st.ARN, tags)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DeliveryStreamARN": st.ARN})
}

func (p *Provider) deleteDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(st.ARN)
	if err := p.store.DeleteStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamDescription": streamToMap(st),
	})
}

func (p *Provider) listDeliveryStreams(params map[string]any) (*plugin.Response, error) {
	streamType, _ := params["DeliveryStreamType"].(string)
	streams, err := p.store.ListStreams(streamType)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(streams))
	for _, s := range streams {
		names = append(names, s.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamNames":    names,
		"HasMoreDeliveryStreams": false,
	})
}

func (p *Provider) putRecord(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RecordId":  shared.GenerateUUID(),
		"Encrypted": false,
	})
}

func (p *Provider) putRecordBatch(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	records, _ := params["Records"].([]any)
	results := make([]map[string]any, 0, len(records))
	for range records {
		results = append(results, map[string]any{"RecordId": shared.GenerateUUID()})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FailedPutCount":   0,
		"Encrypted":        false,
		"RequestResponses": results,
	})
}

func (p *Provider) updateDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	destJSON := "{}"
	if dest, ok := params["ExtendedS3DestinationUpdate"]; ok {
		b, _ := json.Marshal(dest)
		destJSON = string(b)
	}
	if err := p.store.UpdateDestination(name, destJSON); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) tagDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(st.ARN, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(st.ARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(st.ARN)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags":        tagList,
		"HasMoreTags": false,
	})
}

func (p *Provider) startEncryption(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if err := p.store.UpdateEncryption(name, "ENABLED"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopEncryption(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if err := p.store.UpdateEncryption(name, "DISABLED"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func streamToMap(st *DeliveryStream) map[string]any {
	var dest any
	json.Unmarshal([]byte(st.Dest), &dest)
	return map[string]any{
		"DeliveryStreamName":   st.Name,
		"DeliveryStreamARN":    st.ARN,
		"DeliveryStreamStatus": st.Status,
		"DeliveryStreamType":   st.Type,
		"CreateTimestamp":      st.CreatedAt.Unix(),
		"DeliveryStreamEncryptionConfiguration": map[string]string{
			"Status": st.Encryption,
		},
	}
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func sqlite_isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// --- Kinesis source handlers ---

func (p *Provider) addKinesisSource(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	kinesisARN, _ := params["KinesisStreamARN"].(string)
	roleARN, _ := params["RoleARN"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	if err := p.store.AddKinesisSource(name, kinesisARN, roleARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getKinesisSources(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	sources, err := p.store.GetKinesisSources(name)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sources))
	for _, s := range sources {
		items = append(items, map[string]any{
			"KinesisStreamARN": s.KinesisStreamARN,
			"RoleARN":          s.RoleARN,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"KinesisStreamSources": items,
	})
}

func (p *Provider) disableKinesisSource(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Redshift destination ---

func (p *Provider) putRedshiftDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	cfg, _ := params["RedshiftDestinationConfiguration"].(map[string]any)
	if cfg == nil {
		cfg = params
	}
	roleARN, _ := cfg["RoleARN"].(string)
	jdbcURL, _ := cfg["ClusterJDBCURL"].(string)
	db, _ := cfg["DatabaseName"].(string)
	username, _ := cfg["Username"].(string)
	s3Bucket := ""
	if s3, ok := cfg["S3Configuration"].(map[string]any); ok {
		s3Bucket, _ = s3["BucketARN"].(string)
	}
	copyOpts := ""
	if copyCmd, ok := cfg["CopyCommand"].(map[string]any); ok {
		if v, ok := copyCmd["CopyOptions"].(string); ok {
			copyOpts = v
		}
	}
	if err := p.store.PutRedshiftTarget(name, roleARN, jdbcURL, db, username, s3Bucket, copyOpts); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getRedshiftDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	rt, err := p.store.GetRedshiftTarget(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "redshift target not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RedshiftDestinationDescription": map[string]any{
			"RoleARN":        rt.RoleARN,
			"ClusterJDBCURL": rt.ClusterJDBCURL,
			"DatabaseName":   rt.DatabaseName,
			"Username":       rt.Username,
		},
	})
}

// --- Elasticsearch destination ---

func (p *Provider) putElasticsearchDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	cfg, _ := params["ElasticsearchDestinationConfiguration"].(map[string]any)
	if cfg == nil {
		cfg = params
	}
	domainARN, _ := cfg["DomainARN"].(string)
	indexName, _ := cfg["IndexName"].(string)
	typeName, _ := cfg["TypeName"].(string)
	roleARN, _ := cfg["RoleARN"].(string)
	if err := p.store.PutElasticsearchTarget(name, domainARN, indexName, typeName, roleARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- HTTP endpoint destination ---

func (p *Provider) putHTTPEndpointDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	cfg, _ := params["HttpEndpointDestinationConfiguration"].(map[string]any)
	if cfg == nil {
		cfg = params
	}
	url, epName := "", ""
	if ep, ok := cfg["EndpointConfiguration"].(map[string]any); ok {
		url, _ = ep["Url"].(string)
		epName, _ = ep["Name"].(string)
	}
	accessKey := ""
	if ep, ok := cfg["EndpointConfiguration"].(map[string]any); ok {
		accessKey, _ = ep["AccessKey"].(string)
	}
	roleARN, _ := cfg["RoleARN"].(string)
	if err := p.store.PutHTTPEndpointTarget(name, url, epName, accessKey, roleARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Splunk destination ---

func (p *Provider) putSplunkDestination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	cfg, _ := params["SplunkDestinationConfiguration"].(map[string]any)
	if cfg == nil {
		cfg = params
	}
	endpoint, _ := cfg["HECEndpoint"].(string)
	token, _ := cfg["HECToken"].(string)
	ack := false
	if v, ok := cfg["HECAcknowledgmentTimeoutInSeconds"].(float64); ok && v > 0 {
		ack = true
	}
	roleARN := ""
	if s3, ok := cfg["S3Configuration"].(map[string]any); ok {
		roleARN, _ = s3["RoleARN"].(string)
	}
	if err := p.store.PutSplunkTarget(name, endpoint, token, ack, roleARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- S3 destination (unified) ---

func (p *Provider) putS3Destination(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DeliveryStreamName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	destJSON := "{}"
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "S3DestinationUpdate", "ExtendedS3DestinationUpdate"} {
		if d, ok := params[key]; ok {
			b, _ := json.Marshal(d)
			destJSON = string(b)
			break
		}
	}
	if err := p.store.UpdateDestination(name, destJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Lifecycle ---

func (p *Provider) startDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if err := p.store.UpdateStreamStatus(name, "ACTIVE"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopDeliveryStream(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if err := p.store.UpdateStreamStatus(name, "PAUSED"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Metrics / Logs ---

func (p *Provider) getDeliveryStreamMetrics(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": name,
		"Metrics": map[string]any{
			"IncomingBytes":       0,
			"IncomingRecords":     0,
			"DeliveryToS3Bytes":   0,
			"DeliveryToS3Records": 0,
		},
	})
}

func (p *Provider) getDeliveryStreamLogs(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": name,
		"Logs":               []any{},
	})
}

func (p *Provider) describeDeliveryStreamHealth(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	st, err := p.store.GetStream(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": st.Name,
		"DeliveryStreamARN":  st.ARN,
		"Health":             "HEALTHY",
	})
}

// --- Logging configuration ---

func (p *Provider) enableLogging(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"LoggingEnabled": true})
}

func (p *Provider) disableLogging(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	if _, err := p.store.GetStream(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"LoggingEnabled": false})
}

func (p *Provider) getLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": name,
		"LoggingOptions": map[string]any{
			"Enabled":       true,
			"LogGroupName":  "/aws/kinesisfirehose/" + name,
			"LogStreamName": "S3Delivery",
		},
	})
}

// --- Record helpers ---

func (p *Provider) getRecordStatistics(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": name,
		"RecordCount":        0,
		"BytesProcessed":     0,
	})
}

func (p *Provider) listRecords(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DeliveryStreamName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DeliveryStreamName": name,
		"Records":            []any{},
	})
}
