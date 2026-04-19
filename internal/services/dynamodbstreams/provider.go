// SPDX-License-Identifier: Apache-2.0

// internal/services/dynamodbstreams/provider.go
package dynamodbstreams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the DynamoDBStreams_20120810 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "dynamodbstreams" }
func (p *Provider) ServiceName() string           { return "DynamoDBStreams_20120810" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "dynamodbstreams"))
	if err != nil {
		return err
	}
	// Expose to dynamodb write-path integration.
	SetGlobalStore(p.store)
	return nil
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		// Clear the global reference if it still points at us.
		if GetGlobalStore() == p.store {
			SetGlobalStore(nil)
		}
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return json10Err("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return json10Err("SerializationException", "invalid JSON", http.StatusBadRequest), nil
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
	case "ListStreams":
		return p.listStreams(params)
	case "ListStreamArns":
		return p.listStreamArns(params)
	case "DescribeStream":
		return p.describeStream(params)
	case "GetShardIterator":
		return p.getShardIterator(params)
	case "GetRecords":
		return p.getRecords(params)
	case "AddTagsToStream":
		return p.addTagsToStream(params)
	case "RemoveTagsFromStream":
		return p.removeTagsFromStream(params)
	case "ListTagsOfStream":
		return p.listTagsOfStream(params)
	case "EnableStream":
		return p.enableStream(params)
	case "DisableStream":
		return p.disableStream(params)
	case "DescribeContinuousBackups":
		return p.describeContinuousBackups(params)
	case "GetStreamBillingMode":
		return p.getStreamBillingMode(params)
	case "UpdateStream":
		return p.updateStream(params)
	case "DescribeStreamSummary":
		return p.describeStreamSummary(params)
	case "ListShards":
		return p.listShards(params)
	case "GetShardIteratorBatch":
		return p.getShardIteratorBatch(params)
	case "PurgeStream":
		return p.purgeStream(params)
	case "PutRecord":
		return p.putRecord(params)
	case "MergeShards":
		return p.mergeShards(params)
	case "SplitShard":
		return p.splitShard(params)
	case "ResetStreamIterator":
		return p.resetStreamIterator(params)
	case "DescribeLimits":
		return p.describeLimits(params)
	default:
		return json10Err("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	streams, err := p.store.ListStreams("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(streams))
	for _, s := range streams {
		res = append(res, plugin.Resource{Type: "stream", ID: s.ARN, Name: s.TableName})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	streams, err := p.store.ListStreams("")
	if err != nil {
		return &plugin.ServiceMetrics{}, nil
	}
	return &plugin.ServiceMetrics{ResourceCount: len(streams)}, nil
}

// ---- handlers ----

func (p *Provider) listStreams(params map[string]any) (*plugin.Response, error) {
	tableName, _ := params["TableName"].(string)
	streams, err := p.store.ListStreams(tableName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		list = append(list, map[string]any{
			"StreamArn":   s.ARN,
			"StreamLabel": s.Label,
			"TableName":   s.TableName,
		})
	}
	return json10Resp(http.StatusOK, map[string]any{"Streams": list})
}

func (p *Provider) listStreamArns(params map[string]any) (*plugin.Response, error) {
	tableName, _ := params["TableName"].(string)
	streams, err := p.store.ListStreams(tableName)
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(streams))
	for _, s := range streams {
		arns = append(arns, s.ARN)
	}
	return json10Resp(http.StatusOK, map[string]any{"StreamArns": arns})
}

func (p *Provider) describeStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStreamByARN(streamARN)
	if err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}

	shardID := DefaultShardID()
	shards := []map[string]any{
		{
			"ShardId": shardID,
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": "000000000000000000001",
			},
		},
	}

	return json10Resp(http.StatusOK, map[string]any{
		"StreamDescription": map[string]any{
			"StreamArn":               st.ARN,
			"StreamLabel":             st.Label,
			"StreamStatus":            st.Status,
			"StreamViewType":          st.ViewType,
			"TableName":               st.TableName,
			"CreationRequestDateTime": st.CreatedAt.Unix(),
			"Shards":                  shards,
			"KeySchema":               []any{},
		},
	})
}

func (p *Provider) describeStreamSummary(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStreamByARN(streamARN)
	if err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"StreamSummary": map[string]any{
			"StreamArn":      st.ARN,
			"StreamLabel":    st.Label,
			"StreamStatus":   st.Status,
			"StreamViewType": st.ViewType,
			"TableName":      st.TableName,
		},
	})
}

func (p *Provider) listShards(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	shards := []map[string]any{
		{
			"ShardId": DefaultShardID(),
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": "000000000000000000001",
			},
		},
	}
	return json10Resp(http.StatusOK, map[string]any{"Shards": shards})
}

func (p *Provider) getShardIterator(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	shardID, _ := params["ShardId"].(string)
	if shardID == "" {
		return json10Err("ValidationException", "ShardId is required", http.StatusBadRequest), nil
	}
	iteratorType, _ := params["ShardIteratorType"].(string)
	if iteratorType == "" {
		iteratorType = "TRIM_HORIZON"
	}
	seqNum, _ := params["SequenceNumber"].(string)

	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}

	iterID, err := p.store.CreateIterator(streamARN, shardID, iteratorType, seqNum)
	if err != nil {
		return json10Err("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"ShardIterator": iterID})
}

func (p *Provider) getShardIteratorBatch(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	iterID, err := p.store.CreateIterator(streamARN, DefaultShardID(), "TRIM_HORIZON", "")
	if err != nil {
		return json10Err("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"ShardIterators": []map[string]any{{"ShardId": DefaultShardID(), "ShardIterator": iterID}},
	})
}

func (p *Provider) getRecords(params map[string]any) (*plugin.Response, error) {
	iteratorID, _ := params["ShardIterator"].(string)
	if iteratorID == "" {
		return json10Err("ValidationException", "ShardIterator is required", http.StatusBadRequest), nil
	}

	var limit int
	if v, ok := params["Limit"].(float64); ok {
		limit = int(v)
	}

	records, nextIterID, err := p.store.GetRecords(iteratorID, limit)
	if err != nil {
		return json10Err("ExpiredIteratorException", err.Error(), http.StatusBadRequest), nil
	}

	recList := make([]map[string]any, 0, len(records))
	for _, r := range records {
		dyn := map[string]any{
			"SequenceNumber":              r.SequenceNum,
			"ApproximateCreationDateTime": r.Timestamp.Unix(),
			"StreamViewType":              r.ViewType,
			"SizeBytes":                   r.SizeBytes,
		}
		if r.Keys != nil {
			dyn["Keys"] = r.Keys
		}
		if r.NewImage != nil {
			dyn["NewImage"] = r.NewImage
		}
		if r.OldImage != nil {
			dyn["OldImage"] = r.OldImage
		}
		rec := map[string]any{
			"eventID":      r.EventID,
			"eventName":    r.EventName,
			"eventSource":  "aws:dynamodb",
			"eventVersion": "1.1",
			"dynamodb":     dyn,
		}
		recList = append(recList, rec)
	}

	return json10Resp(http.StatusOK, map[string]any{
		"Records":           recList,
		"NextShardIterator": nextIterID,
	})
}

func (p *Provider) addTagsToStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	tags := make(map[string]string, len(rawTags))
	for _, t := range rawTags {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m["Key"].(string)
		v, _ := m["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	if err := p.store.AddTags(streamARN, tags); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) removeTagsFromStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.RemoveTags(streamARN, keys); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsOfStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(streamARN)
	if err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return json10Resp(http.StatusOK, map[string]any{"Tags": tagList})
}

func (p *Provider) enableStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.SetStreamStatus(streamARN, "ENABLED"); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"StreamStatus": "ENABLED"})
}

func (p *Provider) disableStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.SetStreamStatus(streamARN, "DISABLED"); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"StreamStatus": "DISABLED"})
}

func (p *Provider) describeContinuousBackups(params map[string]any) (*plugin.Response, error) {
	tableName, _ := params["TableName"].(string)
	if tableName == "" {
		return json10Err("ValidationException", "TableName is required", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus": "ENABLED",
			"PointInTimeRecoveryDescription": map[string]any{
				"PointInTimeRecoveryStatus": "DISABLED",
			},
		},
	})
}

func (p *Provider) getStreamBillingMode(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"BillingMode": "PROVISIONED",
		"StreamArn":   streamARN,
	})
}

func (p *Provider) updateStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"StreamArn": streamARN})
}

func (p *Provider) purgeStream(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	shardID := DefaultShardID()
	p.store.buf.mu.Lock()
	if shard, ok := p.store.buf.shards[shardID]; ok {
		shard.Records = nil
	}
	p.store.buf.mu.Unlock()
	return json10Resp(http.StatusOK, map[string]any{"StreamArn": streamARN})
}

func (p *Provider) putRecord(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStreamByARN(streamARN)
	if err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	eventName, _ := params["EventName"].(string)
	if eventName == "" {
		eventName = "INSERT"
	}
	keys, _ := params["Keys"].(map[string]any)
	newImg, _ := params["NewImage"].(map[string]any)
	oldImg, _ := params["OldImage"].(map[string]any)
	if err := p.store.PublishRecord(st.TableName, eventName, keys, newImg, oldImg); err != nil {
		return json10Err("InternalServerError", err.Error(), http.StatusInternalServerError), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"StreamArn": streamARN})
}

func (p *Provider) mergeShards(params map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"ShardId": DefaultShardID(),
	})
}

func (p *Provider) splitShard(params map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"ShardId": DefaultShardID(),
	})
}

func (p *Provider) resetStreamIterator(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamArn"].(string)
	if streamARN == "" {
		return json10Err("ValidationException", "StreamArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return json10Err("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	iterID, err := p.store.CreateIterator(streamARN, DefaultShardID(), "TRIM_HORIZON", "")
	if err != nil {
		return json10Err("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{"ShardIterator": iterID})
}

func (p *Provider) describeLimits(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"AccountMaxReadCapacityUnits":  40000,
		"AccountMaxWriteCapacityUnits": 40000,
		"TableMaxReadCapacityUnits":    40000,
		"TableMaxWriteCapacityUnits":   40000,
	})
}

// ---- test helpers ----

// createStreamForTable is a helper exposed for test setup.
func (p *Provider) createStreamForTable(tableName string) (*StreamMeta, error) {
	arn := shared.BuildARNWithAccount("dynamodb", "table", tableName+"/stream/"+time.Now().Format("2006-01-01T15:04:05.000"), shared.DefaultRegion, shared.DefaultAccountID)
	label := time.Now().Format("2006-01-01T15:04:05.000")
	return p.store.CreateStream(arn, tableName, label, "NEW_AND_OLD_IMAGES")
}

// ---- wire helpers ----

// json10Resp returns a JSON 1.0 success response.
func json10Resp(status int, v any) (*plugin.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}, nil
}

// json10Err returns a JSON 1.0 error response.
func json10Err(code, message string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}
}
