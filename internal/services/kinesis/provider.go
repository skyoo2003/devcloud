// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesis/provider.go
package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "kinesis" }
func (p *Provider) ServiceName() string           { return "Kinesis_20131202" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "kinesis"))
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
		return jsonErr("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return jsonErr("SerializationException", "invalid JSON", http.StatusBadRequest), nil
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
	// Stream CRUD
	case "CreateStream":
		return p.createStream(params)
	case "DeleteStream":
		return p.deleteStream(params)
	case "DescribeStream":
		return p.describeStream(params)
	case "DescribeStreamSummary":
		return p.describeStreamSummary(params)
	case "ListStreams":
		return p.listStreams(params)

	// Record operations
	case "PutRecord":
		return p.putRecord(params)
	case "PutRecords":
		return p.putRecords(params)
	case "GetShardIterator":
		return p.getShardIterator(params)
	case "GetRecords":
		return p.getRecords(params)

	// Shard management
	case "ListShards":
		return p.listShards(params)
	case "SplitShard":
		return p.splitShard(params)
	case "MergeShards":
		return p.mergeShards(params)
	case "UpdateShardCount":
		return p.updateShardCount(params)

	// Consumer operations
	case "RegisterStreamConsumer":
		return p.registerStreamConsumer(params)
	case "DeregisterStreamConsumer":
		return p.deregisterStreamConsumer(params)
	case "DescribeStreamConsumer":
		return p.describeStreamConsumer(params)
	case "ListStreamConsumers":
		return p.listStreamConsumers(params)

	// Tags
	case "AddTagsToStream":
		return p.addTagsToStream(params)
	case "RemoveTagsFromStream":
		return p.removeTagsFromStream(params)
	case "ListTagsForStream":
		return p.listTagsForStream(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// Retention
	case "IncreaseStreamRetentionPeriod":
		return p.increaseRetention(params)
	case "DecreaseStreamRetentionPeriod":
		return p.decreaseRetention(params)

	// Encryption
	case "StartStreamEncryption":
		return p.startEncryption(params)
	case "StopStreamEncryption":
		return p.stopEncryption(params)

	// Mode
	case "UpdateStreamMode":
		return p.updateStreamMode(params)

	// Account/limits
	case "DescribeAccountSettings":
		return p.describeAccountSettings()
	case "DescribeLimits":
		return p.describeLimits()

	// Resource policy
	case "GetResourcePolicy":
		return p.getResourcePolicy()
	case "PutResourcePolicy":
		return jsonOK(map[string]any{})
	case "DeleteResourcePolicy":
		return jsonOK(map[string]any{})

	// Enhanced monitoring (stub)
	case "EnableEnhancedMonitoring":
		return p.enhancedMonitoringStub(params)
	case "DisableEnhancedMonitoring":
		return p.enhancedMonitoringStub(params)

	// Not supported / stubs
	case "SubscribeToShard":
		return jsonErr("InvalidArgumentException", "SubscribeToShard is not supported in emulator", http.StatusBadRequest), nil
	case "UpdateAccountSettings":
		return jsonOK(map[string]any{})
	case "UpdateMaxRecordSize":
		return jsonOK(map[string]any{})
	case "UpdateStreamWarmThroughput":
		return jsonOK(map[string]any{"streamARN": "", "streamName": ""})

	default:
		return jsonErr("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	streams, err := p.store.ListStreams()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(streams))
	for _, s := range streams {
		res = append(res, plugin.Resource{Type: "stream", ID: s.Name, Name: s.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Stream CRUD ---

func (p *Provider) createStream(params map[string]any) (*plugin.Response, error) {
	const maxShardCount = 1024
	name, _ := params["StreamName"].(string)
	if name == "" {
		return jsonErr("ValidationException", "StreamName is required", http.StatusBadRequest), nil
	}
	shardCount := intParam(params, "ShardCount", 1)
	if shardCount <= 0 || shardCount > maxShardCount {
		return jsonErr("ValidationException", fmt.Sprintf("ShardCount must be between 1 and %d", maxShardCount), http.StatusBadRequest), nil
	}
	mode := "PROVISIONED"
	if md, ok := params["StreamModeDetails"].(map[string]any); ok {
		if m, ok := md["StreamMode"].(string); ok && m != "" {
			mode = m
		}
	}
	arn := shared.BuildARN("kinesis", "stream", name)
	_, err := p.store.CreateStream(name, arn, shardCount, mode)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return jsonErr("ResourceInUseException", "stream already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags if provided
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := make(map[string]string, len(rawTags))
		for k, v := range rawTags {
			if vs, ok := v.(string); ok {
				tags[k] = vs
			}
		}
		_ = p.store.tags.AddTags(arn, tags)
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) deleteStream(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(st.ARN) //nolint:errcheck
	if err := p.store.DeleteStream(name); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) describeStream(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	shards := p.buildShardList(st)
	return jsonOK(map[string]any{
		"StreamDescription": map[string]any{
			"StreamName":              st.Name,
			"StreamARN":               st.ARN,
			"StreamStatus":            st.Status,
			"StreamModeDetails":       map[string]string{"StreamMode": st.Mode},
			"Shards":                  shards,
			"HasMoreShards":           false,
			"RetentionPeriodHours":    st.RetentionHours,
			"EncryptionType":          st.Encryption,
			"StreamCreationTimestamp": float64(st.CreatedAt.Unix()),
			"EnhancedMonitoring":      []map[string]any{{"ShardLevelMetrics": []string{}}},
		},
	})
}

func (p *Provider) describeStreamSummary(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	consumers, _ := p.store.ListConsumers(st.ARN)
	return jsonOK(map[string]any{
		"StreamDescriptionSummary": map[string]any{
			"StreamName":              st.Name,
			"StreamARN":               st.ARN,
			"StreamStatus":            st.Status,
			"StreamModeDetails":       map[string]string{"StreamMode": st.Mode},
			"RetentionPeriodHours":    st.RetentionHours,
			"StreamCreationTimestamp": float64(st.CreatedAt.Unix()),
			"EncryptionType":          st.Encryption,
			"OpenShardCount":          st.ShardCount,
			"ConsumerCount":           len(consumers),
			"EnhancedMonitoring":      []map[string]any{{"ShardLevelMetrics": []string{}}},
		},
	})
}

func (p *Provider) listStreams(_ map[string]any) (*plugin.Response, error) {
	streams, err := p.store.ListStreams()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(streams))
	summaries := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		names = append(names, s.Name)
		summaries = append(summaries, map[string]any{
			"StreamName":              s.Name,
			"StreamARN":               s.ARN,
			"StreamStatus":            s.Status,
			"StreamModeDetails":       map[string]string{"StreamMode": s.Mode},
			"StreamCreationTimestamp": float64(s.CreatedAt.Unix()),
		})
	}
	return jsonOK(map[string]any{
		"StreamNames":     names,
		"StreamSummaries": summaries,
		"HasMoreStreams":  false,
	})
}

// --- Record operations ---

func (p *Provider) putRecord(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	partitionKey, _ := params["PartitionKey"].(string)
	if partitionKey == "" {
		return jsonErr("ValidationException", "PartitionKey is required", http.StatusBadRequest), nil
	}
	data, err := extractData(params["Data"])
	if err != nil {
		return jsonErr("ValidationException", "invalid Data", http.StatusBadRequest), nil
	}
	shardID, seqNum, err := p.store.buffer.putRecord(name, partitionKey, data, st.ShardCount)
	if err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{
		"ShardId":        shardID,
		"SequenceNumber": seqNum,
		"EncryptionType": st.Encryption,
	})
}

func (p *Provider) putRecords(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	rawRecords, _ := params["Records"].([]any)
	results := make([]map[string]any, 0, len(rawRecords))
	failedCount := 0
	for _, raw := range rawRecords {
		rec, _ := raw.(map[string]any)
		pk, _ := rec["PartitionKey"].(string)
		data, err := extractData(rec["Data"])
		if err != nil || pk == "" {
			failedCount++
			results = append(results, map[string]any{
				"ErrorCode":    "ValidationException",
				"ErrorMessage": "invalid record",
			})
			continue
		}
		shardID, seqNum, err := p.store.buffer.putRecord(name, pk, data, st.ShardCount)
		if err != nil {
			failedCount++
			results = append(results, map[string]any{
				"ErrorCode":    "InternalFailure",
				"ErrorMessage": err.Error(),
			})
			continue
		}
		results = append(results, map[string]any{
			"ShardId":        shardID,
			"SequenceNumber": seqNum,
		})
	}
	return jsonOK(map[string]any{
		"FailedRecordCount": failedCount,
		"Records":           results,
		"EncryptionType":    st.Encryption,
	})
}

func (p *Provider) getShardIterator(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStream(name); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	shardID, _ := params["ShardId"].(string)
	if shardID == "" {
		return jsonErr("ValidationException", "ShardId is required", http.StatusBadRequest), nil
	}
	iterType, _ := params["ShardIteratorType"].(string)
	if iterType == "" {
		return jsonErr("ValidationException", "ShardIteratorType is required", http.StatusBadRequest), nil
	}
	startingSeqNum, _ := params["StartingSequenceNumber"].(string)

	var ts time.Time
	if tsVal, ok := params["Timestamp"]; ok {
		if f, ok := tsVal.(float64); ok {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			ts = time.Unix(sec, nsec)
		}
	}

	iterID, err := p.store.buffer.createIterator(name, shardID, iterType, startingSeqNum, ts)
	if err != nil {
		return jsonErr("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{
		"ShardIterator": iterID,
	})
}

func (p *Provider) getRecords(params map[string]any) (*plugin.Response, error) {
	iterID, _ := params["ShardIterator"].(string)
	if iterID == "" {
		return jsonErr("ValidationException", "ShardIterator is required", http.StatusBadRequest), nil
	}
	limit := intParam(params, "Limit", 10000)
	if limit <= 0 {
		limit = 10000
	}

	records, nextIter, millisBehind, err := p.store.buffer.getRecords(iterID, limit)
	if err != nil {
		return jsonErr("ExpiredIteratorException", err.Error(), http.StatusBadRequest), nil
	}

	recList := make([]map[string]any, 0, len(records))
	for _, r := range records {
		recList = append(recList, map[string]any{
			"PartitionKey":                r.PartitionKey,
			"Data":                        base64.StdEncoding.EncodeToString(r.Data),
			"SequenceNumber":              r.SequenceNumber,
			"ApproximateArrivalTimestamp": float64(r.Timestamp.UnixMilli()) / 1000.0,
		})
	}
	return jsonOK(map[string]any{
		"Records":            recList,
		"NextShardIterator":  nextIter,
		"MillisBehindLatest": millisBehind,
	})
}

// --- Shard management ---

func (p *Provider) listShards(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	shards := p.buildShardList(st)
	return jsonOK(map[string]any{
		"Shards": shards,
	})
}

func (p *Provider) splitShard(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	if _, err := p.store.buffer.addShard(name); err != nil {
		return nil, err
	}
	p.store.UpdateShardCount(name, st.ShardCount+1) //nolint:errcheck
	return jsonOK(map[string]any{})
}

func (p *Provider) mergeShards(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	if st.ShardCount <= 1 {
		return jsonErr("InvalidArgumentException", "cannot merge below 1 shard", http.StatusBadRequest), nil
	}
	// Remove the last shard (simplified)
	lastShardID := fmt.Sprintf("shardId-%012d", st.ShardCount-1)
	if err := p.store.buffer.removeShard(name, lastShardID); err != nil {
		return nil, err
	}
	p.store.UpdateShardCount(name, st.ShardCount-1) //nolint:errcheck
	return jsonOK(map[string]any{})
}

func (p *Provider) updateShardCount(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	targetCount := intParam(params, "TargetShardCount", st.ShardCount)
	if targetCount <= 0 {
		return jsonErr("ValidationException", "TargetShardCount must be positive", http.StatusBadRequest), nil
	}
	oldCount := st.ShardCount
	if err := p.store.UpdateShardCount(name, targetCount); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{
		"StreamName":        name,
		"StreamARN":         st.ARN,
		"CurrentShardCount": oldCount,
		"TargetShardCount":  targetCount,
	})
}

// --- Consumer operations ---

func (p *Provider) registerStreamConsumer(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamARN"].(string)
	if streamARN == "" {
		return jsonErr("ValidationException", "StreamARN is required", http.StatusBadRequest), nil
	}
	consumerName, _ := params["ConsumerName"].(string)
	if consumerName == "" {
		return jsonErr("ValidationException", "ConsumerName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStreamByARN(streamARN); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	c, err := p.store.RegisterConsumer(consumerName, streamARN)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return jsonErr("ResourceInUseException", "consumer already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonOK(map[string]any{
		"Consumer": map[string]any{
			"ConsumerName":              c.Name,
			"ConsumerARN":               c.ARN,
			"ConsumerStatus":            c.Status,
			"ConsumerCreationTimestamp": float64(c.CreatedAt.Unix()),
		},
	})
}

func (p *Provider) deregisterStreamConsumer(params map[string]any) (*plugin.Response, error) {
	consumerARN, _ := params["ConsumerARN"].(string)
	if consumerARN != "" {
		if err := p.store.DeregisterConsumer(consumerARN); err != nil {
			return jsonErr("ResourceNotFoundException", "consumer not found", http.StatusBadRequest), nil
		}
		return jsonOK(map[string]any{})
	}
	streamARN, _ := params["StreamARN"].(string)
	consumerName, _ := params["ConsumerName"].(string)
	if streamARN == "" || consumerName == "" {
		return jsonErr("ValidationException", "ConsumerARN or (StreamARN + ConsumerName) is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterConsumerByName(consumerName, streamARN); err != nil {
		return jsonErr("ResourceNotFoundException", "consumer not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) describeStreamConsumer(params map[string]any) (*plugin.Response, error) {
	var c *ConsumerMeta
	var err error
	consumerARN, _ := params["ConsumerARN"].(string)
	if consumerARN != "" {
		c, err = p.store.GetConsumer(consumerARN)
	} else {
		streamARN, _ := params["StreamARN"].(string)
		consumerName, _ := params["ConsumerName"].(string)
		if streamARN == "" || consumerName == "" {
			return jsonErr("ValidationException", "ConsumerARN or (StreamARN + ConsumerName) is required", http.StatusBadRequest), nil
		}
		c, err = p.store.GetConsumerByName(consumerName, streamARN)
	}
	if err != nil {
		return jsonErr("ResourceNotFoundException", "consumer not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{
		"ConsumerDescription": map[string]any{
			"ConsumerName":              c.Name,
			"ConsumerARN":               c.ARN,
			"ConsumerStatus":            c.Status,
			"StreamARN":                 c.StreamARN,
			"ConsumerCreationTimestamp": float64(c.CreatedAt.Unix()),
		},
	})
}

func (p *Provider) listStreamConsumers(params map[string]any) (*plugin.Response, error) {
	streamARN, _ := params["StreamARN"].(string)
	if streamARN == "" {
		return jsonErr("ValidationException", "StreamARN is required", http.StatusBadRequest), nil
	}
	consumers, err := p.store.ListConsumers(streamARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(consumers))
	for _, c := range consumers {
		list = append(list, map[string]any{
			"ConsumerName":              c.Name,
			"ConsumerARN":               c.ARN,
			"ConsumerStatus":            c.Status,
			"ConsumerCreationTimestamp": float64(c.CreatedAt.Unix()),
		})
	}
	return jsonOK(map[string]any{
		"Consumers": list,
	})
}

// --- Tags ---

func (p *Provider) addTagsToStream(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string, len(rawTags))
	for k, v := range rawTags {
		if vs, ok := v.(string); ok {
			tags[k] = vs
		}
	}
	if err := p.store.tags.AddTags(st.ARN, tags); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) removeTagsFromStream(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	keys := extractStringSlice(params, "TagKeys")
	if err := p.store.tags.RemoveTags(st.ARN, keys); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) listTagsForStream(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStream(name)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(st.ARN)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return jsonOK(map[string]any{
		"Tags":        tagList,
		"HasMoreTags": false,
	})
}

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return jsonErr("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string, len(rawTags))
	for k, v := range rawTags {
		if vs, ok := v.(string); ok {
			tags[k] = vs
		}
	}
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return jsonErr("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	keys := extractStringSlice(params, "TagKeys")
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return jsonErr("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return jsonOK(map[string]any{
		"Tags": tagList,
	})
}

// --- Retention ---

func (p *Provider) increaseRetention(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	hours := intParam(params, "RetentionPeriodHours", 0)
	if hours <= 0 {
		return jsonErr("ValidationException", "RetentionPeriodHours is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRetention(name, hours); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) decreaseRetention(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	hours := intParam(params, "RetentionPeriodHours", 0)
	if hours <= 0 {
		return jsonErr("ValidationException", "RetentionPeriodHours is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRetention(name, hours); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

// --- Encryption ---

func (p *Provider) startEncryption(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateEncryption(name, "KMS"); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

func (p *Provider) stopEncryption(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	if name == "" {
		return jsonErr("ValidationException", "StreamName or StreamARN is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateEncryption(name, "NONE"); err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	return jsonOK(map[string]any{})
}

// --- Mode ---

func (p *Provider) updateStreamMode(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["StreamARN"].(string)
	if arn == "" {
		return jsonErr("ValidationException", "StreamARN is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStreamByARN(arn)
	if err != nil {
		return jsonErr("ResourceNotFoundException", "stream not found", http.StatusBadRequest), nil
	}
	mode := "PROVISIONED"
	if md, ok := params["StreamModeDetails"].(map[string]any); ok {
		if m, ok := md["StreamMode"].(string); ok && m != "" {
			mode = m
		}
	}
	if err := p.store.UpdateMode(st.Name, mode); err != nil {
		return nil, err
	}
	return jsonOK(map[string]any{})
}

// --- Account/limits stubs ---

func (p *Provider) describeAccountSettings() (*plugin.Response, error) {
	return jsonOK(map[string]any{})
}

func (p *Provider) describeLimits() (*plugin.Response, error) {
	return jsonOK(map[string]any{
		"ShardLimit":               200,
		"OpenShardCount":           0,
		"OnDemandStreamCount":      0,
		"OnDemandStreamCountLimit": 10,
	})
}

func (p *Provider) getResourcePolicy() (*plugin.Response, error) {
	return jsonOK(map[string]any{
		"Policy": "{}",
	})
}

func (p *Provider) enhancedMonitoringStub(params map[string]any) (*plugin.Response, error) {
	name := p.resolveStreamName(params)
	st, _ := p.store.GetStream(name)
	streamARN := ""
	if st != nil {
		streamARN = st.ARN
	}
	return jsonOK(map[string]any{
		"StreamName":               name,
		"StreamARN":                streamARN,
		"CurrentShardLevelMetrics": []string{},
		"DesiredShardLevelMetrics": []string{},
	})
}

// --- Helpers ---

func (p *Provider) resolveStreamName(params map[string]any) string {
	if name, ok := params["StreamName"].(string); ok && name != "" {
		return name
	}
	if arn, ok := params["StreamARN"].(string); ok && arn != "" {
		st, err := p.store.GetStreamByARN(arn)
		if err == nil {
			return st.Name
		}
		// Fallback: extract name from ARN
		parts := strings.Split(arn, "/")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
	}
	return ""
}

func (p *Provider) buildShardList(st *StreamMeta) []map[string]any {
	shardMap := p.store.buffer.getShards(st.Name)
	shards := make([]map[string]any, 0, len(shardMap))
	for _, shard := range shardMap {
		shards = append(shards, map[string]any{
			"ShardId": shard.ID,
			"HashKeyRange": map[string]string{
				"StartingHashKey": "0",
				"EndingHashKey":   "340282366920938463463374607431768211455",
			},
			"SequenceNumberRange": map[string]string{
				"StartingSequenceNumber": "0",
			},
		})
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i]["ShardId"].(string) < shards[j]["ShardId"].(string)
	})
	return shards
}

func extractData(v any) ([]byte, error) {
	switch d := v.(type) {
	case string:
		return base64.StdEncoding.DecodeString(d)
	case []byte:
		return d, nil
	default:
		return nil, fmt.Errorf("unsupported data type")
	}
}

func extractStringSlice(params map[string]any, key string) []string {
	rawKeys, _ := params[key].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	return keys
}

func intParam(params map[string]any, key string, defaultVal int) int {
	v, ok := params[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return defaultVal
	}
}

func jsonOK(v any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, v)
}

func jsonErr(code, message string, status int) *plugin.Response {
	return shared.JSONError(code, message, status)
}
