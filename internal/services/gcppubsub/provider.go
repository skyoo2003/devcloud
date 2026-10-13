// SPDX-License-Identifier: Apache-2.0

package gcppubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/gcppubsub"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "gcppubsub" }
func (p *Provider) ServiceName() string           { return "Google Cloud PubSub" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "gcp" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.store = NewStore()
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	topics, err := p.store.ListTopics("")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, t := range topics {
		res = append(res, plugin.Resource{
			Type: "pubsub.topic",
			ID:   t.Name,
			Name: t.Name,
		})
	}
	return res, nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	matchedOp, params := matchPubSubRoute(req.Method, req.URL.RequestURI())
	if matchedOp != "" {
		op = matchedOp
	}

	switch op {
	case "ListTopics":
		project := params["project"]
		topics, err := p.store.ListTopics(project)
		if err != nil {
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		if topics == nil {
			topics = []*Topic{}
		}
		body, _ := json.Marshal(map[string]any{"topics": topics})
		return jsonResponse(http.StatusOK, body)

	case "CreateTopic":
		project := params["project"]
		topic := params["topic"]

		var payload struct {
			Labels map[string]string `json:"labels"`
		}
		if bodyBytes, err := io.ReadAll(req.Body); err == nil && len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &payload)
		}

		t, err := p.store.CreateTopic(project, topic, payload.Labels)
		if err != nil {
			if errors.Is(err, ErrTopicAlreadyExists) {
				return gcpErrorResponse(http.StatusConflict, "AlreadyExists", fmt.Sprintf("Topic [%s] already exists.", topic))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(t)
		return jsonResponse(http.StatusOK, body)

	case "GetTopic":
		project := params["project"]
		topic := params["topic"]
		t, err := p.store.GetTopic(project, topic)
		if err != nil {
			if errors.Is(err, ErrTopicNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Topic [%s] not found.", topic))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(t)
		return jsonResponse(http.StatusOK, body)

	case "DeleteTopic":
		project := params["project"]
		topic := params["topic"]
		err := p.store.DeleteTopic(project, topic)
		if err != nil {
			if errors.Is(err, ErrTopicNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Topic [%s] not found.", topic))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		return jsonResponse(http.StatusOK, []byte("{}"))

	case "Publish":
		project := params["project"]
		topic := params["topic"]

		var payload struct {
			Messages []PubsubMessage `json:"messages"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "failed to read body")
		}
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "invalid JSON")
		}

		ids, err := p.store.Publish(project, topic, payload.Messages)
		if err != nil {
			if errors.Is(err, ErrTopicNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Topic [%s] not found.", topic))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(map[string]any{"messageIds": ids})
		return jsonResponse(http.StatusOK, body)

	case "ListSubscriptions":
		project := params["project"]
		subs, err := p.store.ListSubscriptions(project)
		if err != nil {
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		if subs == nil {
			subs = []*Subscription{}
		}
		body, _ := json.Marshal(map[string]any{"subscriptions": subs})
		return jsonResponse(http.StatusOK, body)

	case "CreateSubscription":
		project := params["project"]
		subscription := params["subscription"]

		var payload struct {
			Topic              string            `json:"topic"`
			AckDeadlineSeconds int               `json:"ackDeadlineSeconds"`
			Labels             map[string]string `json:"labels"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "failed to read body")
		}
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "invalid JSON")
		}

		sub, err := p.store.CreateSubscription(project, subscription, payload.Topic, payload.AckDeadlineSeconds, payload.Labels)
		if err != nil {
			if errors.Is(err, ErrSubAlreadyExists) {
				return gcpErrorResponse(http.StatusConflict, "AlreadyExists", fmt.Sprintf("Subscription [%s] already exists.", subscription))
			}
			if errors.Is(err, ErrTopicNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Topic [%s] not found.", payload.Topic))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sub)
		return jsonResponse(http.StatusOK, body)

	case "GetSubscription":
		project := params["project"]
		subscription := params["subscription"]
		sub, err := p.store.GetSubscription(project, subscription)
		if err != nil {
			if errors.Is(err, ErrSubscriptionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Subscription [%s] not found.", subscription))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sub)
		return jsonResponse(http.StatusOK, body)

	case "DeleteSubscription":
		project := params["project"]
		subscription := params["subscription"]
		err := p.store.DeleteSubscription(project, subscription)
		if err != nil {
			if errors.Is(err, ErrSubscriptionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Subscription [%s] not found.", subscription))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		return jsonResponse(http.StatusOK, []byte("{}"))

	case "Pull":
		project := params["project"]
		subscription := params["subscription"]

		var payload struct {
			MaxMessages int `json:"maxMessages"`
		}
		if bodyBytes, err := io.ReadAll(req.Body); err == nil && len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &payload)
		}
		if payload.MaxMessages <= 0 {
			payload.MaxMessages = 10
		}

		received, err := p.store.Pull(project, subscription, payload.MaxMessages)
		if err != nil {
			if errors.Is(err, ErrSubscriptionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Subscription [%s] not found.", subscription))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		if received == nil {
			received = []*ReceivedMessage{}
		}
		body, _ := json.Marshal(map[string]any{"receivedMessages": received})
		return jsonResponse(http.StatusOK, body)

	case "Acknowledge":
		project := params["project"]
		subscription := params["subscription"]

		var payload struct {
			AckIDs []string `json:"ackIds"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "failed to read body")
		}
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "invalid JSON")
		}

		err = p.store.Acknowledge(project, subscription, payload.AckIDs)
		if err != nil {
			if errors.Is(err, ErrSubscriptionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Subscription [%s] not found.", subscription))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		return jsonResponse(http.StatusOK, []byte("{}"))
	}

	return nil, plugin.ErrUnhandledOp
}

func matchPubSubRoute(method, uri string) (string, map[string]string) {
	uPath, _, _ := strings.Cut(uri, "?")
	parts := strings.Split(strings.Trim(uPath, "/"), "/")
	// Expected prefix: ["v1", "projects", "{project}", ...]
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "projects" {
		return "", nil
	}

	project := parts[2]
	resourceType := parts[3] // "topics" or "subscriptions"

	if resourceType == "topics" {
		params := map[string]string{"project": project}
		if len(parts) == 4 {
			if method == http.MethodGet {
				return "ListTopics", params
			}
			return "", nil
		}

		topicPart := parts[4]
		if tName, action, found := strings.Cut(topicPart, ":"); found {
			params["topic"] = tName
			if action == "publish" && method == http.MethodPost {
				return "Publish", params
			}
			return "", nil
		}

		params["topic"] = topicPart
		if len(parts) == 5 {
			if method == http.MethodPut {
				return "CreateTopic", params
			}
			if method == http.MethodGet {
				return "GetTopic", params
			}
			if method == http.MethodDelete {
				return "DeleteTopic", params
			}
		}
		return "", nil
	}

	if resourceType == "subscriptions" {
		params := map[string]string{"project": project}
		if len(parts) == 4 {
			if method == http.MethodGet {
				return "ListSubscriptions", params
			}
			return "", nil
		}

		subPart := parts[4]
		if sName, action, found := strings.Cut(subPart, ":"); found {
			params["subscription"] = sName
			if action == "pull" && method == http.MethodPost {
				return "Pull", params
			}
			if action == "acknowledge" && method == http.MethodPost {
				return "Acknowledge", params
			}
			return "", nil
		}

		params["subscription"] = subPart
		if len(parts) == 5 {
			if method == http.MethodPut {
				return "CreateSubscription", params
			}
			if method == http.MethodGet {
				return "GetSubscription", params
			}
			if method == http.MethodDelete {
				return "DeleteSubscription", params
			}
		}
		return "", nil
	}

	return "", nil
}

func jsonResponse(status int, body []byte) (*plugin.Response, error) {
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		ContentType: "application/json",
	}, nil
}

func gcpErrorResponse(status int, reason, message string) (*plugin.Response, error) {
	resp := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"status":  reason,
		},
	}
	body, _ := json.Marshal(resp)
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		ContentType: "application/json",
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("gcppubsub", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
