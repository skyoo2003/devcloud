// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type PublishAttribute struct {
	DataType, StringValue string
	BinaryValue           []byte
}
type PublishEntry struct {
	ID, Message, Subject, MessageStructure, MessageGroupID, MessageDeduplicationID string
	Attributes                                                                     map[string]PublishAttribute
}
type Publication struct {
	MessageID, SequenceNumber, MessageDeduplicationID string
	Duplicate                                         bool
}

func validateTopicFIFO(name string, attrs map[string]string) error {
	fifo := attrs["FifoTopic"] == "true"
	if v, ok := attrs["FifoTopic"]; ok && v != "true" && v != "false" {
		return invalidSNS("invalid FifoTopic")
	}
	if strings.HasSuffix(name, ".fifo") != fifo {
		return invalidSNS("FIFO name and FifoTopic must agree")
	}
	if v, ok := attrs["ContentBasedDeduplication"]; ok {
		if !fifo || (v != "true" && v != "false") {
			return invalidSNS("invalid ContentBasedDeduplication")
		}
	}
	if v, ok := attrs["FifoThroughputScope"]; ok {
		if !fifo || (v != "Topic" && v != "MessageGroup") {
			return invalidSNS("invalid FifoThroughputScope")
		}
	}
	return nil
}
func (s *SNSStore) CreateTopicWithAttributes(arn, name, accountID string, attributes map[string]string) (*Topic, error) {
	if e := validateTopicFIFO(name, attributes); e != nil {
		return nil, e
	}
	var out *Topic
	e := s.withStateTx(func(tx *sql.Tx) error {
		old, e := scanTopic(tx.QueryRow(`SELECT arn,name,account_id,attributes,created_at FROM topics WHERE arn=?`, arn))
		if e == nil {
			out = old
			return nil
		}
		if !errors.Is(e, ErrTopicNotFound) {
			return e
		}
		attrs := map[string]string{}
		for k, v := range attributes {
			attrs[k] = v
		}
		if attrs["FifoTopic"] == "true" {
			if _, ok := attrs["ContentBasedDeduplication"]; !ok {
				attrs["ContentBasedDeduplication"] = "false"
			}
			if _, ok := attrs["FifoThroughputScope"]; !ok {
				attrs["FifoThroughputScope"] = "Topic"
			}
		}
		raw, e := json.Marshal(attrs)
		if e != nil {
			return e
		}
		now := time.Now().Unix()
		_, e = tx.Exec(`INSERT INTO topics VALUES (?,?,?,?,?)`, arn, name, accountID, string(raw), now)
		if e == nil {
			out = &Topic{arn, name, accountID, attrs, time.Unix(now, 0)}
		}
		return e
	})
	return out, e
}
func validFIFOIdentifier(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, c := range []byte(id) {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

// Delivery holds the state transaction to preserve admission order. A sink write
// cannot be undone if a later SQLite commit fails; FIFO retry uses the same ID.
func (s *SNSStore) PublishMessage(topicARN string, entry PublishEntry, now time.Time, deliver func(Publication, []Subscription) error) (*Publication, error) {
	var out *Publication
	e := s.withStateTx(func(tx *sql.Tx) error {
		topic, e := scanTopic(tx.QueryRow(`SELECT arn,name,account_id,attributes,created_at FROM topics WHERE arn=?`, topicARN))
		if e != nil {
			return e
		}
		fifo := topic.Attributes["FifoTopic"] == "true"
		if fifo && !validFIFOIdentifier(entry.MessageGroupID) || entry.MessageGroupID != "" && !validFIFOIdentifier(entry.MessageGroupID) {
			return invalidSNS("invalid or missing MessageGroupId")
		}
		if !fifo && entry.MessageDeduplicationID != "" {
			return invalidSNS("MessageDeduplicationId requires FIFO topic")
		}
		publication := Publication{MessageID: randomID(16), MessageDeduplicationID: entry.MessageDeduplicationID}
		scope := "Topic"
		dedup := entry.MessageDeduplicationID
		if fifo {
			if dedup == "" && topic.Attributes["ContentBasedDeduplication"] == "true" {
				h := sha256.Sum256([]byte(entry.Message))
				dedup = hex.EncodeToString(h[:])
			}
			if !validFIFOIdentifier(dedup) {
				return invalidSNS("MessageDeduplicationId required without content-based deduplication")
			}
			publication.MessageDeduplicationID = dedup
			if topic.Attributes["FifoThroughputScope"] == "MessageGroup" {
				scope = "MessageGroup:" + entry.MessageGroupID
			}
			var raw []byte
			var expiry int64
			e = tx.QueryRow(`SELECT document_json,expires_at FROM sns_publish_dedup WHERE topic_arn=? AND scope=? AND dedup_id=?`, topicARN, scope, dedup).Scan(&raw, &expiry)
			if e == nil && now.UnixNano() < expiry {
				if e = json.Unmarshal(raw, &publication); e != nil {
					return e
				}
				publication.Duplicate = true
				out = &publication
				return nil
			}
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
			var sequence int64
			e = tx.QueryRow(`SELECT next_sequence FROM sns_publish_sequences WHERE topic_arn=?`, topicARN).Scan(&sequence)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
			sequence++
			publication.SequenceNumber = fmt.Sprintf("%020d", sequence)
			if _, e = tx.Exec(`INSERT INTO sns_publish_sequences VALUES (?,?) ON CONFLICT(topic_arn) DO UPDATE SET next_sequence=excluded.next_sequence`, topicARN, sequence); e != nil {
				return e
			}
		}
		rows, e := tx.Query(`SELECT arn,topic_arn,protocol,endpoint,account_id,confirmed FROM subscriptions WHERE topic_arn=? ORDER BY arn`, topicARN)
		if e != nil {
			return e
		}
		subs := []Subscription{}
		for rows.Next() {
			var v Subscription
			if e = rows.Scan(&v.ARN, &v.TopicARN, &v.Protocol, &v.Endpoint, &v.AccountID, &v.Confirmed); e != nil {
				_ = rows.Close()
				return e
			}
			subs = append(subs, v)
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if e = deliver(publication, subs); e != nil {
			return e
		}
		if fifo {
			raw, e := json.Marshal(publication)
			if e != nil {
				return e
			}
			if _, e = tx.Exec(`INSERT INTO sns_publish_dedup VALUES (?,?,?,?,?) ON CONFLICT(topic_arn,scope,dedup_id) DO UPDATE SET document_json=excluded.document_json,expires_at=excluded.expires_at`, topicARN, scope, dedup, raw, now.Add(5*time.Minute).UnixNano()); e != nil {
				return e
			}
		}
		out = &publication
		return nil
	})
	return out, e
}
