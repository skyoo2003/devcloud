// SPDX-License-Identifier: Apache-2.0

// internal/services/sns/store.go
package sns

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrTopicNotFound        = errors.New("topic not found")
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS topics (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			account_id TEXT NOT NULL,
			attributes TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS subscriptions (
			arn        TEXT PRIMARY KEY,
			topic_arn  TEXT NOT NULL,
			protocol   TEXT NOT NULL,
			endpoint   TEXT NOT NULL,
			account_id TEXT NOT NULL,
			confirmed  INTEGER NOT NULL DEFAULT 0
		);
	`},
	{Version: 2, SQL: `
		ALTER TABLE subscriptions ADD COLUMN attributes TEXT NOT NULL DEFAULT '{}';
	`},
	{Version: 3, SQL: `
		CREATE TABLE IF NOT EXISTS topic_permissions (
			topic_arn    TEXT NOT NULL,
			statement_id TEXT NOT NULL,
			account_ids  TEXT NOT NULL,
			actions      TEXT NOT NULL,
			PRIMARY KEY (topic_arn, statement_id)
		);
		CREATE TABLE IF NOT EXISTS topic_tags (
			topic_arn TEXT NOT NULL,
			tag_key   TEXT NOT NULL,
			tag_value TEXT NOT NULL,
			PRIMARY KEY (topic_arn, tag_key)
		);
		CREATE TABLE IF NOT EXISTS sms_opt_outs (
			phone_number TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			PRIMARY KEY (phone_number, account_id)
		);
		CREATE TABLE IF NOT EXISTS topic_data_protection (
			topic_arn TEXT NOT NULL PRIMARY KEY,
			policy    TEXT NOT NULL
		);
	`},
}

type Topic struct {
	ARN        string
	Name       string
	AccountID  string
	Attributes map[string]string
	CreatedAt  time.Time
}

type Subscription struct {
	ARN       string
	TopicARN  string
	Protocol  string
	Endpoint  string
	AccountID string
	Confirmed bool
}

type SNSStore struct {
	store *sqlite.Store
}

func NewSNSStore(dataDir string) (*SNSStore, error) {
	dbPath := filepath.Join(dataDir, "sns.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &SNSStore{store: s}, nil
}

func (s *SNSStore) Close() error { return s.store.Close() }

func (s *SNSStore) CreateTopic(arn, name, accountID string) (*Topic, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO topics (arn, name, account_id, attributes, created_at) VALUES (?, ?, ?, '{}', ?)
		 ON CONFLICT(arn) DO NOTHING`,
		arn, name, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Topic{ARN: arn, Name: name, AccountID: accountID, Attributes: map[string]string{}, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *SNSStore) DeleteTopic(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM topics WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTopicNotFound
	}
	// cascade delete subscriptions
	_, _ = s.store.DB().Exec(`DELETE FROM subscriptions WHERE topic_arn = ?`, arn)
	return nil
}

func (s *SNSStore) GetTopic(arn string) (*Topic, error) {
	row := s.store.DB().QueryRow(`SELECT arn, name, account_id, attributes, created_at FROM topics WHERE arn = ?`, arn)
	return scanTopic(row)
}

func (s *SNSStore) ListTopics(accountID string) ([]Topic, error) {
	rows, err := s.store.DB().Query(`SELECT arn, name, account_id, attributes, created_at FROM topics WHERE account_id = ? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var topics []Topic
	for rows.Next() {
		t, err := scanTopic(rows)
		if err != nil {
			return nil, err
		}
		topics = append(topics, *t)
	}
	return topics, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTopic(s scanner) (*Topic, error) {
	var t Topic
	var attrJSON string
	var createdAt int64
	if err := s.Scan(&t.ARN, &t.Name, &t.AccountID, &attrJSON, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTopicNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	if err := json.Unmarshal([]byte(attrJSON), &t.Attributes); err != nil {
		t.Attributes = map[string]string{}
	}
	return &t, nil
}

func (s *SNSStore) Subscribe(arn, topicARN, protocol, endpoint, accountID string) (*Subscription, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO subscriptions (arn, topic_arn, protocol, endpoint, account_id, confirmed) VALUES (?, ?, ?, ?, ?, 0)`,
		arn, topicARN, protocol, endpoint, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &Subscription{ARN: arn, TopicARN: topicARN, Protocol: protocol, Endpoint: endpoint, AccountID: accountID}, nil
}

func (s *SNSStore) Unsubscribe(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM subscriptions WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSubscriptionNotFound
	}
	return nil
}

func (s *SNSStore) ListSubscriptions(accountID string) ([]Subscription, error) {
	rows, err := s.store.DB().Query(`SELECT arn, topic_arn, protocol, endpoint, account_id, confirmed FROM subscriptions WHERE account_id = ?`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

func (s *SNSStore) ListSubscriptionsByTopic(topicARN string) ([]Subscription, error) {
	rows, err := s.store.DB().Query(`SELECT arn, topic_arn, protocol, endpoint, account_id, confirmed FROM subscriptions WHERE topic_arn = ?`, topicARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

func (s *SNSStore) SetSubscriptionAttribute(arn, key, value string) error {
	// First get existing attributes
	var attrJSON string
	err := s.store.DB().QueryRow(`SELECT attributes FROM subscriptions WHERE arn = ?`, arn).Scan(&attrJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSubscriptionNotFound
		}
		return err
	}
	attrs := map[string]string{}
	if err := json.Unmarshal([]byte(attrJSON), &attrs); err != nil {
		attrs = map[string]string{}
	}
	attrs[key] = value
	newJSON, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	_, err = s.store.DB().Exec(`UPDATE subscriptions SET attributes = ? WHERE arn = ?`, string(newJSON), arn)
	return err
}

func scanSubscriptions(rows *sql.Rows) ([]Subscription, error) {
	var subs []Subscription
	for rows.Next() {
		var sub Subscription
		var confirmed int
		if err := rows.Scan(&sub.ARN, &sub.TopicARN, &sub.Protocol, &sub.Endpoint, &sub.AccountID, &confirmed); err != nil {
			return nil, err
		}
		sub.Confirmed = confirmed == 1
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

// SetTopicAttribute merges a single key/value into the topic's attributes JSON.
func (s *SNSStore) SetTopicAttribute(arn, key, value string) error {
	var attrJSON string
	err := s.store.DB().QueryRow(`SELECT attributes FROM topics WHERE arn = ?`, arn).Scan(&attrJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTopicNotFound
		}
		return err
	}
	attrs := map[string]string{}
	if err := json.Unmarshal([]byte(attrJSON), &attrs); err != nil {
		attrs = map[string]string{}
	}
	attrs[key] = value
	newJSON, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	_, err = s.store.DB().Exec(`UPDATE topics SET attributes = ? WHERE arn = ?`, string(newJSON), arn)
	return err
}

// GetSubscriptionAttributes returns all attributes for a subscription.
func (s *SNSStore) GetSubscriptionAttributes(arn string) (map[string]string, *Subscription, error) {
	var sub Subscription
	var confirmed int
	var attrJSON string
	err := s.store.DB().QueryRow(
		`SELECT arn, topic_arn, protocol, endpoint, account_id, confirmed, attributes FROM subscriptions WHERE arn = ?`, arn,
	).Scan(&sub.ARN, &sub.TopicARN, &sub.Protocol, &sub.Endpoint, &sub.AccountID, &confirmed, &attrJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, ErrSubscriptionNotFound
		}
		return nil, nil, err
	}
	sub.Confirmed = confirmed == 1
	attrs := map[string]string{}
	if err := json.Unmarshal([]byte(attrJSON), &attrs); err != nil {
		attrs = map[string]string{}
	}
	return attrs, &sub, nil
}

// AddPermission inserts or replaces a permission statement on a topic.
func (s *SNSStore) AddPermission(topicARN, statementID string, accountIDs, actions []string) error {
	aJSON, err := json.Marshal(accountIDs)
	if err != nil {
		return err
	}
	acJSON, err := json.Marshal(actions)
	if err != nil {
		return err
	}
	_, err = s.store.DB().Exec(
		`INSERT OR REPLACE INTO topic_permissions (topic_arn, statement_id, account_ids, actions) VALUES (?, ?, ?, ?)`,
		topicARN, statementID, string(aJSON), string(acJSON),
	)
	return err
}

// RemovePermission deletes a permission statement from a topic.
func (s *SNSStore) RemovePermission(topicARN, statementID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM topic_permissions WHERE topic_arn = ? AND statement_id = ?`,
		topicARN, statementID,
	)
	return err
}

// TagResource sets (upsert) tags on a topic.
func (s *SNSStore) TagResource(topicARN string, tags map[string]string) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for k, v := range tags {
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO topic_tags (topic_arn, tag_key, tag_value) VALUES (?, ?, ?)`,
			topicARN, k, v,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UntagResource removes tags from a topic.
func (s *SNSStore) UntagResource(topicARN string, keys []string) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, k := range keys {
		if _, err := tx.Exec(
			`DELETE FROM topic_tags WHERE topic_arn = ? AND tag_key = ?`,
			topicARN, k,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListTagsForResource returns all tags for a topic.
func (s *SNSStore) ListTagsForResource(topicARN string) (map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT tag_key, tag_value FROM topic_tags WHERE topic_arn = ?`, topicARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// PutDataProtectionPolicy stores a data protection policy for a topic.
func (s *SNSStore) PutDataProtectionPolicy(topicARN, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO topic_data_protection (topic_arn, policy) VALUES (?, ?)`,
		topicARN, policy,
	)
	return err
}

// GetDataProtectionPolicy retrieves the data protection policy for a topic.
func (s *SNSStore) GetDataProtectionPolicy(topicARN string) (string, error) {
	var policy string
	err := s.store.DB().QueryRow(`SELECT policy FROM topic_data_protection WHERE topic_arn = ?`, topicARN).Scan(&policy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return policy, nil
}

// ConfirmSubscription marks a subscription as confirmed.
func (s *SNSStore) ConfirmSubscription(arn string) error {
	_, err := s.store.DB().Exec(`UPDATE subscriptions SET confirmed = 1 WHERE arn = ?`, arn)
	return err
}
