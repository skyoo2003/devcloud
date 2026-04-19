// SPDX-License-Identifier: Apache-2.0

// internal/services/codecommit/store.go
package codecommit

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errRepositoryNotFound           = errors.New("repository not found")
	errBranchNotFound               = errors.New("branch not found")
	errPullRequestNotFound          = errors.New("pull request not found")
	errApprovalRuleTemplateNotFound = errors.New("approval rule template not found")
	errCommentNotFound              = errors.New("comment not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS repositories (
			name           TEXT PRIMARY KEY,
			id             TEXT NOT NULL UNIQUE,
			arn            TEXT NOT NULL UNIQUE,
			description    TEXT NOT NULL DEFAULT '',
			default_branch TEXT NOT NULL DEFAULT 'main',
			clone_url      TEXT NOT NULL DEFAULT '',
			created_at     INTEGER NOT NULL,
			updated_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS branches (
			repo_name  TEXT NOT NULL,
			name       TEXT NOT NULL,
			commit_id  TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (repo_name, name)
		);
		CREATE TABLE IF NOT EXISTS pull_requests (
			id            TEXT PRIMARY KEY,
			title         TEXT NOT NULL DEFAULT '',
			description   TEXT NOT NULL DEFAULT '',
			status        TEXT NOT NULL DEFAULT 'OPEN',
			source_repo   TEXT NOT NULL DEFAULT '',
			source_branch TEXT NOT NULL DEFAULT '',
			dest_repo     TEXT NOT NULL DEFAULT '',
			dest_branch   TEXT NOT NULL DEFAULT '',
			author        TEXT NOT NULL DEFAULT '',
			created_at    INTEGER NOT NULL,
			updated_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS approval_rule_templates (
			name        TEXT PRIMARY KEY,
			id          TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			content     TEXT NOT NULL DEFAULT '{}',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS comments (
			id          TEXT PRIMARY KEY,
			content     TEXT NOT NULL DEFAULT '',
			author      TEXT NOT NULL DEFAULT '',
			in_reply_to TEXT NOT NULL DEFAULT '',
			deleted     INTEGER NOT NULL DEFAULT 0,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS repo_template_associations (
			repo_name     TEXT NOT NULL,
			template_name TEXT NOT NULL,
			PRIMARY KEY (repo_name, template_name)
		);
		CREATE TABLE IF NOT EXISTS pr_approval_rules (
			pr_id    TEXT NOT NULL,
			name     TEXT NOT NULL,
			content  TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (pr_id, name)
		);
		CREATE TABLE IF NOT EXISTS pr_approvals (
			pr_id     TEXT NOT NULL,
			user_arn  TEXT NOT NULL,
			state     TEXT NOT NULL DEFAULT 'APPROVE',
			PRIMARY KEY (pr_id, user_arn)
		);
		CREATE TABLE IF NOT EXISTS pr_override (
			pr_id      TEXT PRIMARY KEY,
			overridden INTEGER NOT NULL DEFAULT 0,
			overrider  TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS comment_reactions (
			comment_id TEXT NOT NULL,
			user_arn   TEXT NOT NULL,
			emoji      TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (comment_id, user_arn)
		);
	`},
}

// ---- model structs ----

type Repository struct {
	Name          string
	ID            string
	ARN           string
	Description   string
	DefaultBranch string
	CloneURL      string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Branch struct {
	RepoName string
	Name     string
	CommitID string
}

type PullRequest struct {
	ID           string
	Title        string
	Description  string
	Status       string
	SourceRepo   string
	SourceBranch string
	DestRepo     string
	DestBranch   string
	Author       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type ApprovalRuleTemplate struct {
	Name        string
	ID          string
	Description string
	Content     string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Comment struct {
	ID        string
	Content   string
	Author    string
	InReplyTo string
	Deleted   bool
	CreatedAt time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "codecommit.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Repository ----

func (s *Store) CreateRepository(name, id, arn, description, cloneURL string) (*Repository, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO repositories (name, id, arn, description, clone_url, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, id, arn, description, cloneURL, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Repository{
		Name: name, ID: id, ARN: arn, Description: description,
		DefaultBranch: "main", CloneURL: cloneURL,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetRepository(name string) (*Repository, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, arn, description, default_branch, clone_url, created_at, updated_at
		 FROM repositories WHERE name = ?`, name)
	return scanRepository(row)
}

func (s *Store) GetRepositoryByID(id string) (*Repository, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, arn, description, default_branch, clone_url, created_at, updated_at
		 FROM repositories WHERE id = ?`, id)
	return scanRepository(row)
}

func (s *Store) ListRepositories() ([]Repository, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, id, arn, description, default_branch, clone_url, created_at, updated_at
		 FROM repositories ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var repos []Repository
	for rows.Next() {
		r, err := scanRepository(rows)
		if err != nil {
			return nil, err
		}
		repos = append(repos, *r)
	}
	return repos, rows.Err()
}

func (s *Store) UpdateRepositoryName(oldName, newName string) error {
	res, err := s.store.DB().Exec(`UPDATE repositories SET name = ?, updated_at = ? WHERE name = ?`,
		newName, time.Now().Unix(), oldName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

func (s *Store) UpdateRepositoryDescription(name, description string) error {
	res, err := s.store.DB().Exec(`UPDATE repositories SET description = ?, updated_at = ? WHERE name = ?`,
		description, time.Now().Unix(), name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

func (s *Store) UpdateDefaultBranch(repoName, branchName string) error {
	res, err := s.store.DB().Exec(`UPDATE repositories SET default_branch = ?, updated_at = ? WHERE name = ?`,
		branchName, time.Now().Unix(), repoName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

func (s *Store) DeleteRepository(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM repositories WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	// cascade-delete branches
	_, _ = s.store.DB().Exec(`DELETE FROM branches WHERE repo_name = ?`, name)
	return nil
}

// ---- Branch ----

func (s *Store) CreateBranch(repoName, branchName, commitID string) (*Branch, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO branches (repo_name, name, commit_id) VALUES (?, ?, ?)`,
		repoName, branchName, commitID,
	)
	if err != nil {
		return nil, err
	}
	return &Branch{RepoName: repoName, Name: branchName, CommitID: commitID}, nil
}

func (s *Store) GetBranch(repoName, branchName string) (*Branch, error) {
	row := s.store.DB().QueryRow(
		`SELECT repo_name, name, commit_id FROM branches WHERE repo_name = ? AND name = ?`,
		repoName, branchName)
	var b Branch
	if err := row.Scan(&b.RepoName, &b.Name, &b.CommitID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBranchNotFound
		}
		return nil, err
	}
	return &b, nil
}

func (s *Store) ListBranches(repoName string) ([]Branch, error) {
	rows, err := s.store.DB().Query(
		`SELECT repo_name, name, commit_id FROM branches WHERE repo_name = ? ORDER BY name`, repoName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var branches []Branch
	for rows.Next() {
		var b Branch
		if err := rows.Scan(&b.RepoName, &b.Name, &b.CommitID); err != nil {
			return nil, err
		}
		branches = append(branches, b)
	}
	return branches, rows.Err()
}

func (s *Store) DeleteBranch(repoName, branchName string) (*Branch, error) {
	b, err := s.GetBranch(repoName, branchName)
	if err != nil {
		return nil, err
	}
	_, err = s.store.DB().Exec(`DELETE FROM branches WHERE repo_name = ? AND name = ?`, repoName, branchName)
	return b, err
}

// ---- Pull Request ----

func (s *Store) CreatePullRequest(id, title, description, sourceRepo, sourceBranch, destRepo, destBranch, author string) (*PullRequest, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pull_requests (id, title, description, status, source_repo, source_branch, dest_repo, dest_branch, author, created_at, updated_at)
		 VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?)`,
		id, title, description, sourceRepo, sourceBranch, destRepo, destBranch, author, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &PullRequest{
		ID: id, Title: title, Description: description, Status: "OPEN",
		SourceRepo: sourceRepo, SourceBranch: sourceBranch,
		DestRepo: destRepo, DestBranch: destBranch, Author: author,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetPullRequest(id string) (*PullRequest, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, title, description, status, source_repo, source_branch, dest_repo, dest_branch, author, created_at, updated_at
		 FROM pull_requests WHERE id = ?`, id)
	return scanPullRequest(row)
}

func (s *Store) ListPullRequests(repoName, status, authorARN string) ([]PullRequest, error) {
	query := `SELECT id, title, description, status, source_repo, source_branch, dest_repo, dest_branch, author, created_at, updated_at
	          FROM pull_requests WHERE 1=1`
	var args []any
	if repoName != "" {
		query += ` AND (source_repo = ? OR dest_repo = ?)`
		args = append(args, repoName, repoName)
	}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	if authorARN != "" {
		query += ` AND author = ?`
		args = append(args, authorARN)
	}
	query += ` ORDER BY created_at DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var prs []PullRequest
	for rows.Next() {
		pr, err := scanPullRequest(rows)
		if err != nil {
			return nil, err
		}
		prs = append(prs, *pr)
	}
	return prs, rows.Err()
}

func (s *Store) UpdatePullRequestTitle(id, title string) (*PullRequest, error) {
	_, err := s.store.DB().Exec(`UPDATE pull_requests SET title = ?, updated_at = ? WHERE id = ?`,
		title, time.Now().Unix(), id)
	if err != nil {
		return nil, err
	}
	return s.GetPullRequest(id)
}

func (s *Store) UpdatePullRequestDescription(id, description string) (*PullRequest, error) {
	_, err := s.store.DB().Exec(`UPDATE pull_requests SET description = ?, updated_at = ? WHERE id = ?`,
		description, time.Now().Unix(), id)
	if err != nil {
		return nil, err
	}
	return s.GetPullRequest(id)
}

func (s *Store) UpdatePullRequestStatus(id, status string) (*PullRequest, error) {
	_, err := s.store.DB().Exec(`UPDATE pull_requests SET status = ?, updated_at = ? WHERE id = ?`,
		status, time.Now().Unix(), id)
	if err != nil {
		return nil, err
	}
	return s.GetPullRequest(id)
}

// ---- PR Approval Rules ----

func (s *Store) CreatePRApprovalRule(prID, name, content string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO pr_approval_rules (pr_id, name, content) VALUES (?, ?, ?)`,
		prID, name, content)
	return err
}

func (s *Store) DeletePRApprovalRule(prID, name string) error {
	_, err := s.store.DB().Exec(`DELETE FROM pr_approval_rules WHERE pr_id = ? AND name = ?`, prID, name)
	return err
}

func (s *Store) UpdatePRApprovalRuleContent(prID, name, content string) error {
	_, err := s.store.DB().Exec(`UPDATE pr_approval_rules SET content = ? WHERE pr_id = ? AND name = ?`,
		content, prID, name)
	return err
}

func (s *Store) ListPRApprovalRules(prID string) ([]map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT name, content FROM pr_approval_rules WHERE pr_id = ?`, prID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var rules []map[string]string
	for rows.Next() {
		var name, content string
		if err := rows.Scan(&name, &content); err != nil {
			return nil, err
		}
		rules = append(rules, map[string]string{"name": name, "content": content})
	}
	return rules, rows.Err()
}

// ---- PR Approvals ----

func (s *Store) SetApprovalState(prID, userARN, state string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO pr_approvals (pr_id, user_arn, state) VALUES (?, ?, ?)`,
		prID, userARN, state)
	return err
}

func (s *Store) GetApprovals(prID string) ([]map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT user_arn, state FROM pr_approvals WHERE pr_id = ?`, prID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var approvals []map[string]string
	for rows.Next() {
		var userARN, state string
		if err := rows.Scan(&userARN, &state); err != nil {
			return nil, err
		}
		approvals = append(approvals, map[string]string{"userArn": userARN, "approvalState": state})
	}
	return approvals, rows.Err()
}

// ---- PR Override ----

func (s *Store) SetOverride(prID, overrider string, overridden bool) error {
	val := 0
	if overridden {
		val = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO pr_override (pr_id, overridden, overrider) VALUES (?, ?, ?)`,
		prID, val, overrider)
	return err
}

func (s *Store) GetOverride(prID string) (bool, string) {
	row := s.store.DB().QueryRow(`SELECT overridden, overrider FROM pr_override WHERE pr_id = ?`, prID)
	var overridden int
	var overrider string
	_ = row.Scan(&overridden, &overrider)
	return overridden == 1, overrider
}

// ---- Approval Rule Templates ----

func (s *Store) CreateApprovalRuleTemplate(name, id, description, content string) (*ApprovalRuleTemplate, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO approval_rule_templates (name, id, description, content, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		name, id, description, content, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &ApprovalRuleTemplate{
		Name: name, ID: id, Description: description, Content: content,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetApprovalRuleTemplate(name string) (*ApprovalRuleTemplate, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, description, content, created_at, updated_at
		 FROM approval_rule_templates WHERE name = ?`, name)
	return scanApprovalRuleTemplate(row)
}

func (s *Store) ListApprovalRuleTemplates() ([]ApprovalRuleTemplate, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, id, description, content, created_at, updated_at
		 FROM approval_rule_templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var templates []ApprovalRuleTemplate
	for rows.Next() {
		t, err := scanApprovalRuleTemplate(rows)
		if err != nil {
			return nil, err
		}
		templates = append(templates, *t)
	}
	return templates, rows.Err()
}

func (s *Store) UpdateApprovalRuleTemplateName(oldName, newName string) (*ApprovalRuleTemplate, error) {
	_, err := s.store.DB().Exec(`UPDATE approval_rule_templates SET name = ?, updated_at = ? WHERE name = ?`,
		newName, time.Now().Unix(), oldName)
	if err != nil {
		return nil, err
	}
	return s.GetApprovalRuleTemplate(newName)
}

func (s *Store) UpdateApprovalRuleTemplateDescription(name, description string) (*ApprovalRuleTemplate, error) {
	_, err := s.store.DB().Exec(`UPDATE approval_rule_templates SET description = ?, updated_at = ? WHERE name = ?`,
		description, time.Now().Unix(), name)
	if err != nil {
		return nil, err
	}
	return s.GetApprovalRuleTemplate(name)
}

func (s *Store) UpdateApprovalRuleTemplateContent(name, content string) (*ApprovalRuleTemplate, error) {
	_, err := s.store.DB().Exec(`UPDATE approval_rule_templates SET content = ?, updated_at = ? WHERE name = ?`,
		content, time.Now().Unix(), name)
	if err != nil {
		return nil, err
	}
	return s.GetApprovalRuleTemplate(name)
}

func (s *Store) DeleteApprovalRuleTemplate(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM approval_rule_templates WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApprovalRuleTemplateNotFound
	}
	return nil
}

// ---- Template-Repository Associations ----

func (s *Store) AssociateTemplate(repoName, templateName string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO repo_template_associations (repo_name, template_name) VALUES (?, ?)`,
		repoName, templateName)
	return err
}

func (s *Store) DisassociateTemplate(repoName, templateName string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM repo_template_associations WHERE repo_name = ? AND template_name = ?`,
		repoName, templateName)
	return err
}

func (s *Store) ListTemplatesForRepo(repoName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT template_name FROM repo_template_associations WHERE repo_name = ? ORDER BY template_name`, repoName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

func (s *Store) ListReposForTemplate(templateName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT repo_name FROM repo_template_associations WHERE template_name = ? ORDER BY repo_name`, templateName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// ---- Comments ----

func (s *Store) CreateComment(id, content, author, inReplyTo string) (*Comment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO comments (id, content, author, in_reply_to, deleted, created_at) VALUES (?, ?, ?, ?, 0, ?)`,
		id, content, author, inReplyTo, now,
	)
	if err != nil {
		return nil, err
	}
	return &Comment{
		ID: id, Content: content, Author: author,
		InReplyTo: inReplyTo, Deleted: false, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetComment(id string) (*Comment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, content, author, in_reply_to, deleted, created_at FROM comments WHERE id = ?`, id)
	return scanComment(row)
}

func (s *Store) UpdateComment(id, content string) (*Comment, error) {
	_, err := s.store.DB().Exec(`UPDATE comments SET content = ? WHERE id = ? AND deleted = 0`, content, id)
	if err != nil {
		return nil, err
	}
	return s.GetComment(id)
}

func (s *Store) DeleteCommentContent(id string) (*Comment, error) {
	_, err := s.store.DB().Exec(`UPDATE comments SET content = '', deleted = 1 WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	return s.GetComment(id)
}

func (s *Store) ListComments() ([]Comment, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, content, author, in_reply_to, deleted, created_at FROM comments ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var comments []Comment
	for rows.Next() {
		c, err := scanComment(rows)
		if err != nil {
			return nil, err
		}
		comments = append(comments, *c)
	}
	return comments, rows.Err()
}

// ---- Comment Reactions ----

func (s *Store) PutCommentReaction(commentID, userARN, emoji string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO comment_reactions (comment_id, user_arn, emoji) VALUES (?, ?, ?)`,
		commentID, userARN, emoji)
	return err
}

func (s *Store) GetCommentReactions(commentID string) ([]map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT user_arn, emoji FROM comment_reactions WHERE comment_id = ?`, commentID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var reactions []map[string]string
	for rows.Next() {
		var userARN, emoji string
		if err := rows.Scan(&userARN, &emoji); err != nil {
			return nil, err
		}
		reactions = append(reactions, map[string]string{"userArn": userARN, "emoji": emoji})
	}
	return reactions, rows.Err()
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanRepository(s scanner) (*Repository, error) {
	var r Repository
	var createdAt, updatedAt int64
	err := s.Scan(&r.Name, &r.ID, &r.ARN, &r.Description, &r.DefaultBranch, &r.CloneURL, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRepositoryNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.UpdatedAt = time.Unix(updatedAt, 0)
	return &r, nil
}

func scanPullRequest(s scanner) (*PullRequest, error) {
	var pr PullRequest
	var createdAt, updatedAt int64
	err := s.Scan(&pr.ID, &pr.Title, &pr.Description, &pr.Status,
		&pr.SourceRepo, &pr.SourceBranch, &pr.DestRepo, &pr.DestBranch,
		&pr.Author, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPullRequestNotFound
		}
		return nil, err
	}
	pr.CreatedAt = time.Unix(createdAt, 0)
	pr.UpdatedAt = time.Unix(updatedAt, 0)
	return &pr, nil
}

func scanApprovalRuleTemplate(s scanner) (*ApprovalRuleTemplate, error) {
	var t ApprovalRuleTemplate
	var createdAt, updatedAt int64
	err := s.Scan(&t.Name, &t.ID, &t.Description, &t.Content, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errApprovalRuleTemplateNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	t.UpdatedAt = time.Unix(updatedAt, 0)
	return &t, nil
}

func scanComment(s scanner) (*Comment, error) {
	var c Comment
	var deleted int
	var createdAt int64
	err := s.Scan(&c.ID, &c.Content, &c.Author, &c.InReplyTo, &deleted, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errCommentNotFound
		}
		return nil, err
	}
	c.Deleted = deleted == 1
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
