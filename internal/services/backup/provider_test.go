// SPDX-License-Identifier: Apache-2.0

// internal/services/backup/provider_test.go
package backup

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

// ========== TestBackupPlanCRUD ==========

func TestBackupPlanCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"BackupPlan":{"BackupPlanName":"my-plan","Rules":[]}}`
	resp := call(t, p, "PUT", "/backup/plans", "CreateBackupPlan", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	planID, ok := rb["BackupPlanId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, planID)
	planARN, _ := rb["BackupPlanArn"].(string)
	assert.NotEmpty(t, planARN)

	// Duplicate create
	resp2 := call(t, p, "PUT", "/backup/plans", "CreateBackupPlan", body)
	// Same name but different ID, so should succeed (no unique name constraint)
	assert.Equal(t, 200, resp2.StatusCode)

	// Get
	resp3 := call(t, p, "GET", "/backup/plans/"+planID, "GetBackupPlan", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	bp, ok := rb3["BackupPlan"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-plan", bp["BackupPlanName"])

	// Get not found
	resp4 := call(t, p, "GET", "/backup/plans/nonexistent-id", "GetBackupPlan", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// List
	resp5 := call(t, p, "GET", "/backup/plans", "ListBackupPlans", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	list, ok := rb5["BackupPlansList"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(list), 1)

	// Update
	updateBody := `{"BackupPlan":{"BackupPlanName":"my-plan-updated","Rules":[]}}`
	resp6 := call(t, p, "POST", "/backup/plans/"+planID, "UpdateBackupPlan", updateBody)
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify update
	resp7 := call(t, p, "GET", "/backup/plans/"+planID, "GetBackupPlan", "")
	rb7 := parseBody(t, resp7)
	bp7, _ := rb7["BackupPlan"].(map[string]any)
	assert.Equal(t, "my-plan-updated", bp7["BackupPlanName"])

	// ListVersions
	resp8 := call(t, p, "GET", "/backup/plans/"+planID+"/versions", "ListBackupPlanVersions", "")
	assert.Equal(t, 200, resp8.StatusCode)
	rb8 := parseBody(t, resp8)
	versions, _ := rb8["BackupPlanVersionsList"].([]any)
	assert.Len(t, versions, 1)

	// Export template
	resp9 := call(t, p, "GET", "/backup/plans/"+planID+"/toTemplate", "ExportBackupPlanTemplate", "")
	assert.Equal(t, 200, resp9.StatusCode)
	rb9 := parseBody(t, resp9)
	assert.NotEmpty(t, rb9["BackupPlanDocument"])

	// GetBackupPlanFromJSON
	docBody := `{"BackupPlanDocument":"{\"BackupPlanName\":\"test\",\"Rules\":[]}"}`
	resp10 := call(t, p, "POST", "/backup/template/json/toPlan", "GetBackupPlanFromJSON", docBody)
	assert.Equal(t, 200, resp10.StatusCode)

	// Delete
	resp11 := call(t, p, "DELETE", "/backup/plans/"+planID, "DeleteBackupPlan", "")
	assert.Equal(t, 200, resp11.StatusCode)

	// Get after delete should 404
	resp12 := call(t, p, "GET", "/backup/plans/"+planID, "GetBackupPlan", "")
	assert.Equal(t, 404, resp12.StatusCode)

	// Delete not found
	resp13 := call(t, p, "DELETE", "/backup/plans/nonexistent", "DeleteBackupPlan", "")
	assert.Equal(t, 404, resp13.StatusCode)
}

// ========== TestBackupVaultCRUD ==========

func TestBackupVaultCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "PUT", "/backup-vaults/my-vault", "CreateBackupVault",
		`{"EncryptionKeyArn":"arn:aws:kms:us-east-1:000000000000:key/test-key"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-vault", rb["BackupVaultName"])
	assert.NotEmpty(t, rb["BackupVaultArn"])

	// Duplicate create
	resp2 := call(t, p, "PUT", "/backup-vaults/my-vault", "CreateBackupVault", `{}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := call(t, p, "GET", "/backup-vaults/my-vault", "DescribeBackupVault", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, "my-vault", rb3["BackupVaultName"])

	// Describe not found
	resp4 := call(t, p, "GET", "/backup-vaults/nonexistent", "DescribeBackupVault", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// List
	call(t, p, "PUT", "/backup-vaults/vault-b", "CreateBackupVault", `{}`)
	resp5 := call(t, p, "GET", "/backup-vaults", "ListBackupVaults", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	vaultList, _ := rb5["BackupVaultList"].([]any)
	assert.Len(t, vaultList, 2)

	// Access policy
	policyBody := `{"Policy":"{\"Version\":\"2012-10-17\"}"}`
	respAP := call(t, p, "PUT", "/backup-vaults/my-vault/access-policy", "PutBackupVaultAccessPolicy", policyBody)
	assert.Equal(t, 200, respAP.StatusCode)

	respGetAP := call(t, p, "GET", "/backup-vaults/my-vault/access-policy", "GetBackupVaultAccessPolicy", "")
	assert.Equal(t, 200, respGetAP.StatusCode)
	rbAP := parseBody(t, respGetAP)
	assert.Contains(t, rbAP["Policy"].(string), "2012-10-17")

	respDelAP := call(t, p, "DELETE", "/backup-vaults/my-vault/access-policy", "DeleteBackupVaultAccessPolicy", "")
	assert.Equal(t, 200, respDelAP.StatusCode)

	// Notifications
	notifBody := `{"SNSTopicArn":"arn:aws:sns:us-east-1:000000000000:test","BackupVaultEvents":["BACKUP_JOB_STARTED"]}`
	respPN := call(t, p, "PUT", "/backup-vaults/my-vault/notification-configuration", "PutBackupVaultNotifications", notifBody)
	assert.Equal(t, 200, respPN.StatusCode)

	respGN := call(t, p, "GET", "/backup-vaults/my-vault/notification-configuration", "GetBackupVaultNotifications", "")
	assert.Equal(t, 200, respGN.StatusCode)
	rbGN := parseBody(t, respGN)
	assert.Equal(t, "my-vault", rbGN["BackupVaultName"])

	respDN := call(t, p, "DELETE", "/backup-vaults/my-vault/notification-configuration", "DeleteBackupVaultNotifications", "")
	assert.Equal(t, 200, respDN.StatusCode)

	// Lock config
	lockBody := `{"MinRetentionDays":7,"MaxRetentionDays":365}`
	respLC := call(t, p, "PUT", "/backup-vaults/my-vault/vault-lock", "PutBackupVaultLockConfiguration", lockBody)
	assert.Equal(t, 200, respLC.StatusCode)

	respDLC := call(t, p, "DELETE", "/backup-vaults/my-vault/vault-lock", "DeleteBackupVaultLockConfiguration", "")
	assert.Equal(t, 200, respDLC.StatusCode)

	// Delete
	respDel := call(t, p, "DELETE", "/backup-vaults/my-vault", "DeleteBackupVault", "")
	assert.Equal(t, 200, respDel.StatusCode)

	// Describe after delete
	respAfterDel := call(t, p, "GET", "/backup-vaults/my-vault", "DescribeBackupVault", "")
	assert.Equal(t, 404, respAfterDel.StatusCode)

	// Delete not found
	respDelNF := call(t, p, "DELETE", "/backup-vaults/nonexistent", "DeleteBackupVault", "")
	assert.Equal(t, 404, respDelNF.StatusCode)
}

// ========== TestBackupSelectionCRUD ==========

func TestBackupSelectionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create a plan first
	planResp := call(t, p, "PUT", "/backup/plans", "CreateBackupPlan", `{"BackupPlan":{"BackupPlanName":"sel-plan","Rules":[]}}`)
	require.Equal(t, 200, planResp.StatusCode)
	planRB := parseBody(t, planResp)
	planID := planRB["BackupPlanId"].(string)

	// Create selection
	selBody := `{"BackupSelection":{"SelectionName":"my-selection","IamRoleArn":"arn:aws:iam::000000000000:role/backup-role","Resources":["arn:aws:s3:::my-bucket"]}}`
	resp := call(t, p, "PUT", "/backup/plans/"+planID+"/selections", "CreateBackupSelection", selBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	selID, ok := rb["SelectionId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, selID)
	assert.Equal(t, planID, rb["BackupPlanId"])

	// Get selection
	resp2 := call(t, p, "GET", "/backup/plans/"+planID+"/selections/"+selID, "GetBackupSelection", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	bs, ok := rb2["BackupSelection"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-selection", bs["SelectionName"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/backup-role", bs["IamRoleArn"])

	// List selections
	resp3 := call(t, p, "GET", "/backup/plans/"+planID+"/selections", "ListBackupSelections", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, _ := rb3["BackupSelectionsList"].([]any)
	assert.Len(t, list, 1)

	// Delete selection
	resp4 := call(t, p, "DELETE", "/backup/plans/"+planID+"/selections/"+selID, "DeleteBackupSelection", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Get after delete
	resp5 := call(t, p, "GET", "/backup/plans/"+planID+"/selections/"+selID, "GetBackupSelection", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

// ========== TestBackupJobCRUD ==========

func TestBackupJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create vault first
	call(t, p, "PUT", "/backup-vaults/job-vault", "CreateBackupVault", `{}`)

	// Start backup job
	jobBody := `{"BackupVaultName":"job-vault","ResourceArn":"arn:aws:s3:::test-bucket","ResourceType":"S3"}`
	resp := call(t, p, "PUT", "/backup-jobs", "StartBackupJob", jobBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	jobID, ok := rb["BackupJobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)
	rpARN, _ := rb["RecoveryPointArn"].(string)
	assert.NotEmpty(t, rpARN)

	// Describe backup job
	resp2 := call(t, p, "GET", "/backup-jobs/"+jobID, "DescribeBackupJob", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, jobID, rb2["BackupJobId"])
	assert.Equal(t, "job-vault", rb2["BackupVaultName"])
	assert.Equal(t, "COMPLETED", rb2["State"])

	// Describe not found
	resp3 := call(t, p, "GET", "/backup-jobs/nonexistent", "DescribeBackupJob", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List backup jobs
	resp4 := call(t, p, "GET", "/backup-jobs", "ListBackupJobs", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	jobs, _ := rb4["BackupJobs"].([]any)
	assert.GreaterOrEqual(t, len(jobs), 1)

	// Stop backup job
	resp5 := call(t, p, "POST", "/backup-jobs/"+jobID, "StopBackupJob", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Describe recovery point
	resp6 := call(t, p, "GET", "/backup-vaults/job-vault/recovery-points/"+rpARN, "DescribeRecoveryPoint", "")
	assert.Equal(t, 200, resp6.StatusCode)
	rb6 := parseBody(t, resp6)
	assert.Equal(t, rpARN, rb6["RecoveryPointArn"])

	// List recovery points by vault
	resp7 := call(t, p, "GET", "/backup-vaults/job-vault/recovery-points", "ListRecoveryPointsByBackupVault", "")
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	rps, _ := rb7["RecoveryPoints"].([]any)
	assert.Len(t, rps, 1)

	// List recovery points by resource
	resp8 := call(t, p, "GET", "/resources/arn:aws:s3:::test-bucket/recovery-points", "ListRecoveryPointsByResource", "")
	assert.Equal(t, 200, resp8.StatusCode)

	// Update recovery point lifecycle
	resp9 := call(t, p, "POST", "/backup-vaults/job-vault/recovery-points/"+rpARN, "UpdateRecoveryPointLifecycle", `{"Lifecycle":{"DeleteAfterDays":90}}`)
	assert.Equal(t, 200, resp9.StatusCode)

	// Delete recovery point
	resp10 := call(t, p, "DELETE", "/backup-vaults/job-vault/recovery-points/"+rpARN, "DeleteRecoveryPoint", "")
	assert.Equal(t, 200, resp10.StatusCode)

	// Describe recovery point after delete
	resp11 := call(t, p, "GET", "/backup-vaults/job-vault/recovery-points/"+rpARN, "DescribeRecoveryPoint", "")
	assert.Equal(t, 404, resp11.StatusCode)
}

// ========== TestRestoreJobCRUD ==========

func TestRestoreJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create vault and backup job first to get a recovery point
	call(t, p, "PUT", "/backup-vaults/restore-vault", "CreateBackupVault", `{}`)
	jobResp := call(t, p, "PUT", "/backup-jobs", "StartBackupJob",
		`{"BackupVaultName":"restore-vault","ResourceArn":"arn:aws:ec2:us-east-1:000000000000:instance/i-1234","ResourceType":"EC2"}`)
	jobRB := parseBody(t, jobResp)
	rpARN := jobRB["RecoveryPointArn"].(string)

	// Start restore job
	restoreBody, _ := json.Marshal(map[string]any{
		"RecoveryPointArn": rpARN,
		"Metadata":         map[string]string{},
		"IamRoleArn":       "arn:aws:iam::000000000000:role/restore-role",
		"ResourceType":     "EC2",
	})
	resp := call(t, p, "PUT", "/restore-jobs", "StartRestoreJob", string(restoreBody))
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	restoreJobID, ok := rb["RestoreJobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, restoreJobID)

	// Describe restore job
	resp2 := call(t, p, "GET", "/restore-jobs/"+restoreJobID, "DescribeRestoreJob", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, restoreJobID, rb2["RestoreJobId"])
	assert.Equal(t, "COMPLETED", rb2["Status"])
	assert.Equal(t, rpARN, rb2["RecoveryPointArn"])

	// Describe not found
	resp3 := call(t, p, "GET", "/restore-jobs/nonexistent", "DescribeRestoreJob", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List restore jobs
	resp4 := call(t, p, "GET", "/restore-jobs", "ListRestoreJobs", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	rjList, _ := rb4["RestoreJobs"].([]any)
	assert.Len(t, rjList, 1)
}

// ========== TestFrameworkCRUD ==========

func TestFrameworkCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	fwBody := `{"FrameworkName":"my-framework","FrameworkDescription":"test framework","FrameworkControls":[]}`
	resp := call(t, p, "POST", "/audit/frameworks", "CreateFramework", fwBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-framework", rb["FrameworkName"])
	assert.NotEmpty(t, rb["FrameworkArn"])

	// Duplicate
	resp2 := call(t, p, "POST", "/audit/frameworks", "CreateFramework", fwBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := call(t, p, "GET", "/audit/frameworks/my-framework", "DescribeFramework", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, "my-framework", rb3["FrameworkName"])
	assert.Equal(t, "test framework", rb3["FrameworkDescription"])

	// Describe not found
	resp4 := call(t, p, "GET", "/audit/frameworks/nonexistent", "DescribeFramework", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// List
	call(t, p, "POST", "/audit/frameworks", "CreateFramework", `{"FrameworkName":"fw-2","FrameworkControls":[]}`)
	resp5 := call(t, p, "GET", "/audit/frameworks", "ListFrameworks", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	fwList, _ := rb5["Frameworks"].([]any)
	assert.Len(t, fwList, 2)

	// Update
	updateBody := `{"FrameworkDescription":"updated description","FrameworkControls":[{"ControlName":"test"}]}`
	resp6 := call(t, p, "PUT", "/audit/frameworks/my-framework", "UpdateFramework", updateBody)
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify update
	resp7 := call(t, p, "GET", "/audit/frameworks/my-framework", "DescribeFramework", "")
	rb7 := parseBody(t, resp7)
	assert.Equal(t, "updated description", rb7["FrameworkDescription"])

	// Delete
	resp8 := call(t, p, "DELETE", "/audit/frameworks/my-framework", "DeleteFramework", "")
	assert.Equal(t, 200, resp8.StatusCode)

	// Describe after delete
	resp9 := call(t, p, "GET", "/audit/frameworks/my-framework", "DescribeFramework", "")
	assert.Equal(t, 404, resp9.StatusCode)

	// Delete not found
	resp10 := call(t, p, "DELETE", "/audit/frameworks/nonexistent", "DeleteFramework", "")
	assert.Equal(t, 404, resp10.StatusCode)
}

// ========== TestReportPlanCRUD ==========

func TestReportPlanCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	rpBody := `{"ReportPlanName":"my-report-plan","ReportPlanDescription":"test","ReportDeliveryChannel":{"S3BucketName":"my-bucket"},"ReportSetting":{"ReportTemplate":"BACKUP_JOB_REPORT"}}`
	resp := call(t, p, "POST", "/audit/report-plans", "CreateReportPlan", rpBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-report-plan", rb["ReportPlanName"])
	assert.NotEmpty(t, rb["ReportPlanArn"])

	// Duplicate
	resp2 := call(t, p, "POST", "/audit/report-plans", "CreateReportPlan", rpBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := call(t, p, "GET", "/audit/report-plans/my-report-plan", "DescribeReportPlan", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	rp, ok := rb3["ReportPlan"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-report-plan", rp["ReportPlanName"])

	// Describe not found
	resp4 := call(t, p, "GET", "/audit/report-plans/nonexistent", "DescribeReportPlan", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// List
	call(t, p, "POST", "/audit/report-plans", "CreateReportPlan", `{"ReportPlanName":"rp-2","ReportSetting":{"ReportTemplate":"BACKUP_JOB_REPORT"}}`)
	resp5 := call(t, p, "GET", "/audit/report-plans", "ListReportPlans", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	rpList, _ := rb5["ReportPlans"].([]any)
	assert.Len(t, rpList, 2)

	// Update
	updateBody := `{"ReportPlanDescription":"updated description"}`
	resp6 := call(t, p, "PUT", "/audit/report-plans/my-report-plan", "UpdateReportPlan", updateBody)
	assert.Equal(t, 200, resp6.StatusCode)

	// Start report job
	resp7 := call(t, p, "POST", "/audit/report-jobs/my-report-plan", "StartReportJob", "")
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	reportJobID, ok := rb7["ReportJobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, reportJobID)

	// Describe report job
	resp8 := call(t, p, "GET", "/audit/report-jobs/"+reportJobID, "DescribeReportJob", "")
	assert.Equal(t, 200, resp8.StatusCode)
	rb8 := parseBody(t, resp8)
	rj, ok := rb8["ReportJob"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "COMPLETED", rj["Status"])

	// List report jobs
	resp9 := call(t, p, "GET", "/audit/report-jobs", "ListReportJobs", "")
	assert.Equal(t, 200, resp9.StatusCode)
	rb9 := parseBody(t, resp9)
	rjList, _ := rb9["ReportJobs"].([]any)
	assert.Len(t, rjList, 1)

	// Delete
	resp10 := call(t, p, "DELETE", "/audit/report-plans/my-report-plan", "DeleteReportPlan", "")
	assert.Equal(t, 200, resp10.StatusCode)

	// Describe after delete
	resp11 := call(t, p, "GET", "/audit/report-plans/my-report-plan", "DescribeReportPlan", "")
	assert.Equal(t, 404, resp11.StatusCode)
}

// ========== TestRestoreTestingPlanCRUD ==========

func TestRestoreTestingPlanCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	planBody := `{"RestoreTestingPlan":{"RestoreTestingPlanName":"my-rtp","ScheduleExpression":"cron(0 12 * * ? *)","StartWindowHours":2,"RecoveryPointSelection":{"Algorithm":"RANDOM_WITHIN_WINDOW"}}}`
	resp := call(t, p, "PUT", "/restore-testing/plans", "CreateRestoreTestingPlan", planBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-rtp", rb["RestoreTestingPlanName"])
	assert.NotEmpty(t, rb["RestoreTestingPlanArn"])

	// Duplicate
	resp2 := call(t, p, "PUT", "/restore-testing/plans", "CreateRestoreTestingPlan", planBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Get
	resp3 := call(t, p, "GET", "/restore-testing/plans/my-rtp", "GetRestoreTestingPlan", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	rtPlan, ok := rb3["RestoreTestingPlan"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-rtp", rtPlan["RestoreTestingPlanName"])
	assert.Equal(t, "cron(0 12 * * ? *)", rtPlan["ScheduleExpression"])
	assert.Equal(t, float64(2), rtPlan["StartWindowHours"])

	// Get not found
	resp4 := call(t, p, "GET", "/restore-testing/plans/nonexistent", "GetRestoreTestingPlan", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// List
	call(t, p, "PUT", "/restore-testing/plans", "CreateRestoreTestingPlan", `{"RestoreTestingPlan":{"RestoreTestingPlanName":"rtp-2","ScheduleExpression":"rate(1 day)"}}`)
	resp5 := call(t, p, "GET", "/restore-testing/plans", "ListRestoreTestingPlans", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	rtpList, _ := rb5["RestoreTestingPlans"].([]any)
	assert.Len(t, rtpList, 2)

	// Update
	updateBody := `{"RestoreTestingPlan":{"ScheduleExpression":"rate(7 days)","StartWindowHours":4}}`
	resp6 := call(t, p, "PUT", "/restore-testing/plans/my-rtp", "UpdateRestoreTestingPlan", updateBody)
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify update
	resp7 := call(t, p, "GET", "/restore-testing/plans/my-rtp", "GetRestoreTestingPlan", "")
	rb7 := parseBody(t, resp7)
	rtPlan7, _ := rb7["RestoreTestingPlan"].(map[string]any)
	assert.Equal(t, "rate(7 days)", rtPlan7["ScheduleExpression"])
	assert.Equal(t, float64(4), rtPlan7["StartWindowHours"])

	// Create selection
	selBody := `{"RestoreTestingSelection":{"RestoreTestingSelectionName":"my-sel","IamRoleArn":"arn:aws:iam::000000000000:role/restore-role","ProtectedResourceType":"S3"}}`
	resp8 := call(t, p, "PUT", "/restore-testing/plans/my-rtp/selections", "CreateRestoreTestingSelection", selBody)
	assert.Equal(t, 200, resp8.StatusCode)
	rb8 := parseBody(t, resp8)
	assert.Equal(t, "my-sel", rb8["RestoreTestingSelectionName"])

	// Get selection
	resp9 := call(t, p, "GET", "/restore-testing/plans/my-rtp/selections/my-sel", "GetRestoreTestingSelection", "")
	assert.Equal(t, 200, resp9.StatusCode)
	rb9 := parseBody(t, resp9)
	sel, ok := rb9["RestoreTestingSelection"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-sel", sel["RestoreTestingSelectionName"])
	assert.Equal(t, "S3", sel["ProtectedResourceType"])

	// List selections
	resp10 := call(t, p, "GET", "/restore-testing/plans/my-rtp/selections", "ListRestoreTestingSelections", "")
	assert.Equal(t, 200, resp10.StatusCode)
	rb10 := parseBody(t, resp10)
	selList, _ := rb10["RestoreTestingSelections"].([]any)
	assert.Len(t, selList, 1)

	// Update selection
	updateSelBody := `{"RestoreTestingSelection":{"IamRoleArn":"arn:aws:iam::000000000000:role/new-role","ProtectedResourceType":"EC2"}}`
	resp11 := call(t, p, "PUT", "/restore-testing/plans/my-rtp/selections/my-sel", "UpdateRestoreTestingSelection", updateSelBody)
	assert.Equal(t, 200, resp11.StatusCode)

	// Verify selection update
	resp12 := call(t, p, "GET", "/restore-testing/plans/my-rtp/selections/my-sel", "GetRestoreTestingSelection", "")
	rb12 := parseBody(t, resp12)
	sel12, _ := rb12["RestoreTestingSelection"].(map[string]any)
	assert.Equal(t, "EC2", sel12["ProtectedResourceType"])

	// Delete selection
	resp13 := call(t, p, "DELETE", "/restore-testing/plans/my-rtp/selections/my-sel", "DeleteRestoreTestingSelection", "")
	assert.Equal(t, 200, resp13.StatusCode)

	// Get selection after delete
	resp14 := call(t, p, "GET", "/restore-testing/plans/my-rtp/selections/my-sel", "GetRestoreTestingSelection", "")
	assert.Equal(t, 404, resp14.StatusCode)

	// Delete plan
	resp15 := call(t, p, "DELETE", "/restore-testing/plans/my-rtp", "DeleteRestoreTestingPlan", "")
	assert.Equal(t, 200, resp15.StatusCode)

	// Get after delete
	resp16 := call(t, p, "GET", "/restore-testing/plans/my-rtp", "GetRestoreTestingPlan", "")
	assert.Equal(t, 404, resp16.StatusCode)
}

// ========== TestTags ==========

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a vault to tag
	call(t, p, "PUT", "/backup-vaults/tag-vault", "CreateBackupVault", `{}`)
	descResp := call(t, p, "GET", "/backup-vaults/tag-vault", "DescribeBackupVault", "")
	descRB := parseBody(t, descResp)
	arn := descRB["BackupVaultArn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	resp := call(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, resp.StatusCode)

	// ListTags
	resp2 := call(t, p, "GET", "/tags/"+arn, "ListTags", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "platform", tags["Team"])

	// UntagResource
	req := httptest.NewRequest("POST", "/untag/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify 1 tag remains
	resp3 := call(t, p, "GET", "/tags/"+arn, "ListTags", "")
	rb3 := parseBody(t, resp3)
	tags3 := rb3["Tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "platform", tags3["Team"])

	// Test tag on plan
	planResp := call(t, p, "PUT", "/backup/plans", "CreateBackupPlan", `{"BackupPlanTags":{"Key":"value"},"BackupPlan":{"BackupPlanName":"tagged-plan","Rules":[]}}`)
	planRB := parseBody(t, planResp)
	planARN := planRB["BackupPlanArn"].(string)

	resp4 := call(t, p, "GET", "/tags/"+planARN, "ListTags", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	planTags, _ := rb4["Tags"].(map[string]any)
	assert.Equal(t, "value", planTags["Key"])

	// Stub ops return OK
	assert.Equal(t, 200, call(t, p, "GET", "/global-settings", "DescribeGlobalSettings", "").StatusCode)
	assert.Equal(t, 200, call(t, p, "PUT", "/global-settings", "UpdateGlobalSettings", `{}`).StatusCode)
	assert.Equal(t, 200, call(t, p, "GET", "/account-settings", "DescribeRegionSettings", "").StatusCode)
	assert.Equal(t, 200, call(t, p, "PUT", "/account-settings", "UpdateRegionSettings", `{}`).StatusCode)
	assert.Equal(t, 200, call(t, p, "GET", "/supported-resource-types", "GetSupportedResourceTypes", "").StatusCode)
	assert.Equal(t, 200, call(t, p, "GET", "/backup-jobs", "ListBackupJobSummaries", "").StatusCode)
	assert.Equal(t, 200, call(t, p, "GET", "/restore-jobs", "ListRestoreJobSummaries", "").StatusCode)
}
