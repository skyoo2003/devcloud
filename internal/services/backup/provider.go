// SPDX-License-Identifier: Apache-2.0

// internal/services/backup/provider.go
package backup

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

// Provider implements the CryoControllerUserManager (Backup) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "backup" }
func (p *Provider) ServiceName() string           { return "CryoControllerUserManager" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "backup"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}
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

	switch op {
	// --- BackupPlan ---
	case "CreateBackupPlan":
		return p.createBackupPlan(params)
	case "GetBackupPlan":
		id := pathSegment(req.URL.Path, "plans", 0)
		return p.getBackupPlan(id)
	case "ListBackupPlans":
		return p.listBackupPlans()
	case "UpdateBackupPlan":
		id := pathSegment(req.URL.Path, "plans", 0)
		return p.updateBackupPlan(id, params)
	case "DeleteBackupPlan":
		id := pathSegment(req.URL.Path, "plans", 0)
		return p.deleteBackupPlan(id)
	case "ListBackupPlanVersions":
		id := pathSegment(req.URL.Path, "plans", 0)
		return p.listBackupPlanVersions(id)
	case "ExportBackupPlanTemplate":
		id := pathSegment(req.URL.Path, "plans", 0)
		return p.exportBackupPlanTemplate(id)
	case "GetBackupPlanFromJSON":
		return p.getBackupPlanFromJSON(params)
	case "GetBackupPlanFromTemplate":
		return p.getBackupPlanFromTemplate(req.URL.Path)
	case "ListBackupPlanTemplates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BackupPlanTemplatesList": []any{}})

	// --- BackupVault ---
	case "CreateBackupVault":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.createBackupVault(name, params)
	case "DescribeBackupVault":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.describeBackupVault(name)
	case "ListBackupVaults":
		return p.listBackupVaults()
	case "DeleteBackupVault":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.deleteBackupVault(name)
	case "PutBackupVaultAccessPolicy":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.putBackupVaultAccessPolicy(name, params)
	case "GetBackupVaultAccessPolicy":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.getBackupVaultAccessPolicy(name)
	case "DeleteBackupVaultAccessPolicy":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.deleteBackupVaultAccessPolicy(name)
	case "PutBackupVaultNotifications":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.putBackupVaultNotifications(name, params)
	case "GetBackupVaultNotifications":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.getBackupVaultNotifications(name)
	case "DeleteBackupVaultNotifications":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.deleteBackupVaultNotifications(name)
	case "PutBackupVaultLockConfiguration":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.putBackupVaultLockConfiguration(name, params)
	case "DeleteBackupVaultLockConfiguration":
		name := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.deleteBackupVaultLockConfiguration(name)

	// --- BackupSelection ---
	case "CreateBackupSelection":
		planID := pathSegment(req.URL.Path, "plans", 0)
		return p.createBackupSelection(planID, params)
	case "GetBackupSelection":
		selID := pathSegment(req.URL.Path, "selections", 0)
		planID := pathSegment(req.URL.Path, "plans", 0)
		return p.getBackupSelection(planID, selID)
	case "ListBackupSelections":
		planID := pathSegment(req.URL.Path, "plans", 0)
		return p.listBackupSelections(planID)
	case "DeleteBackupSelection":
		selID := pathSegment(req.URL.Path, "selections", 0)
		return p.deleteBackupSelection(selID)

	// --- BackupJob ---
	case "StartBackupJob":
		return p.startBackupJob(params)
	case "DescribeBackupJob":
		id := pathSegment(req.URL.Path, "backup-jobs", 0)
		return p.describeBackupJob(id)
	case "ListBackupJobs":
		return p.listBackupJobs()
	case "StopBackupJob":
		id := pathSegment(req.URL.Path, "backup-jobs", 0)
		return p.stopBackupJob(id)
	case "ListBackupJobSummaries":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BackupJobSummaries": []any{}})

	// --- RecoveryPoint ---
	case "DescribeRecoveryPoint":
		rpArn := pathSegment(req.URL.Path, "recovery-points", 0)
		vaultName := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.describeRecoveryPoint(vaultName, rpArn)
	case "ListRecoveryPointsByBackupVault":
		vaultName := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.listRecoveryPointsByBackupVault(vaultName)
	case "ListRecoveryPointsByResource":
		resourceARN := pathSegment(req.URL.Path, "resources", 0)
		return p.listRecoveryPointsByResource(resourceARN)
	case "UpdateRecoveryPointLifecycle":
		rpArn := pathSegment(req.URL.Path, "recovery-points", 0)
		vaultName := pathSegment(req.URL.Path, "backup-vaults", 0)
		return p.updateRecoveryPointLifecycle(vaultName, rpArn)
	case "DeleteRecoveryPoint":
		rpArn := pathSegment(req.URL.Path, "recovery-points", 0)
		return p.deleteRecoveryPoint(rpArn)
	case "GetRecoveryPointRestoreMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreMetadata": map[string]any{}})
	case "UpdateRecoveryPointIndexSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetRecoveryPointIndexDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListIndexedRecoveryPoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"IndexedRecoveryPoints": []any{}})

	// --- RestoreJob ---
	case "StartRestoreJob":
		return p.startRestoreJob(params)
	case "DescribeRestoreJob":
		id := pathSegment(req.URL.Path, "restore-jobs", 0)
		return p.describeRestoreJob(id)
	case "ListRestoreJobs":
		return p.listRestoreJobs()
	case "GetRestoreJobMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Metadata": map[string]any{}})
	case "PutRestoreValidationResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListRestoreJobSummaries":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreJobSummaries": []any{}})
	case "ListRestoreJobsByProtectedResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreJobs": []any{}})

	// --- Framework ---
	case "CreateFramework":
		return p.createFramework(params)
	case "DescribeFramework":
		name := pathSegment(req.URL.Path, "frameworks", 0)
		return p.describeFramework(name)
	case "ListFrameworks":
		return p.listFrameworks()
	case "UpdateFramework":
		name := pathSegment(req.URL.Path, "frameworks", 0)
		return p.updateFramework(name, params)
	case "DeleteFramework":
		name := pathSegment(req.URL.Path, "frameworks", 0)
		return p.deleteFramework(name)

	// --- ReportPlan ---
	case "CreateReportPlan":
		return p.createReportPlan(params)
	case "DescribeReportPlan":
		name := pathSegment(req.URL.Path, "report-plans", 0)
		return p.describeReportPlan(name)
	case "ListReportPlans":
		return p.listReportPlans()
	case "UpdateReportPlan":
		name := pathSegment(req.URL.Path, "report-plans", 0)
		return p.updateReportPlan(name, params)
	case "DeleteReportPlan":
		name := pathSegment(req.URL.Path, "report-plans", 0)
		return p.deleteReportPlan(name)
	case "StartReportJob":
		name := pathSegment(req.URL.Path, "report-jobs", 0)
		return p.startReportJob(name)
	case "DescribeReportJob":
		id := pathSegment(req.URL.Path, "report-jobs", 0)
		return p.describeReportJob(id)
	case "ListReportJobs":
		return p.listReportJobs()

	// --- RestoreTestingPlan ---
	case "CreateRestoreTestingPlan":
		return p.createRestoreTestingPlan(params)
	case "GetRestoreTestingPlan":
		name := pathSegment(req.URL.Path, "plans", 0)
		return p.getRestoreTestingPlan(name)
	case "ListRestoreTestingPlans":
		return p.listRestoreTestingPlans()
	case "UpdateRestoreTestingPlan":
		name := pathSegment(req.URL.Path, "plans", 0)
		return p.updateRestoreTestingPlan(name, params)
	case "DeleteRestoreTestingPlan":
		name := pathSegment(req.URL.Path, "plans", 0)
		return p.deleteRestoreTestingPlan(name)

	// --- RestoreTestingSelection ---
	case "CreateRestoreTestingSelection":
		planName := pathSegment(req.URL.Path, "plans", 0)
		return p.createRestoreTestingSelection(planName, params)
	case "GetRestoreTestingSelection":
		planName := pathSegment(req.URL.Path, "plans", 0)
		selName := pathSegment(req.URL.Path, "selections", 0)
		return p.getRestoreTestingSelection(planName, selName)
	case "ListRestoreTestingSelections":
		planName := pathSegment(req.URL.Path, "plans", 0)
		return p.listRestoreTestingSelections(planName)
	case "UpdateRestoreTestingSelection":
		planName := pathSegment(req.URL.Path, "plans", 0)
		selName := pathSegment(req.URL.Path, "selections", 0)
		return p.updateRestoreTestingSelection(planName, selName, params)
	case "DeleteRestoreTestingSelection":
		planName := pathSegment(req.URL.Path, "plans", 0)
		selName := pathSegment(req.URL.Path, "selections", 0)
		return p.deleteRestoreTestingSelection(planName, selName)
	case "GetRestoreTestingInferredMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InferredMetadata": map[string]any{}})

	// --- Global/Region Settings ---
	case "DescribeGlobalSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"GlobalSettings": map[string]any{},
			"LastUpdateTime": 0,
		})
	case "UpdateGlobalSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeRegionSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceTypeManagementPreference": map[string]any{},
			"ResourceTypeOptInPreference":      map[string]any{},
		})
	case "UpdateRegionSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetSupportedResourceTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceTypes": []string{"Aurora", "CloudFormation", "DynamoDB", "EBS", "EC2",
				"EFS", "FSx", "Neptune", "RDS", "Redshift", "S3", "SAP HANA on Amazon EC2",
				"Storage Gateway", "Timestream", "VirtualMachine"},
		})

	// --- Tags ---
	case "ListTags":
		arn := pathRemainder(req.URL.Path, "tags")
		return p.listTags(arn)
	case "TagResource":
		arn := pathRemainder(req.URL.Path, "tags")
		return p.tagResource(arn, params)
	case "UntagResource":
		arn := pathRemainder(req.URL.Path, "untag")
		return p.untagResource(arn, req)

	// --- Protected Resources (stub) ---
	case "DescribeProtectedResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListProtectedResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Results": []any{}})
	case "ListProtectedResourcesByBackupVault":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Results": []any{}})

	// --- Default stubs for remaining ops ---
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	vaults, err := p.store.ListBackupVaults()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(vaults))
	for _, v := range vaults {
		res = append(res, plugin.Resource{Type: "backup-vault", ID: v.Name, Name: v.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ========== BackupPlan ==========

func (p *Provider) createBackupPlan(params map[string]any) (*plugin.Response, error) {
	planInput, _ := params["BackupPlan"].(map[string]any)
	if planInput == nil {
		planInput = params
	}
	name, _ := planInput["BackupPlanName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "BackupPlanName is required", http.StatusBadRequest), nil
	}

	id := shared.GenerateUUID()
	arn := shared.BuildARN("backup", "backup-plan", id)
	versionID := shared.GenerateID("", 32)

	rulesJSON := "{}"
	if rules, ok := planInput["Rules"]; ok {
		if b, err := json.Marshal(rules); err == nil {
			rulesJSON = string(b)
		}
	}

	plan := &BackupPlan{
		ID:        id,
		ARN:       arn,
		Name:      name,
		VersionID: versionID,
		Rules:     rulesJSON,
	}
	if err := p.store.CreateBackupPlan(plan); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "backup plan already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["BackupPlanTags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		_ = p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanId":  id,
		"BackupPlanArn": arn,
		"VersionId":     versionID,
		"CreationDate":  plan.CreatedAt.Unix(),
	})
}

func (p *Provider) getBackupPlan(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "backup plan ID is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetBackupPlan(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanId":  plan.ID,
		"BackupPlanArn": plan.ARN,
		"VersionId":     plan.VersionID,
		"CreationDate":  plan.CreatedAt.Unix(),
		"BackupPlan": map[string]any{
			"BackupPlanName": plan.Name,
			"Rules":          jsonAny(plan.Rules),
		},
	})
}

func (p *Provider) listBackupPlans() (*plugin.Response, error) {
	plans, err := p.store.ListBackupPlans()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(plans))
	for _, plan := range plans {
		list = append(list, map[string]any{
			"BackupPlanId":   plan.ID,
			"BackupPlanArn":  plan.ARN,
			"BackupPlanName": plan.Name,
			"VersionId":      plan.VersionID,
			"CreationDate":   plan.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupPlansList": list})
}

func (p *Provider) updateBackupPlan(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "backup plan ID is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetBackupPlan(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}

	planInput, _ := params["BackupPlan"].(map[string]any)
	if planInput == nil {
		planInput = params
	}
	name := plan.Name
	if v, ok := planInput["BackupPlanName"].(string); ok && v != "" {
		name = v
	}
	rules := plan.Rules
	if v, ok := planInput["Rules"]; ok {
		if b, err := json.Marshal(v); err == nil {
			rules = string(b)
		}
	}
	newVersionID := shared.GenerateID("", 32)

	if err := p.store.UpdateBackupPlan(id, name, rules, newVersionID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanId":  id,
		"BackupPlanArn": plan.ARN,
		"VersionId":     newVersionID,
	})
}

func (p *Provider) deleteBackupPlan(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "backup plan ID is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetBackupPlan(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(plan.ARN)
	if err := p.store.DeleteBackupPlan(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanId":  id,
		"BackupPlanArn": plan.ARN,
		"VersionId":     plan.VersionID,
		"DeletionDate":  0,
	})
}

func (p *Provider) listBackupPlanVersions(planID string) (*plugin.Response, error) {
	if planID == "" {
		return shared.JSONError("ValidationException", "backup plan ID is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetBackupPlan(planID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanVersionsList": []any{
			map[string]any{
				"BackupPlanId":   plan.ID,
				"BackupPlanArn":  plan.ARN,
				"BackupPlanName": plan.Name,
				"VersionId":      plan.VersionID,
				"CreationDate":   plan.CreatedAt.Unix(),
			},
		},
	})
}

func (p *Provider) exportBackupPlanTemplate(planID string) (*plugin.Response, error) {
	if planID == "" {
		return shared.JSONError("ValidationException", "backup plan ID is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetBackupPlan(planID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup plan not found", http.StatusNotFound), nil
	}
	templateJSON, _ := json.Marshal(map[string]any{
		"BackupPlanName": plan.Name,
		"Rules":          jsonAny(plan.Rules),
	})
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlanDocument": string(templateJSON),
	})
}

func (p *Provider) getBackupPlanFromJSON(params map[string]any) (*plugin.Response, error) {
	doc, _ := params["BackupPlanDocument"].(string)
	var planInput map[string]any
	if doc != "" {
		_ = json.Unmarshal([]byte(doc), &planInput)
	}
	if planInput == nil {
		planInput = map[string]any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlan": planInput,
	})
}

func (p *Provider) getBackupPlanFromTemplate(path string) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "template-plan",
			"Rules":          []any{},
		},
	})
}

// ========== BackupVault ==========

func (p *Provider) createBackupVault(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		if n, ok := params["BackupVaultName"].(string); ok {
			name = n
		}
	}
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}

	arn := shared.BuildARN("backup", "backup-vault", name)
	encryptionKey, _ := params["EncryptionKeyArn"].(string)

	vault := &BackupVault{
		Name:          name,
		ARN:           arn,
		EncryptionKey: encryptionKey,
		Notifications: "{}",
		AccessPolicy:  "",
		LockConfig:    "{}",
	}
	if err := p.store.CreateBackupVault(vault); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "backup vault already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["BackupVaultTags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupVaultName": name,
		"BackupVaultArn":  arn,
		"CreationDate":    vault.CreatedAt.Unix(),
	})
}

func (p *Provider) describeBackupVault(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetBackupVault(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, vaultToMap(v))
}

func (p *Provider) listBackupVaults() (*plugin.Response, error) {
	vaults, err := p.store.ListBackupVaults()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(vaults))
	for _, v := range vaults {
		list = append(list, vaultToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupVaultList": list})
}

func (p *Provider) deleteBackupVault(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetBackupVault(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(v.ARN)
	if err := p.store.DeleteBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putBackupVaultAccessPolicy(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	policy, _ := params["Policy"].(string)
	if err := p.store.UpdateBackupVaultAccessPolicy(name, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getBackupVaultAccessPolicy(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetBackupVault(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupVaultName": v.Name,
		"BackupVaultArn":  v.ARN,
		"Policy":          v.AccessPolicy,
	})
}

func (p *Provider) deleteBackupVaultAccessPolicy(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	p.store.UpdateBackupVaultAccessPolicy(name, "") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putBackupVaultNotifications(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	notifJSON := "{}"
	if v, ok := params["SNSTopicArn"]; ok {
		b, _ := json.Marshal(map[string]any{
			"SNSTopicArn":       v,
			"BackupVaultEvents": params["BackupVaultEvents"],
		})
		notifJSON = string(b)
	}
	p.store.UpdateBackupVaultNotifications(name, notifJSON) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getBackupVaultNotifications(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetBackupVault(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	var notif map[string]any
	_ = json.Unmarshal([]byte(v.Notifications), &notif)
	if notif == nil {
		notif = map[string]any{}
	}
	notif["BackupVaultName"] = v.Name
	notif["BackupVaultArn"] = v.ARN
	return shared.JSONResponse(http.StatusOK, notif)
}

func (p *Provider) deleteBackupVaultNotifications(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	p.store.UpdateBackupVaultNotifications(name, "{}") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putBackupVaultLockConfiguration(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	b, _ := json.Marshal(params)
	p.store.UpdateBackupVaultLockConfig(name, string(b)) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteBackupVaultLockConfiguration(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup vault not found", http.StatusNotFound), nil
	}
	p.store.UpdateBackupVaultLockConfig(name, "{}") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== BackupSelection ==========

func (p *Provider) createBackupSelection(planID string, params map[string]any) (*plugin.Response, error) {
	if planID == "" {
		if v, ok := params["BackupPlanId"].(string); ok {
			planID = v
		}
	}
	if planID == "" {
		return shared.JSONError("ValidationException", "BackupPlanId is required", http.StatusBadRequest), nil
	}
	selInput, _ := params["BackupSelection"].(map[string]any)
	if selInput == nil {
		selInput = params
	}
	selName, _ := selInput["SelectionName"].(string)
	if selName == "" {
		return shared.JSONError("ValidationException", "SelectionName is required", http.StatusBadRequest), nil
	}

	id := shared.GenerateUUID()
	iamRole, _ := selInput["IamRoleArn"].(string)

	resourcesJSON := "[]"
	if r, ok := selInput["Resources"]; ok {
		if b, err := json.Marshal(r); err == nil {
			resourcesJSON = string(b)
		}
	}
	conditionsJSON := "{}"
	if c, ok := selInput["Conditions"]; ok {
		if b, err := json.Marshal(c); err == nil {
			conditionsJSON = string(b)
		}
	}

	sel := &BackupSelection{
		ID:         id,
		PlanID:     planID,
		Name:       selName,
		IAMRole:    iamRole,
		Resources:  resourcesJSON,
		Conditions: conditionsJSON,
	}
	if err := p.store.CreateBackupSelection(sel); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SelectionId":  id,
		"BackupPlanId": planID,
		"CreationDate": sel.CreatedAt.Unix(),
	})
}

func (p *Provider) getBackupSelection(planID, selID string) (*plugin.Response, error) {
	if selID == "" {
		return shared.JSONError("ValidationException", "SelectionId is required", http.StatusBadRequest), nil
	}
	sel, err := p.store.GetBackupSelection(selID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup selection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SelectionId":  sel.ID,
		"BackupPlanId": sel.PlanID,
		"CreationDate": sel.CreatedAt.Unix(),
		"BackupSelection": map[string]any{
			"SelectionName": sel.Name,
			"IamRoleArn":    sel.IAMRole,
			"Resources":     jsonAny(sel.Resources),
			"Conditions":    jsonAny(sel.Conditions),
		},
	})
}

func (p *Provider) listBackupSelections(planID string) (*plugin.Response, error) {
	if planID == "" {
		return shared.JSONError("ValidationException", "BackupPlanId is required", http.StatusBadRequest), nil
	}
	sels, err := p.store.ListBackupSelections(planID)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(sels))
	for _, sel := range sels {
		list = append(list, map[string]any{
			"SelectionId":   sel.ID,
			"SelectionName": sel.Name,
			"BackupPlanId":  sel.PlanID,
			"IamRoleArn":    sel.IAMRole,
			"CreationDate":  sel.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupSelectionsList": list})
}

func (p *Provider) deleteBackupSelection(selID string) (*plugin.Response, error) {
	if selID == "" {
		return shared.JSONError("ValidationException", "SelectionId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteBackupSelection(selID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup selection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== BackupJob ==========

func (p *Provider) startBackupJob(params map[string]any) (*plugin.Response, error) {
	vaultName, _ := params["BackupVaultName"].(string)
	if vaultName == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	resourceARN, _ := params["ResourceArn"].(string)
	resourceType, _ := params["ResourceType"].(string)
	planID, _ := params["BackupPlanId"].(string)

	id := shared.GenerateUUID()
	rpARN := fmt.Sprintf("arn:aws:backup:us-east-1:000000000000:recovery-point:%s", id)

	job := &BackupJob{
		ID:           id,
		VaultName:    vaultName,
		ResourceARN:  resourceARN,
		ResourceType: resourceType,
		Status:       "COMPLETED",
		PlanID:       planID,
	}
	if err := p.store.CreateBackupJob(job); err != nil {
		return nil, err
	}

	// Create associated recovery point
	rp := &RecoveryPoint{
		ARN:          rpARN,
		VaultName:    vaultName,
		ResourceARN:  resourceARN,
		ResourceType: resourceType,
		Status:       "COMPLETED",
	}
	p.store.CreateRecoveryPoint(rp) //nolint:errcheck

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupJobId":      id,
		"RecoveryPointArn": rpARN,
		"CreationDate":     job.CreatedAt.Unix(),
	})
}

func (p *Provider) describeBackupJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "BackupJobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetBackupJob(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, jobToMap(job))
}

func (p *Provider) listBackupJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListBackupJobs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupJobs": list})
}

func (p *Provider) stopBackupJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "BackupJobId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetBackupJob(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "backup job not found", http.StatusNotFound), nil
	}
	p.store.UpdateBackupJobStatus(id, "ABORTED") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== RecoveryPoint ==========

func (p *Provider) describeRecoveryPoint(vaultName, rpArn string) (*plugin.Response, error) {
	if rpArn == "" {
		return shared.JSONError("ValidationException", "RecoveryPointArn is required", http.StatusBadRequest), nil
	}
	rp, err := p.store.GetRecoveryPoint(rpArn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "recovery point not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, recoveryPointToMap(rp))
}

func (p *Provider) listRecoveryPointsByBackupVault(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "BackupVaultName is required", http.StatusBadRequest), nil
	}
	rps, err := p.store.ListRecoveryPointsByVault(vaultName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(rps))
	for _, rp := range rps {
		list = append(list, recoveryPointToMap(&rp))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RecoveryPoints": list})
}

func (p *Provider) listRecoveryPointsByResource(resourceARN string) (*plugin.Response, error) {
	rps, err := p.store.ListRecoveryPointsByResource(resourceARN)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(rps))
	for _, rp := range rps {
		list = append(list, recoveryPointToMap(&rp))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RecoveryPoints": list})
}

func (p *Provider) updateRecoveryPointLifecycle(vaultName, rpArn string) (*plugin.Response, error) {
	if rpArn == "" {
		return shared.JSONError("ValidationException", "RecoveryPointArn is required", http.StatusBadRequest), nil
	}
	rp, err := p.store.GetRecoveryPoint(rpArn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "recovery point not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BackupVaultArn":      shared.BuildARN("backup", "backup-vault", vaultName),
		"RecoveryPointArn":    rp.ARN,
		"Lifecycle":           map[string]any{},
		"CalculatedLifecycle": map[string]any{},
	})
}

func (p *Provider) deleteRecoveryPoint(rpArn string) (*plugin.Response, error) {
	if rpArn == "" {
		return shared.JSONError("ValidationException", "RecoveryPointArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRecoveryPoint(rpArn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "recovery point not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== RestoreJob ==========

func (p *Provider) startRestoreJob(params map[string]any) (*plugin.Response, error) {
	rpArn, _ := params["RecoveryPointArn"].(string)
	if rpArn == "" {
		return shared.JSONError("ValidationException", "RecoveryPointArn is required", http.StatusBadRequest), nil
	}
	resourceType, _ := params["ResourceType"].(string)

	id := shared.GenerateUUID()
	job := &RestoreJob{
		ID:            id,
		RecoveryPoint: rpArn,
		ResourceType:  resourceType,
		Status:        "COMPLETED",
	}
	if err := p.store.CreateRestoreJob(job); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestoreJobId": id,
	})
}

func (p *Provider) describeRestoreJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "RestoreJobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetRestoreJob(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, restoreJobToMap(job))
}

func (p *Provider) listRestoreJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListRestoreJobs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, restoreJobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreJobs": list})
}

// ========== Framework ==========

func (p *Provider) createFramework(params map[string]any) (*plugin.Response, error) {
	name, _ := params["FrameworkName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "FrameworkName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("backup", "framework", name)
	description, _ := params["FrameworkDescription"].(string)

	controlsJSON := "[]"
	if c, ok := params["FrameworkControls"]; ok {
		if b, err := json.Marshal(c); err == nil {
			controlsJSON = string(b)
		}
	}

	fw := &Framework{
		Name:        name,
		ARN:         arn,
		Description: description,
		Controls:    controlsJSON,
		Status:      "ACTIVE",
	}
	if err := p.store.CreateFramework(fw); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "framework already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FrameworkName": name,
		"FrameworkArn":  arn,
	})
}

func (p *Provider) describeFramework(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "FrameworkName is required", http.StatusBadRequest), nil
	}
	fw, err := p.store.GetFramework(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "framework not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, frameworkToMap(fw))
}

func (p *Provider) listFrameworks() (*plugin.Response, error) {
	fws, err := p.store.ListFrameworks()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(fws))
	for _, fw := range fws {
		list = append(list, frameworkToMap(&fw))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Frameworks": list})
}

func (p *Provider) updateFramework(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "FrameworkName is required", http.StatusBadRequest), nil
	}
	fw, err := p.store.GetFramework(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "framework not found", http.StatusNotFound), nil
	}
	description := fw.Description
	if v, ok := params["FrameworkDescription"].(string); ok {
		description = v
	}
	controls := fw.Controls
	if c, ok := params["FrameworkControls"]; ok {
		if b, err := json.Marshal(c); err == nil {
			controls = string(b)
		}
	}
	if err := p.store.UpdateFramework(name, description, controls, fw.Status); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FrameworkName": name,
		"FrameworkArn":  fw.ARN,
	})
}

func (p *Provider) deleteFramework(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "FrameworkName is required", http.StatusBadRequest), nil
	}
	fw, err := p.store.GetFramework(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "framework not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(fw.ARN)
	if err := p.store.DeleteFramework(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "framework not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== ReportPlan ==========

func (p *Provider) createReportPlan(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ReportPlanName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ReportPlanName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("backup", "report-plan", name)
	description, _ := params["ReportPlanDescription"].(string)

	deliveryJSON := "{}"
	if d, ok := params["ReportDeliveryChannel"]; ok {
		if b, err := json.Marshal(d); err == nil {
			deliveryJSON = string(b)
		}
	}
	settingJSON := "{}"
	if s, ok := params["ReportSetting"]; ok {
		if b, err := json.Marshal(s); err == nil {
			settingJSON = string(b)
		}
	}

	rp := &ReportPlan{
		Name:            name,
		ARN:             arn,
		Description:     description,
		DeliveryChannel: deliveryJSON,
		ReportSetting:   settingJSON,
	}
	if err := p.store.CreateReportPlan(rp); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "report plan already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReportPlanName": name,
		"ReportPlanArn":  arn,
	})
}

func (p *Provider) describeReportPlan(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ReportPlanName is required", http.StatusBadRequest), nil
	}
	rp, err := p.store.GetReportPlan(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "report plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReportPlan": reportPlanToMap(rp)})
}

func (p *Provider) listReportPlans() (*plugin.Response, error) {
	rps, err := p.store.ListReportPlans()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(rps))
	for _, rp := range rps {
		list = append(list, reportPlanToMap(&rp))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReportPlans": list})
}

func (p *Provider) updateReportPlan(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ReportPlanName is required", http.StatusBadRequest), nil
	}
	rp, err := p.store.GetReportPlan(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "report plan not found", http.StatusNotFound), nil
	}
	description := rp.Description
	if v, ok := params["ReportPlanDescription"].(string); ok {
		description = v
	}
	deliveryChannel := rp.DeliveryChannel
	if d, ok := params["ReportDeliveryChannel"]; ok {
		if b, err := json.Marshal(d); err == nil {
			deliveryChannel = string(b)
		}
	}
	reportSetting := rp.ReportSetting
	if s, ok := params["ReportSetting"]; ok {
		if b, err := json.Marshal(s); err == nil {
			reportSetting = string(b)
		}
	}
	if err := p.store.UpdateReportPlan(name, description, deliveryChannel, reportSetting); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReportPlanName": name,
		"ReportPlanArn":  rp.ARN,
	})
}

func (p *Provider) deleteReportPlan(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ReportPlanName is required", http.StatusBadRequest), nil
	}
	rp, err := p.store.GetReportPlan(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "report plan not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(rp.ARN)
	if err := p.store.DeleteReportPlan(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "report plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startReportJob(planName string) (*plugin.Response, error) {
	if planName == "" {
		return shared.JSONError("ValidationException", "ReportPlanName is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	job := &ReportJob{
		ID:       id,
		PlanName: planName,
		Status:   "COMPLETED",
	}
	if err := p.store.CreateReportJob(job); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReportJobId": id})
}

func (p *Provider) describeReportJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "ReportJobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetReportJob(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "report job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReportJob": reportJobToMap(job)})
}

func (p *Provider) listReportJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListReportJobs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, reportJobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReportJobs": list})
}

// ========== RestoreTestingPlan ==========

func (p *Provider) createRestoreTestingPlan(params map[string]any) (*plugin.Response, error) {
	planInput, _ := params["RestoreTestingPlan"].(map[string]any)
	if planInput == nil {
		planInput = params
	}
	name, _ := planInput["RestoreTestingPlanName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("backup", "restore-testing-plan", name)
	schedule, _ := planInput["ScheduleExpression"].(string)
	var startWindow int64
	if v, ok := planInput["StartWindowHours"].(float64); ok {
		startWindow = int64(v)
	}
	configJSON := "{}"
	if c, ok := planInput["RecoveryPointSelection"]; ok {
		if b, err := json.Marshal(c); err == nil {
			configJSON = string(b)
		}
	}

	plan := &RestoreTestingPlan{
		Name:        name,
		ARN:         arn,
		Schedule:    schedule,
		StartWindow: startWindow,
		Config:      configJSON,
	}
	if err := p.store.CreateRestoreTestingPlan(plan); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "restore testing plan already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestoreTestingPlanArn":  arn,
		"RestoreTestingPlanName": name,
	})
}

func (p *Provider) getRestoreTestingPlan(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetRestoreTestingPlan(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreTestingPlan": restoreTestingPlanToMap(plan)})
}

func (p *Provider) listRestoreTestingPlans() (*plugin.Response, error) {
	plans, err := p.store.ListRestoreTestingPlans()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(plans))
	for _, plan := range plans {
		list = append(list, restoreTestingPlanToMap(&plan))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreTestingPlans": list})
}

func (p *Provider) updateRestoreTestingPlan(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	plan, err := p.store.GetRestoreTestingPlan(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing plan not found", http.StatusNotFound), nil
	}
	planInput, _ := params["RestoreTestingPlan"].(map[string]any)
	if planInput == nil {
		planInput = params
	}
	schedule := plan.Schedule
	if v, ok := planInput["ScheduleExpression"].(string); ok && v != "" {
		schedule = v
	}
	startWindow := plan.StartWindow
	if v, ok := planInput["StartWindowHours"].(float64); ok {
		startWindow = int64(v)
	}
	config := plan.Config
	if c, ok := planInput["RecoveryPointSelection"]; ok {
		if b, err := json.Marshal(c); err == nil {
			config = string(b)
		}
	}
	if err := p.store.UpdateRestoreTestingPlan(name, schedule, config, startWindow); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestoreTestingPlanArn":  plan.ARN,
		"RestoreTestingPlanName": name,
	})
}

func (p *Provider) deleteRestoreTestingPlan(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRestoreTestingPlan(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing plan not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== RestoreTestingSelection ==========

func (p *Provider) createRestoreTestingSelection(planName string, params map[string]any) (*plugin.Response, error) {
	if planName == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	selInput, _ := params["RestoreTestingSelection"].(map[string]any)
	if selInput == nil {
		selInput = params
	}
	selName, _ := selInput["RestoreTestingSelectionName"].(string)
	if selName == "" {
		return shared.JSONError("ValidationException", "RestoreTestingSelectionName is required", http.StatusBadRequest), nil
	}
	iamRole, _ := selInput["IamRoleArn"].(string)
	resourceType, _ := selInput["ProtectedResourceType"].(string)
	configJSON := "{}"
	if c, ok := selInput["ProtectedResourceConditions"]; ok {
		if b, err := json.Marshal(c); err == nil {
			configJSON = string(b)
		}
	}

	sel := &RestoreTestingSelection{
		Name:         selName,
		PlanName:     planName,
		IAMRole:      iamRole,
		ResourceType: resourceType,
		Config:       configJSON,
	}
	if err := p.store.CreateRestoreTestingSelection(sel); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "restore testing selection already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestoreTestingPlanArn":       shared.BuildARN("backup", "restore-testing-plan", planName),
		"RestoreTestingPlanName":      planName,
		"RestoreTestingSelectionName": selName,
	})
}

func (p *Provider) getRestoreTestingSelection(planName, selName string) (*plugin.Response, error) {
	if planName == "" || selName == "" {
		return shared.JSONError("ValidationException", "plan and selection names are required", http.StatusBadRequest), nil
	}
	sel, err := p.store.GetRestoreTestingSelection(planName, selName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing selection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreTestingSelection": restoreTestingSelectionToMap(sel)})
}

func (p *Provider) listRestoreTestingSelections(planName string) (*plugin.Response, error) {
	if planName == "" {
		return shared.JSONError("ValidationException", "RestoreTestingPlanName is required", http.StatusBadRequest), nil
	}
	sels, err := p.store.ListRestoreTestingSelections(planName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(sels))
	for _, sel := range sels {
		list = append(list, restoreTestingSelectionToMap(&sel))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RestoreTestingSelections": list})
}

func (p *Provider) updateRestoreTestingSelection(planName, selName string, params map[string]any) (*plugin.Response, error) {
	if planName == "" || selName == "" {
		return shared.JSONError("ValidationException", "plan and selection names are required", http.StatusBadRequest), nil
	}
	sel, err := p.store.GetRestoreTestingSelection(planName, selName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing selection not found", http.StatusNotFound), nil
	}
	selInput, _ := params["RestoreTestingSelection"].(map[string]any)
	if selInput == nil {
		selInput = params
	}
	iamRole := sel.IAMRole
	if v, ok := selInput["IamRoleArn"].(string); ok && v != "" {
		iamRole = v
	}
	resourceType := sel.ResourceType
	if v, ok := selInput["ProtectedResourceType"].(string); ok && v != "" {
		resourceType = v
	}
	config := sel.Config
	if c, ok := selInput["ProtectedResourceConditions"]; ok {
		if b, err := json.Marshal(c); err == nil {
			config = string(b)
		}
	}
	if err := p.store.UpdateRestoreTestingSelection(planName, selName, iamRole, resourceType, config); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestoreTestingPlanArn":       shared.BuildARN("backup", "restore-testing-plan", planName),
		"RestoreTestingPlanName":      planName,
		"RestoreTestingSelectionName": selName,
	})
}

func (p *Provider) deleteRestoreTestingSelection(planName, selName string) (*plugin.Response, error) {
	if planName == "" || selName == "" {
		return shared.JSONError("ValidationException", "plan and selection names are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRestoreTestingSelection(planName, selName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "restore testing selection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== Tags ==========

func (p *Provider) listTags(arn string) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

func (p *Provider) tagResource(arn string, params map[string]any) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(arn string, req *http.Request) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ========== Helpers ==========

// resolveOp maps an HTTP method + URL path to a Backup API operation name.
func resolveOp(method, path string) string {
	// Trim trailing slash and split into segments.
	path = strings.TrimRight(path, "/")
	segs := strings.Split(strings.TrimLeft(path, "/"), "/")
	n := len(segs)

	// Helper: check if segs starts with the given prefix.
	match := func(parts ...string) bool {
		if n < len(parts) {
			return false
		}
		for i, p := range parts {
			if p != "*" && segs[i] != p {
				return false
			}
		}
		return n == len(parts)
	}

	switch method {
	case http.MethodPut:
		switch {
		case match("backup", "plans"):
			return "CreateBackupPlan"
		case match("backup", "plans", "*", "selections"):
			return "CreateBackupSelection"
		case match("backup-vaults", "*"):
			return "CreateBackupVault"
		case match("backup-vaults", "*", "access-policy"):
			return "PutBackupVaultAccessPolicy"
		case match("backup-vaults", "*", "notification-configuration"):
			return "PutBackupVaultNotifications"
		case match("backup-vaults", "*", "vault-lock"):
			return "PutBackupVaultLockConfiguration"
		case match("backup-jobs"):
			return "StartBackupJob"
		case match("restore-jobs"):
			return "StartRestoreJob"
		case match("audit", "frameworks"):
			return "CreateFramework"
		case match("audit", "report-plans"):
			return "CreateReportPlan"
		case match("global-settings"):
			return "UpdateGlobalSettings"
		case match("account-settings"):
			return "UpdateRegionSettings"
		}

	case http.MethodGet:
		switch {
		// backup/plans sub-resources first (longer matches)
		case match("backup", "plans", "*", "versions"):
			return "ListBackupPlanVersions"
		case match("backup", "plans", "*", "toTemplate"):
			return "ExportBackupPlanTemplate"
		case match("backup", "plans", "*", "selections", "*"):
			return "GetBackupSelection"
		case match("backup", "plans", "*", "selections"):
			return "ListBackupSelections"
		case match("backup", "plans", "*"):
			return "GetBackupPlan"
		case match("backup", "plans"):
			return "ListBackupPlans"
		case match("backup", "template", "plans", "*"):
			return "GetBackupPlanFromTemplate"
		case match("backup", "templates"):
			return "ListBackupPlanTemplates"
		// backup-vaults sub-resources
		case match("backup-vaults", "*", "access-policy"):
			return "GetBackupVaultAccessPolicy"
		case match("backup-vaults", "*", "notification-configuration"):
			return "GetBackupVaultNotifications"
		case match("backup-vaults", "*", "recovery-points", "*"):
			return "DescribeRecoveryPoint"
		case match("backup-vaults", "*", "recovery-points"):
			return "ListRecoveryPointsByBackupVault"
		case match("backup-vaults", "*"):
			return "DescribeBackupVault"
		case match("backup-vaults"):
			return "ListBackupVaults"
		// backup-jobs
		case match("backup-jobs", "summary"):
			return "ListBackupJobSummaries"
		case match("backup-jobs", "*"):
			return "DescribeBackupJob"
		case match("backup-jobs"):
			return "ListBackupJobs"
		// restore-jobs
		case match("restore-jobs", "*"):
			return "DescribeRestoreJob"
		case match("restore-jobs"):
			return "ListRestoreJobs"
		// audit
		case match("audit", "frameworks", "*"):
			return "DescribeFramework"
		case match("audit", "frameworks"):
			return "ListFrameworks"
		case match("audit", "report-plans", "*"):
			return "DescribeReportPlan"
		case match("audit", "report-plans"):
			return "ListReportPlans"
		// misc
		case match("global-settings"):
			return "DescribeGlobalSettings"
		case match("account-settings"):
			return "DescribeRegionSettings"
		case match("supported-resource-types"):
			return "GetSupportedResourceTypes"
		case match("tags", "*"):
			return "ListTags"
		case match("resources", "*"):
			return "DescribeProtectedResource"
		case match("resources"):
			return "ListProtectedResources"
		}

	case http.MethodPost:
		switch {
		case match("backup", "plans", "*"):
			return "UpdateBackupPlan"
		case match("backup", "template", "json", "toPlan"):
			return "GetBackupPlanFromJSON"
		case match("backup-jobs", "*"):
			return "StopBackupJob"
		case match("tags", "*"):
			return "TagResource"
		case match("untag", "*"):
			return "UntagResource"
		}

	case http.MethodDelete:
		switch {
		case match("backup", "plans", "*", "selections", "*"):
			return "DeleteBackupSelection"
		case match("backup", "plans", "*"):
			return "DeleteBackupPlan"
		case match("backup-vaults", "*", "access-policy"):
			return "DeleteBackupVaultAccessPolicy"
		case match("backup-vaults", "*", "notification-configuration"):
			return "DeleteBackupVaultNotifications"
		case match("backup-vaults", "*", "vault-lock"):
			return "DeleteBackupVaultLockConfiguration"
		case match("backup-vaults", "*"):
			return "DeleteBackupVault"
		}
	}

	return ""
}

func pathSegment(path, key string, offset int) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1+offset < len(parts) {
			return parts[i+1+offset]
		}
	}
	return ""
}

// pathRemainder returns everything after the first occurrence of key in path
// joined with "/". Useful for ARNs that contain slashes.
func pathRemainder(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return strings.Join(parts[i+1:], "/")
		}
	}
	return ""
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func jsonAny(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	return v
}

func vaultToMap(v *BackupVault) map[string]any {
	return map[string]any{
		"BackupVaultName":        v.Name,
		"BackupVaultArn":         v.ARN,
		"EncryptionKeyArn":       v.EncryptionKey,
		"CreationDate":           v.CreatedAt.Unix(),
		"NumberOfRecoveryPoints": 0,
	}
}

func jobToMap(j *BackupJob) map[string]any {
	return map[string]any{
		"BackupJobId":       j.ID,
		"BackupVaultName":   j.VaultName,
		"ResourceArn":       j.ResourceARN,
		"ResourceType":      j.ResourceType,
		"State":             j.Status,
		"BackupPlanId":      j.PlanID,
		"CreationDate":      j.CreatedAt.Unix(),
		"CompletionDate":    j.CompletedAt.Unix(),
		"PercentDone":       "100.0",
		"BackupSizeInBytes": 0,
	}
}

func restoreJobToMap(j *RestoreJob) map[string]any {
	return map[string]any{
		"RestoreJobId":     j.ID,
		"BackupVaultName":  j.VaultName,
		"RecoveryPointArn": j.RecoveryPoint,
		"ResourceType":     j.ResourceType,
		"Status":           j.Status,
		"CreationDate":     j.CreatedAt.Unix(),
		"CompletionDate":   j.CompletedAt.Unix(),
	}
}

func recoveryPointToMap(rp *RecoveryPoint) map[string]any {
	return map[string]any{
		"RecoveryPointArn":  rp.ARN,
		"BackupVaultName":   rp.VaultName,
		"BackupVaultArn":    shared.BuildARN("backup", "backup-vault", rp.VaultName),
		"ResourceArn":       rp.ResourceARN,
		"ResourceType":      rp.ResourceType,
		"Status":            rp.Status,
		"CreationDate":      rp.CreatedAt.Unix(),
		"BackupSizeInBytes": 0,
		"IsEncrypted":       false,
	}
}

func frameworkToMap(f *Framework) map[string]any {
	return map[string]any{
		"FrameworkName":        f.Name,
		"FrameworkArn":         f.ARN,
		"FrameworkDescription": f.Description,
		"FrameworkControls":    jsonAny(f.Controls),
		"FrameworkStatus":      f.Status,
		"CreationTime":         f.CreatedAt.Unix(),
	}
}

func reportPlanToMap(rp *ReportPlan) map[string]any {
	return map[string]any{
		"ReportPlanName":              rp.Name,
		"ReportPlanArn":               rp.ARN,
		"ReportPlanDescription":       rp.Description,
		"ReportDeliveryChannel":       jsonAny(rp.DeliveryChannel),
		"ReportSetting":               jsonAny(rp.ReportSetting),
		"CreationTime":                rp.CreatedAt.Unix(),
		"LastAttemptedExecutionTime":  0,
		"LastSuccessfulExecutionTime": 0,
		"DeploymentStatus":            "COMPLETED",
	}
}

func reportJobToMap(j *ReportJob) map[string]any {
	return map[string]any{
		"ReportJobId":    j.ID,
		"ReportPlanArn":  shared.BuildARN("backup", "report-plan", j.PlanName),
		"Status":         j.Status,
		"CreationTime":   j.CreatedAt.Unix(),
		"CompletionTime": j.CompletedAt.Unix(),
	}
}

func restoreTestingPlanToMap(p *RestoreTestingPlan) map[string]any {
	return map[string]any{
		"RestoreTestingPlanArn":  p.ARN,
		"RestoreTestingPlanName": p.Name,
		"ScheduleExpression":     p.Schedule,
		"StartWindowHours":       p.StartWindow,
		"RecoveryPointSelection": jsonAny(p.Config),
		"CreationTime":           p.CreatedAt.Unix(),
	}
}

func restoreTestingSelectionToMap(sel *RestoreTestingSelection) map[string]any {
	return map[string]any{
		"RestoreTestingSelectionName": sel.Name,
		"RestoreTestingPlanName":      sel.PlanName,
		"IamRoleArn":                  sel.IAMRole,
		"ProtectedResourceType":       sel.ResourceType,
		"ProtectedResourceConditions": jsonAny(sel.Config),
		"CreationTime":                sel.CreatedAt.Unix(),
	}
}
