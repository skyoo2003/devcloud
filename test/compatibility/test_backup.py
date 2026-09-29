import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_backup_vault(backup_client):
    backup_client.create_backup_vault(BackupVaultName="compat-vault")
    resp = backup_client.describe_backup_vault(BackupVaultName="compat-vault")
    assert resp["BackupVaultName"] == "compat-vault"


def test_list_backup_vaults(backup_client):
    backup_client.create_backup_vault(BackupVaultName="list-vault")
    resp = backup_client.list_backup_vaults()
    names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
    assert "list-vault" in names


def test_delete_backup_vault(backup_client):
    backup_client.create_backup_vault(BackupVaultName="del-vault")
    backup_client.delete_backup_vault(BackupVaultName="del-vault")
    resp = backup_client.list_backup_vaults()
    names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
    assert "del-vault" not in names


def test_create_and_get_backup_plan(backup_client):
    resp = backup_client.create_backup_plan(
        BackupPlan={
            "BackupPlanName": "compat-plan",
            "Rules": [
                {
                    "RuleName": "daily",
                    "TargetBackupVaultName": "Default",
                    "ScheduleExpression": "cron(0 12 * * ? *)",
                }
            ],
        }
    )
    plan_id = resp["BackupPlanId"]
    assert plan_id

    get = backup_client.get_backup_plan(BackupPlanId=plan_id)
    assert get["BackupPlan"]["BackupPlanName"] == "compat-plan"


def test_list_backup_plans(backup_client):
    backup_client.create_backup_plan(
        BackupPlan={
            "BackupPlanName": "list-plan",
            "Rules": [
                {
                    "RuleName": "r1",
                    "TargetBackupVaultName": "Default",
                    "ScheduleExpression": "cron(0 12 * * ? *)",
                }
            ],
        }
    )
    resp = backup_client.list_backup_plans()
    names = [p["BackupPlanName"] for p in resp["BackupPlansList"]]
    assert "list-plan" in names


def test_describe_nonexistent_vault(backup_client):
    with pytest.raises(ClientError) as exc:
        backup_client.describe_backup_vault(BackupVaultName="no-such-vault-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
