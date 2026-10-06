import pytest
import conftest


def test_external_sns_outbox_requires_explicit_data_directory(monkeypatch):
    monkeypatch.delenv("DEVCLOUD_DATA_DIR", raising=False)
    with pytest.raises(RuntimeError, match="DEVCLOUD_DATA_DIR"):
        conftest._devcloud_data_path(None)


def test_external_sns_outbox_rejects_missing_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("DEVCLOUD_DATA_DIR", str(tmp_path / "missing"))
    with pytest.raises(RuntimeError, match="readable"):
        conftest._devcloud_data_path(None)
