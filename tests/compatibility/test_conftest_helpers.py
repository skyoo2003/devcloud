import io

from tests.compatibility.conftest import _build_devcloud_cmd, _start_server_error


class TestBuildDevcloudCmd:
    def test_includes_config_when_file_exists(self, monkeypatch, tmp_path):
        def fake_isfile(path):
            return path.endswith("devcloud.yaml")

        monkeypatch.setattr("tests.compatibility.conftest.os.path.isfile", fake_isfile)

        cmd = _build_devcloud_cmd(str(tmp_path), None)

        assert cmd[:3] == ["go", "run", "./cmd/devcloud"]
        assert "-config" in cmd
        assert "devcloud.yaml" in cmd

    def test_omits_config_when_file_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "tests.compatibility.conftest.os.path.isfile", lambda p: False
        )

        cmd = _build_devcloud_cmd(str(tmp_path), None)

        assert cmd == ["go", "run", "./cmd/devcloud"]
        assert "-config" not in cmd
        assert "devcloud.yaml" not in cmd

    def test_uses_bin_path_when_provided(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "tests.compatibility.conftest.os.path.isfile", lambda p: False
        )

        cmd = _build_devcloud_cmd(str(tmp_path), "/usr/local/bin/devcloud")

        assert cmd == ["/usr/local/bin/devcloud"]

    def test_bin_path_with_config(self, monkeypatch, tmp_path):
        def fake_isfile(path):
            return path.endswith("devcloud.yaml")

        monkeypatch.setattr("tests.compatibility.conftest.os.path.isfile", fake_isfile)

        cmd = _build_devcloud_cmd(str(tmp_path), "/usr/local/bin/devcloud")

        assert cmd[0] == "/usr/local/bin/devcloud"
        assert "-config" in cmd


class TestDevcloudServerErrorHandling:
    def test_raises_runtime_error_with_stderr_on_startup_failure(self, monkeypatch):
        fake_stderr = io.BytesIO(b"fatal: config file not found\n")

        class FakeProc:
            def kill(self):
                pass

            def wait(self):
                return 1

            stderr = fake_stderr

        monkeypatch.setattr(
            "tests.compatibility.conftest.subprocess.Popen", lambda *a, **kw: FakeProc()
        )
        monkeypatch.setattr(
            "tests.compatibility.conftest._wait_for_server",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("timeout")),
        )

        import pytest

        with pytest.raises(RuntimeError) as exc_info:
            _start_server_error(["go", "run", "./cmd/devcloud"], "/tmp/project", {})

        msg = str(exc_info.value)
        assert "fatal: config file not found" in msg
        assert "command: go run ./cmd/devcloud" in msg
        assert "stderr:" in msg
