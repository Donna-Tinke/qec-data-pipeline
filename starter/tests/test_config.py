from pathlib import Path

from quantum_lake_student.config import Settings


def test_settings_support_local_test_mode(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LAKE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKE_ROOT", str(tmp_path))
    settings = Settings.from_environment()
    assert settings.lake_backend == "local"
    assert settings.local_lake_root == tmp_path

