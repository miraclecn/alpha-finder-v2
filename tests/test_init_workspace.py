"""Tests for init_workspace.py"""
from __future__ import annotations

from pathlib import Path

import pytest

from alpha_find_v2.data_ingest.config_models import load_data_sources_config
from alpha_find_v2.data_ingest.init_workspace import FileInitResult, InitReport, init_workspace


def _result_map(report: InitReport) -> dict[str, str]:
    """Return {relative-stem: action} for easier assertion."""
    return {r.path.name: r.action for r in report.actions}


# 1. Fresh workspace: all three files are created
def test_init_workspace_creates_all_files(tmp_path: Path) -> None:
    report = init_workspace(tmp_path)

    assert isinstance(report, InitReport)
    assert len(report.actions) == 3
    actions = _result_map(report)
    assert actions[".env"] == "created"
    assert actions["data_sources.toml"] == "created"
    assert actions[".gitkeep"] == "created"

    # Files must actually exist on disk
    assert (tmp_path / ".env").is_file()
    assert (tmp_path / "config" / "data_sources.toml").is_file()
    assert (tmp_path / "output" / ".gitkeep").is_file()


# 2. Second run: all three files are reported as skipped, contents unchanged
def test_init_workspace_skips_existing_files(tmp_path: Path) -> None:
    init_workspace(tmp_path)  # first run

    # Modify .env so we can prove it was not overwritten
    env_path = tmp_path / ".env"
    env_path.write_text("MODIFIED=1\n", encoding="utf-8")

    report = init_workspace(tmp_path)  # second run

    actions = _result_map(report)
    assert actions[".env"] == "skipped"
    assert actions["data_sources.toml"] == "skipped"
    assert actions[".gitkeep"] == "skipped"

    # Content must still be the modified value
    assert env_path.read_text(encoding="utf-8") == "MODIFIED=1\n"


# 3. Generated data_sources.toml parses cleanly via load_data_sources_config
def test_generated_config_is_valid(tmp_path: Path) -> None:
    init_workspace(tmp_path)
    config = load_data_sources_config(tmp_path / "config" / "data_sources.toml")
    # Basic sanity: schema_version present and adapters non-empty
    assert config.schema_version == 1
    assert len(config.adapters) > 0
    assert len(config.datasets) > 0


# 4. 5000-credit datasets in generated config have enabled = false
def test_5000_credit_datasets_disabled(tmp_path: Path) -> None:
    init_workspace(tmp_path)
    config = load_data_sources_config(tmp_path / "config" / "data_sources.toml")
    five_k_datasets = [
        ds for ds in config.datasets.values() if ds.credit_tier == 5000
    ]
    assert len(five_k_datasets) > 0, "Expected at least one 5000-credit dataset"
    for ds in five_k_datasets:
        assert not ds.enabled, f"Expected {ds.dataset_id} to be disabled (5000 credits)"


# 5. .env contains TUSHARE_TOKEN= placeholder
def test_env_contains_tushare_token_placeholder(tmp_path: Path) -> None:
    init_workspace(tmp_path)
    env_content = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "TUSHARE_TOKEN=" in env_content
