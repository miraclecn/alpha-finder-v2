"""
init_workspace.py — write starter files into a new workspace directory.

Creates .env, config/data_sources.toml, and output/.gitkeep from packaged
templates if they don't already exist. Existing files are left untouched.
"""
from __future__ import annotations

import importlib.resources
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import alpha_find_v2.data_ingest.templates as _templates_pkg


@dataclass
class FileInitResult:
    path: Path
    action: Literal["created", "skipped"]


@dataclass
class InitReport:
    workspace: Path
    actions: list[FileInitResult] = field(default_factory=list)

    # Alias so callers using the spec name `results` also work.
    @property
    def results(self) -> list[FileInitResult]:
        return self.actions


def init_workspace(workspace: Path) -> InitReport:
    """Write starter files into *workspace* if they are absent.

    Files written:
      - {workspace}/.env               from .env.template
      - {workspace}/config/data_sources.toml  from data_sources.toml.template
      - {workspace}/output/.gitkeep    (empty sentinel)

    Already-present files are reported as action="skipped" and not modified.
    """
    actions: list[FileInitResult] = []

    # --- .env ----------------------------------------------------------------
    env_path = workspace / ".env"
    if env_path.exists():
        actions.append(FileInitResult(path=env_path, action="skipped"))
    else:
        env_path.parent.mkdir(parents=True, exist_ok=True)
        content = importlib.resources.read_text(_templates_pkg, ".env.template", encoding="utf-8")
        env_path.write_text(content, encoding="utf-8")
        actions.append(FileInitResult(path=env_path, action="created"))

    # --- config/data_sources.toml --------------------------------------------
    toml_path = workspace / "config" / "data_sources.toml"
    if toml_path.exists():
        actions.append(FileInitResult(path=toml_path, action="skipped"))
    else:
        toml_path.parent.mkdir(parents=True, exist_ok=True)
        content = importlib.resources.read_text(
            _templates_pkg, "data_sources.toml.template", encoding="utf-8"
        )
        toml_path.write_text(content, encoding="utf-8")
        actions.append(FileInitResult(path=toml_path, action="created"))

    # --- output/.gitkeep -----------------------------------------------------
    gitkeep_path = workspace / "output" / ".gitkeep"
    if gitkeep_path.exists():
        actions.append(FileInitResult(path=gitkeep_path, action="skipped"))
    else:
        gitkeep_path.parent.mkdir(parents=True, exist_ok=True)
        gitkeep_path.touch()
        actions.append(FileInitResult(path=gitkeep_path, action="created"))

    return InitReport(workspace=workspace, actions=actions)
