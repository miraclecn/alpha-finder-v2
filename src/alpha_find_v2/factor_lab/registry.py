"""Registry for factor mining sandbox runs.

Reads and writes ``output/factor_lab/registry.json`` — a JSON array of
run-summary entries, preserved in insertion order with newest entries
appended at the end.

Public API
----------
append_run_entry(...)  -- append one entry atomically (R7.6)
list_runs(...)         -- filter + sort entries (R1.5, R1.6, R1.7, R1.12)

Requirements: R1.5, R1.6, R1.7, R1.12, R7.6, R7.7
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from statistics import mean
from typing import Any


_REGISTRY_PATH = Path("output/factor_lab/registry.json")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_registry(registry_path: Path) -> list[dict[str, Any]]:
    """Return entries from *registry_path*, or [] if missing/empty."""
    if not registry_path.exists():
        return []
    text = registry_path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    return json.loads(text)  # type: ignore[return-value]


def _atomic_write_json(path: Path, data: list[dict[str, Any]]) -> None:
    """Write *data* as JSON to *path* atomically (temp + rename, R7.7)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _mean_oos_ic_ir(shortlist_entry: dict[str, Any]) -> float | None:
    """Return mean OOS IC_IR across all segments for *shortlist_entry*, or None."""
    segments = shortlist_entry.get("oos_segments", [])
    values = [s["oos_ic_ir"] for s in segments if "oos_ic_ir" in s]
    if not values:
        return None
    return mean(values)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def append_run_entry(
    run_id: str,
    run_at: str,
    run_dir: Path,
    candidate_count: int,
    accepted_count: int,
    families_present: list[str],
    registry_path: Path = _REGISTRY_PATH,
) -> None:
    """Append one entry to the registry JSON, preserving prior order (R7.6).

    ``run_dir`` is stored as a POSIX path (forward slashes, R7.10).
    The write is atomic: the registry is not modified if an error occurs (R7.7).
    """
    entries = _read_registry(registry_path)
    entries.append(
        {
            "run_id": run_id,
            "run_at": run_at,
            "run_dir": run_dir.as_posix(),
            "candidate_count": candidate_count,
            "accepted_count": accepted_count,
            "families_present": list(families_present),
        }
    )
    _atomic_write_json(registry_path, entries)


def list_runs(
    family: str | None,
    min_ic_ir: float | None,
    shortlist_dir_base: Path,
    registry_path: Path = _REGISTRY_PATH,
) -> list[dict[str, Any]]:
    """Return run entries, filtered and sorted by ``run_at`` descending (R1.5–R1.7).

    Filters (applied as AND):
    - ``family``: keep only entries whose ``families_present`` contains *family*
      (case-sensitive).
    - ``min_ic_ir``: keep only entries whose shortlist contains at least one
      candidate with mean OOS IC_IR >= *min_ic_ir*.  If the run's
      ``shortlist.json`` does not exist, skip that run for this filter.

    Missing or empty registry → return [] (R1.12).
    """
    entries = _read_registry(registry_path)
    if not entries:
        return []

    result: list[dict[str, Any]] = []

    for entry in entries:
        # --- family filter (case-sensitive) ---
        if family is not None:
            if family not in entry.get("families_present", []):
                continue

        # --- min_ic_ir filter ---
        if min_ic_ir is not None:
            run_dir = shortlist_dir_base / entry["run_dir"]
            shortlist_path = run_dir / "shortlist.json"
            if not shortlist_path.exists():
                continue
            shortlist: list[dict[str, Any]] = json.loads(
                shortlist_path.read_text(encoding="utf-8")
            )
            qualifies = any(
                (ic := _mean_oos_ic_ir(c)) is not None and ic >= min_ic_ir
                for c in shortlist
            )
            if not qualifies:
                continue

        result.append(entry)

    # Sort by run_at descending
    result.sort(key=lambda e: e["run_at"], reverse=True)
    return result
