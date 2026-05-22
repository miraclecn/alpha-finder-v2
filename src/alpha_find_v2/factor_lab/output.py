"""Output writer for factor mining sandbox runs.

Writes five artifacts under a run directory:
  - manifest.json      (R7.1)
  - candidates.jsonl   (R7.2)
  - shortlist.json     (R7.3)
  - correlation_matrix.csv (R7.4)
  - audit.md           (R7.5, R8.6)

All path strings in artifacts use POSIX forward slashes (R7.10).
Writes are atomic-ish: write to .tmp then rename (R7.7).

Requirements: R7.1–R7.5, R7.7, R7.10
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alpha_find_v2.factor_lab.search.beam import Candidate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _atomic_write_text(path: Path, text: str) -> None:
    """Write *text* to *path* atomically via a temp file in the same directory (R7.7)."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        Path(tmp_path).rename(path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _to_posix(s: str) -> str:
    """Convert OS path separators to POSIX forward slashes (R7.10)."""
    return s.replace(os.sep, "/")


def _candidate_dict(c: "Candidate") -> dict:
    """Serialize a Candidate to the candidates.jsonl schema (R7.2)."""
    return {
        "expr_id": c.expr_id,
        "expression": c.canonical,
        "node_count": c.node_count,
        "family": c.family,
        "sources": list(c.sources),
        "train_ic_ir": c.train_ic_ir,
        "fitness": c.fitness,
        "oos_segments": list(c.oos_segments),
        "status": c.status,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_manifest(run_dir: Path, run_metadata: dict) -> None:
    """Write manifest.json under *run_dir* (R7.1).

    *run_metadata* must contain all required keys as documented in R7.1.
    """
    path = run_dir / "manifest.json"
    text = json.dumps(run_metadata, indent=2, ensure_ascii=False) + "\n"
    _atomic_write_text(path, text)


def write_candidates_jsonl(run_dir: Path, candidates: list["Candidate"]) -> None:
    """Write candidates.jsonl under *run_dir* (R7.2).

    One JSON object per line, UTF-8, \\n-terminated.
    """
    path = run_dir / "candidates.jsonl"
    lines = [json.dumps(_candidate_dict(c), ensure_ascii=False) + "\n" for c in candidates]
    _atomic_write_text(path, "".join(lines))


def write_shortlist(run_dir: Path, accepted_candidates: list["Candidate"]) -> None:
    """Write shortlist.json under *run_dir* (R7.3).

    Only ``status == "accepted_oos"`` candidates, ordered by fitness desc,
    expr_id asc as tiebreaker, each row including ``family_rank``.
    """
    path = run_dir / "shortlist.json"

    # Sort: fitness desc, expr_id asc as tiebreaker
    sorted_candidates = sorted(
        accepted_candidates,
        key=lambda c: (-(c.fitness if c.fitness is not None else float("-inf")), c.expr_id),
    )

    # Compute family_rank within each family using the same ordering
    family_counter: dict[str | None, int] = {}
    entries = []
    for c in sorted_candidates:
        fam = c.family
        family_counter[fam] = family_counter.get(fam, 0) + 1
        row = _candidate_dict(c)
        row["family_rank"] = family_counter[fam]
        entries.append(row)

    text = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"
    _atomic_write_text(path, text)


def write_correlation_matrix(
    run_dir: Path,
    matrix: dict[str, dict[str, float | None]],
    candidates: list["Candidate"],
    reference_ids: list[str],
) -> None:
    """Write correlation_matrix.csv under *run_dir* (R7.4).

    Args:
        run_dir: Output directory.
        matrix: Outer key = candidate canonical string, inner key = reference id,
                value = Pearson r (float) or None (undefined → empty string).
        candidates: All evaluated candidates in evaluation order (rows keyed by expr_id).
        reference_ids: Column ids in order: descriptors first, then accepted candidates.
    """
    path = run_dir / "correlation_matrix.csv"

    # Build canonical → expr_id map
    canon_to_id = {c.canonical: c.expr_id for c in candidates}

    rows: list[list[str]] = []
    # Header: expr_id + one column per reference id
    header = ["expr_id"] + list(reference_ids)
    rows.append(header)

    for c in candidates:
        inner = matrix.get(c.canonical, {})
        row = [c.expr_id]
        for ref_id in reference_ids:
            val = inner.get(ref_id)
            if val is None:
                row.append("")
            else:
                row.append(f"{val:.6f}")
        rows.append(row)

    # Build CSV string
    import io
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    _atomic_write_text(path, buf.getvalue())


def write_audit_md(run_dir: Path, accepted_candidates: list["Candidate"]) -> None:
    """Write audit.md under *run_dir* (R7.5, R8.6).

    One section per accepted candidate plus a Promotion Path section.
    """
    path = run_dir / "audit.md"

    # Sort same as shortlist: fitness desc, expr_id asc
    sorted_candidates = sorted(
        accepted_candidates,
        key=lambda c: (-(c.fitness if c.fitness is not None else float("-inf")), c.expr_id),
    )

    lines: list[str] = ["# Factor Mining Audit Report\n\n"]

    for c in sorted_candidates:
        lines.append(f"## {c.expr_id}\n\n")
        lines.append(f"- **expression**: `{c.canonical}`\n")
        lines.append(f"- **family**: {c.family}\n")
        lines.append(f"- **node_count**: {c.node_count}\n")
        lines.append("\n### OOS Segments\n\n")

        for seg in c.oos_segments:
            k = seg.get("segment", "?")
            train_start = seg.get("train_start", "")
            train_end = seg.get("train_end", "")
            oos_start = seg.get("oos_start", "")
            oos_end = seg.get("oos_end", "")
            oos_ic_ir = seg.get("oos_ic_ir", "")
            oos_ic_mean = seg.get("oos_ic_mean", "")
            lines.append(
                f"- Segment {k}: train [{train_start}–{train_end}]"
                f" OOS [{oos_start}–{oos_end}]"
                f" IC_IR={oos_ic_ir} IC_mean={oos_ic_mean}\n"
            )

        lines.append("\n### Research Notes\n\n")
        lines.append("- **economic_story**: TODO\n")
        lines.append("- **risk_notes**: TODO\n")
        lines.append("- **suggested_promote**: needs_more_data\n")
        lines.append("\n")

    # Promotion Path section (R8.6)
    lines.append("---\n\n")
    lines.append("## Promotion Path\n\n")
    lines.append(
        "To promote a candidate from sandbox to production, follow these manual steps:\n\n"
    )
    lines.append(
        "a. **Human authoring**: Fill in `economic_story` and `risk_notes` above "
        "with a clear rationale for why this factor should be promoted.\n\n"
    )
    lines.append(
        "b. **Open a PR**: Add `config/descriptors/<expr_id>.toml` defining the descriptor "
        "configuration and a corresponding compute function in the descriptor registry. "
        "Reference this audit report in the PR description.\n\n"
    )
    lines.append(
        "c. **Human review and merge**: A second researcher reviews the economic story, "
        "risk notes, and OOS metrics before approving and merging the PR.\n"
    )

    _atomic_write_text(path, "".join(lines))
