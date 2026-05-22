"""Inspect-candidate handler for factor_lab.

Runs a full Stage 2 evaluation on a single sandbox candidate and writes the
evaluation report to the run's inspections directory.

Requirements: R1.8, R1.9, R1.10, R1.13, R7.8
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from alpha_find_v2.factor_evaluation import report_writer
from alpha_find_v2.factor_evaluation.descriptor_evaluator import evaluate_descriptor
from alpha_find_v2.factor_lab.dsl import parser
from alpha_find_v2.factor_lab.dsl.validator import RejectionRecord
from alpha_find_v2.factor_lab.walk_forward import _make_adhoc_spec, _temporary_registration


def run_inspection(
    run_id: str,
    expr_id: str,
    output_root: Path,
    research_db: Path,
) -> None:
    """Run a full Stage 2 evaluation on a sandbox candidate.

    Side effects: writes to output_root/runs/<run_id>/inspections/<expr_id>/
    Exit codes: 0 success, 4 missing run, 5 missing expr_id, 3 pipeline failure
    """
    run_dir = output_root / "runs" / run_id

    # R1.9: validate run_dir exists
    if not run_dir.exists():
        print(str(run_dir), file=sys.stderr)
        sys.exit(4)

    # Read candidates.jsonl and find the target expr
    candidates_path = run_dir / "candidates.jsonl"
    candidates: list[dict] = []
    if candidates_path.exists():
        for line in candidates_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                candidates.append(json.loads(line))

    # R1.10: validate expr_id present
    candidate: dict | None = None
    for c in candidates:
        if c.get("expr_id") == expr_id:
            candidate = c
            break

    if candidate is None:
        known_ids = [c.get("expr_id", "") for c in candidates]
        print(str(known_ids), file=sys.stderr)
        sys.exit(5)

    # Read manifest for start/end dates and universe
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    start_date = manifest["start_date"]
    end_date = manifest["end_date"]
    universe = (
        manifest.get("config_snapshot", {})
        .get("universe", {})
        .get("id", "csi800")
    )

    # Parse the canonical expression into an AST
    ast_result = parser.parse(candidate["expression"])
    if isinstance(ast_result, RejectionRecord):
        print(f"pipeline failure in stage 'dsl_parser': {ast_result.reason}", file=sys.stderr)
        sys.exit(3)

    # Build ad-hoc spec (same pattern as walk_forward.py)
    spec = _make_adhoc_spec(ast_result)

    # Inspection output directory (R7.8)
    inspection_dir = run_dir / "inspections" / expr_id

    # R1.8, R1.13: run evaluation; clean up partial dir on failure
    stage = "evaluate_descriptor"
    try:
        with _temporary_registration(spec):
            report = evaluate_descriptor(
                descriptor_id=spec.descriptor_id,
                research_db=research_db,
                universe=universe,
                start_date=start_date,
                end_date=end_date,
            )
        stage = "report_writer"
        report_writer.write_report(report, out_dir=inspection_dir)
    except Exception as exc:
        # R1.13: remove any partially-created inspection dir
        if inspection_dir.exists():
            shutil.rmtree(inspection_dir, ignore_errors=True)
        print(f"pipeline failure in stage {stage!r}: {exc}", file=sys.stderr)
        sys.exit(3)

    sys.exit(0)
