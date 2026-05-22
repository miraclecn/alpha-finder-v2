"""Top-level mining run orchestrator for factor_lab.

Executes the full pipeline: isolation → config → date validation → DB check →
git SHA → beam search → random sampling → merge → quota → walk-forward → dedup
→ output artifacts → registry append.

Requirements: R11.3, R11.4, R11.6, R11.7
"""

from __future__ import annotations

import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np

from alpha_find_v2.factor_lab.config import load_mining_config
from alpha_find_v2.factor_lab.dedup import run_correlation_dedup
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.git_meta import GitMetadataError, resolve_git_sha
from alpha_find_v2.factor_lab.isolation import OUTPUT_ROOT, assert_output_root_safe
from alpha_find_v2.factor_lab.output import (
    write_audit_md,
    write_candidates_jsonl,
    write_correlation_matrix,
    write_manifest,
    write_shortlist,
)
from alpha_find_v2.factor_lab.quota import apply_family_quota
from alpha_find_v2.factor_lab.registry import append_run_entry
from alpha_find_v2.factor_lab.search.beam import run_beam_search
from alpha_find_v2.factor_lab.search.merge import merge_streams
from alpha_find_v2.factor_lab.search.random_sampler import run_random_sampling
from alpha_find_v2.factor_lab.walk_forward import run_walk_forward

# Synth-fixture budget; full-DB budget is 1800s (recorded only, not enforced).
_SYNTH_BUDGET_SECONDS = 300
_FULL_BUDGET_SECONDS = 1800


def _available_descriptor_ids(conn) -> list[str]:
    """Return registered descriptor IDs whose required tables all exist in conn."""
    from alpha_find_v2.factor_evaluation.descriptor_compute import REGISTRY

    # Fetch table names from the DuckDB connection.
    try:
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
    except Exception:
        existing_tables = set()

    result = []
    for desc_id, spec in REGISTRY.items():
        if desc_id.startswith("__adhoc__"):
            continue
        # Include descriptor if all its required tables are present.
        if all(t in existing_tables for t in spec.requires):
            result.append(desc_id)
    return result


def execute_mining_run(
    research_db: Path,
    start: str,  # YYYYMMDD
    end: str,    # YYYYMMDD
    config_path: Path,
    output_root: Path | None = None,  # defaults to output/factor_lab
    repo_root: Path | None = None,    # defaults to cwd; for testing
) -> dict:  # {"run_id": str, "run_dir": str}
    """Execute one full mining run and return {run_id, run_dir}.

    Exit codes (via sys.exit):
        0  success
        2  config/arg validation error
        4  research_db missing or unreadable
        6  isolation violation

    Args:
        research_db: Path to research_source.duckdb.
        start: Run start date YYYYMMDD (inclusive).
        end: Run end date YYYYMMDD (inclusive).
        config_path: Path to mining config TOML.
        output_root: Override for output/factor_lab root (for testing).
        repo_root: Git repository root (defaults to cwd).

    Returns:
        {"run_id": str, "run_dir": str} on success.
    """
    wall_start = time.monotonic()
    run_at = datetime.now(timezone.utc).isoformat()

    _using_default_root = output_root is None
    if output_root is None:
        output_root = OUTPUT_ROOT

    # ── a. Isolation check (only when using the default output root) ──────
    if _using_default_root:
        assert_output_root_safe(output_root)

    # ── b. Load and validate config ───────────────────────────────────────
    # load_mining_config calls sys.exit(2) on validation error.
    config, _defaults = load_mining_config(config_path)

    # ── c. Validate start/end dates ───────────────────────────────────────
    if len(start) != 8 or not start.isdigit() or len(end) != 8 or not end.isdigit():
        print(
            f"Config error: dates must be YYYYMMDD format; got start={start!r} end={end!r}",
            file=sys.stderr,
        )
        sys.exit(2)
    if start > end:
        print(
            f"Config error: start={start} must be <= end={end}",
            file=sys.stderr,
        )
        sys.exit(2)

    # ── d. Check research_db exists ───────────────────────────────────────
    research_db = Path(research_db)
    if not research_db.exists():
        print(
            f"Missing database: research_db path does not exist: {research_db}",
            file=sys.stderr,
        )
        sys.exit(4)

    # ── e. Resolve git SHA ────────────────────────────────────────────────
    if repo_root is None:
        repo_root = Path.cwd()
    try:
        sha, is_dirty = resolve_git_sha(repo_root)
    except GitMetadataError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    git_sha = f"{sha}-dirty" if is_dirty else sha

    # ── f. Generate run_id and create run_dir ─────────────────────────────
    run_id = uuid.uuid4().hex
    run_dir = output_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # ── g. Init RNG ───────────────────────────────────────────────────────
    rng = np.random.default_rng(config.search.seed)

    try:
        # ── h. Load EvaluationContext (leaf panels loaded lazily) ─────────
        conn = duckdb.connect(str(research_db), read_only=True)
        ctx = EvaluationContext(conn=conn, start_date=start, end_date=end)

        # ── i. Beam search ────────────────────────────────────────────────
        beam_candidates = run_beam_search(
            config=config.search,
            fitness_config=config.fitness,
            ctx=ctx,
            primary_horizon_days=config.walk_forward.primary_horizon_days,
        )

        # ── j. Random sampling (pass RNG continuation) ───────────────────
        random_candidates = run_random_sampling(
            config=config.search,
            fitness_config=config.fitness,
            ctx=ctx,
            primary_horizon_days=config.walk_forward.primary_horizon_days,
            rng=rng,
        )

        # Per-segment frame release (R11.4): clear leaf cache after search.
        ctx._leaf_cache.clear()

        # ── k. Merge streams ──────────────────────────────────────────────
        merge_result = merge_streams(beam_candidates, random_candidates)
        merged = merge_result.candidates

        # ── l. Apply family quota ─────────────────────────────────────────
        admitted, rejected_quota = apply_family_quota(
            merged, config.family.quota_per_family
        )

        # ── m/n. Walk-forward on admitted candidates ───────────────────────
        wf_accepted: list = []
        for cand in admitted:
            if cand.status == "rejected_quota":
                continue
            if cand.fitness is None:
                cand.status = "rejected_oos"
                continue
            try:
                wf_result = run_walk_forward(
                    candidate=cand,
                    walk_fwd_config=config.walk_forward,
                    research_db=research_db,
                    start_date=start,
                    end_date=end,
                    universe_id=config.universe.id,
                )
                cand.status = wf_result.status
                cand.oos_segments = wf_result.oos_segments
                if wf_result.status == "accepted_oos":
                    wf_accepted.append(cand)
            except ValueError:
                cand.status = "rejected_oos"

        # ── o. Correlation dedup ──────────────────────────────────────────
        # Filter to only descriptors whose required tables exist in the DB.
        registered_ids = _available_descriptor_ids(conn)
        dedup_result = run_correlation_dedup(
            candidates=wf_accepted,
            registered_descriptor_ids=registered_ids,
            ctx=ctx,
            dedup_rho=config.dedup.rho_threshold,
            dedup_min_obs=config.dedup.min_obs,
        )

        conn.close()

        # ── Counting ──────────────────────────────────────────────────────
        # random_only: candidates that came only from random (not beam)
        random_only_count = sum(
            1 for c in merge_result.candidates
            if "beam" not in c.sources
        )
        total_evaluated = len(beam_candidates) + random_only_count
        accepted_count = len(dedup_result.admitted)
        rejected_oos_count = sum(
            1 for c in merged if c.status == "rejected_oos"
        )
        rejected_correlation_count = len(dedup_result.rejected_correlation)
        rejected_quota_count = len(rejected_quota)

        # Timing
        elapsed = time.monotonic() - wall_start
        # Use synth budget heuristic: if DB is small (<100MB) use synth budget.
        db_size = research_db.stat().st_size
        budget = _SYNTH_BUDGET_SECONDS if db_size < 100 * 1024 * 1024 else _FULL_BUDGET_SECONDS

        families_present = sorted({
            c.family for c in dedup_result.admitted if c.family is not None
        })

        # ── p. Write all 5 artifacts ──────────────────────────────────────
        warnings: list[str] = []
        if merge_result.beam_underperforms_random:
            warnings.append("beam_underperforms_random")
        if elapsed > 2 * budget:
            warnings.append("time_budget_exceeded")

        run_metadata = {
            "run_id": run_id,
            "run_at": run_at,
            "git_sha": git_sha,
            "research_db": str(research_db),
            "start_date": start,
            "end_date": end,
            "config_snapshot": config.to_snapshot_dict(),
            "total_candidates_evaluated": total_evaluated,
            "accepted_count": accepted_count,
            "rejected_oos_count": rejected_oos_count,
            "rejected_correlation_count": rejected_correlation_count,
            "rejected_quota_count": rejected_quota_count,
            "families_present": families_present,
            "duration_seconds": round(elapsed, 3),
            "warnings": warnings,
        }

        write_manifest(run_dir, run_metadata)
        write_candidates_jsonl(run_dir, merged)
        write_shortlist(run_dir, dedup_result.admitted)

        # Correlation matrix column order: registered descriptors + admitted canonicals
        matrix_col_ids = registered_ids + [
            c.canonical for c in dedup_result.admitted
        ]
        write_correlation_matrix(
            run_dir,
            dedup_result.matrix,
            wf_accepted,
            matrix_col_ids,
        )
        write_audit_md(run_dir, dedup_result.admitted)

        # ── q. Append to registry ─────────────────────────────────────────
        registry_path = output_root / "registry.json"
        append_run_entry(
            run_id=run_id,
            run_at=run_at,
            run_dir=run_dir,
            candidate_count=total_evaluated,
            accepted_count=accepted_count,
            families_present=families_present,
            registry_path=registry_path,
        )

    except Exception:
        # Do not append to registry on error; re-raise.
        raise

    return {"run_id": run_id, "run_dir": str(run_dir)}
