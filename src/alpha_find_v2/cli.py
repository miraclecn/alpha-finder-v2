from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import json
import sys
from pathlib import Path

from .config_loader import (
    CONFIG_ROOT,
    list_configs,
    load_cost_model,
    load_decay_monitor,
    load_descriptor,
    load_descriptor_set,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
    load_promotion_gate,
    load_risk_model,
    load_sleeve,
    load_target,
    load_thesis,
)
from .research_artifact_builder import build_sleeve_artifact, write_sleeve_artifact
from .deployment import DecayMonitorEvaluator, ExecutableSignalBuilder, build_run_manifest, write_run_manifest
from .deployment_loader import (
    load_decay_watch_case,
    load_executable_signal_case,
    load_portfolio_state_snapshot,
    load_run_manifest_case,
)
from .live_state import (
    account_state_to_portfolio_state,
    load_account_state_snapshot,
    load_benchmark_state_artifact,
)
from .market_data_bootstrap import build_research_source_db
from .benchmark_state_builder import (
    build_benchmark_state_artifact,
    load_benchmark_state_build_case,
    write_benchmark_state_artifact,
)
from .reference_data_staging import (
    BenchmarkReferenceDefinition,
    build_tushare_reference_db,
)
from .portfolio_constructor import PortfolioConstructor
from .portfolio_promotion_replay import (
    PortfolioPromotionReplay,
    PortfolioPromotionReplayResult,
)
from .research_artifact_loader import (
    load_sleeve_artifact_build_case,
    load_portfolio_promotion_replay_case,
    load_sleeve_artifact,
)
from .fundamental_research_input_builder import (
    build_fundamental_research_observation_input,
    load_fundamental_research_input_build_case,
    write_fundamental_research_observation_input,
)
from .trend_research_input_builder import (
    build_trend_research_observation_input,
    load_trend_research_input_build_case,
    write_trend_research_observation_input,
)


def _dump_json(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _promotion_replay_research_evidence_payload(
    result: PortfolioPromotionReplayResult,
) -> dict[str, object]:
    return {
        "baseline_summary": asdict(result.baseline_summary),
        "candidate_summary": asdict(result.candidate_summary),
        "marginal": asdict(result.marginal),
        "diagnostics": asdict(result.diagnostics),
        "walk_forward": asdict(result.walk_forward) if result.walk_forward is not None else None,
        "regime_breakdown": (
            asdict(result.regime_breakdown)
            if result.regime_breakdown is not None
            else None
        ),
        "regime_overlay": (
            asdict(result.regime_overlay)
            if result.regime_overlay is not None
            else None
        ),
        "market_data_quality": (
            asdict(result.market_data_quality)
            if result.market_data_quality is not None
            else None
        ),
    }


def _build_executable_signal_package(loaded_case: object) -> tuple[object, object]:
    construction_step = PortfolioConstructor(
        mandate=loaded_case.mandate,
        portfolio=loaded_case.portfolio,
        construction_model=loaded_case.construction_model,
    ).build([loaded_case.construction_input]).steps[0]
    package = ExecutableSignalBuilder(
        mandate=loaded_case.mandate,
        portfolio=loaded_case.portfolio,
        execution_policy=loaded_case.execution_policy,
        default_cost_model=loaded_case.default_cost_model,
        cost_models=loaded_case.cost_models,
        portfolio_overlay=loaded_case.regime_overlay,
    ).build(
        trade_date=loaded_case.definition.trade_date,
        execution_date=loaded_case.definition.execution_date,
        signals=construction_step.signals,
        portfolio_state=loaded_case.portfolio_state,
        regime_overlay_decision=loaded_case.regime_overlay_decision,
    )
    return construction_step, package


def _parse_benchmark_reference(value: str) -> BenchmarkReferenceDefinition:
    benchmark_id, separator, index_code = value.partition("=")
    if not separator or not benchmark_id.strip() or not index_code.strip():
        raise ValueError(
            "Benchmark references must use '<benchmark_id>=<index_code>', "
            f"got: {value}"
        )
    return BenchmarkReferenceDefinition(
        benchmark_id=benchmark_id.strip(),
        index_code=index_code.strip(),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect alpha-find-v2 research objects.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-descriptors", help="List descriptor config files.")
    subparsers.add_parser("list-descriptor-sets", help="List descriptor-set config files.")
    subparsers.add_parser("list-cost-models", help="List cost-model config files.")
    subparsers.add_parser("list-execution-policies", help="List execution-policy config files.")
    subparsers.add_parser("list-decay-monitors", help="List decay-monitor config files.")
    subparsers.add_parser(
        "list-portfolio-construction-models",
        help="List portfolio-construction config files.",
    )
    subparsers.add_parser("list-risk-models", help="List risk-model config files.")
    subparsers.add_parser("list-theses", help="List thesis config files.")

    show_descriptor = subparsers.add_parser("show-descriptor", help="Show a descriptor config.")
    show_descriptor.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "descriptors" / "sector_relative_valuation.toml"),
        help="Path to the descriptor TOML file.",
    )

    show_descriptor_set = subparsers.add_parser("show-descriptor-set", help="Show a descriptor-set config.")
    show_descriptor_set.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "descriptor_sets" / "trend_leadership_core.toml"),
        help="Path to the descriptor-set TOML file.",
    )

    show_mandate = subparsers.add_parser("show-mandate", help="Show a mandate config.")
    show_mandate.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "mandates" / "a_share_long_only_eod.toml"),
        help="Path to the mandate TOML file.",
    )

    show_thesis = subparsers.add_parser("show-thesis", help="Show a thesis config.")
    show_thesis.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "theses" / "trend_leadership.toml"),
        help="Path to the thesis TOML file.",
    )

    show_target = subparsers.add_parser("show-target", help="Show an executable target config.")
    show_target.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_residual_net_cost.toml"),
        help="Path to the target TOML file.",
    )

    show_risk_model = subparsers.add_parser("show-risk-model", help="Show a risk model config.")
    show_risk_model.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "risk_models" / "a_share_core_equity.toml"),
        help="Path to the risk-model TOML file.",
    )

    show_cost_model = subparsers.add_parser("show-cost-model", help="Show a cost model config.")
    show_cost_model.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml"),
        help="Path to the cost model TOML file.",
    )

    show_execution_policy = subparsers.add_parser(
        "show-execution-policy",
        help="Show an execution-policy config.",
    )
    show_execution_policy.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "execution_policies" / "a_share_next_open_v1.toml"),
        help="Path to the execution-policy TOML file.",
    )

    show_benchmark_state = subparsers.add_parser(
        "show-benchmark-state",
        help="Show a benchmark state history artifact.",
    )
    show_benchmark_state.add_argument(
        "--path",
        default="research/examples/promotion_replay_minimal/benchmark_state_history.json",
        help="Path to the benchmark-state JSON file.",
    )

    show_account_state = subparsers.add_parser(
        "show-account-state",
        help="Show an account state snapshot and its derived portfolio state.",
    )
    show_account_state.add_argument(
        "--path",
        default="research/examples/deployment_minimal/account_state_2026_04_20.json",
        help="Path to the account-state JSON file.",
    )
    show_account_state.add_argument(
        "--portfolio-id",
        default="research_example_candidate_portfolio",
        help="Portfolio id used when adapting account state into a portfolio state snapshot.",
    )

    show_portfolio_state = subparsers.add_parser(
        "show-portfolio-state",
        help="Show a portfolio state snapshot.",
    )
    show_portfolio_state.add_argument(
        "--path",
        default="research/examples/deployment_minimal/portfolio_state_2026_04_20.json",
        help="Path to the portfolio-state JSON file.",
    )

    show_decay_monitor = subparsers.add_parser(
        "show-decay-monitor",
        help="Show a decay-monitor config.",
    )
    show_decay_monitor.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "decay_monitors" / "a_share_core_watch.toml"),
        help="Path to the decay-monitor TOML file.",
    )

    show_sleeve = subparsers.add_parser("show-sleeve", help="Show a sleeve config.")
    show_sleeve.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "sleeves" / "trend_leadership_core.toml"),
        help="Path to the sleeve TOML file.",
    )

    show_portfolio = subparsers.add_parser("show-portfolio", help="Show a portfolio recipe.")
    show_portfolio.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "portfolio" / "a_share_core.toml"),
        help="Path to the portfolio TOML file.",
    )

    show_portfolio_construction = subparsers.add_parser(
        "show-portfolio-construction-model",
        help="Show a portfolio-construction config.",
    )
    show_portfolio_construction.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "portfolio_construction" / "a_share_core_blend.toml"),
        help="Path to the portfolio-construction TOML file.",
    )

    show_promotion_gate = subparsers.add_parser("show-promotion-gate", help="Show a promotion gate config.")
    show_promotion_gate.add_argument(
        "--path",
        default=str(CONFIG_ROOT / "promotion_gates" / "a_share_core_portfolio_gate.toml"),
        help="Path to the promotion gate TOML file.",
    )

    show_sleeve_artifact = subparsers.add_parser(
        "show-sleeve-artifact",
        help="Show a persisted sleeve research artifact.",
    )
    show_sleeve_artifact.add_argument(
        "--path",
        default="research/examples/promotion_replay_minimal/sleeve_artifacts/trend_leadership_core.json",
        help="Path to the sleeve artifact JSON file.",
    )

    build_sleeve_artifact_cmd = subparsers.add_parser(
        "build-sleeve-artifact",
        help="Build a persisted sleeve research artifact from normalized research observations.",
    )
    build_sleeve_artifact_cmd.add_argument(
        "--case",
        default="research/examples/artifact_build_minimal/trend_leadership_core.toml",
        help="Path to the sleeve-artifact build-case TOML file.",
    )

    build_research_source_db_cmd = subparsers.add_parser(
        "build-research-source-db",
        help="Build an isolated V2 research-source DuckDB from the audited V1 market database.",
    )
    build_research_source_db_cmd.add_argument(
        "--source-db",
        default="/home/nan/alpha-find/output/stock_data_audited.duckdb",
        help="Path to the populated V1 audited DuckDB file.",
    )
    build_research_source_db_cmd.add_argument(
        "--target-db",
        default="output/research_source.duckdb",
        help="Path to the V2 research-source DuckDB file to create or refresh.",
    )
    build_research_source_db_cmd.add_argument(
        "--supplemental-db",
        default="",
        help="Optional DuckDB containing audited PIT reference tables such as industry_classification_pit, benchmark_membership_pit, and benchmark_weight_snapshot_pit.",
    )

    build_reference_staging_db = subparsers.add_parser(
        "build-reference-staging-db",
        help="Build a supplemental DuckDB of PIT benchmark and industry reference tables from Tushare.",
    )
    build_reference_staging_db.add_argument(
        "--target-db",
        default="output/pit_reference_staging.duckdb",
        help="Path to the supplemental DuckDB file to create or refresh.",
    )
    build_reference_staging_db.add_argument(
        "--start-date",
        default="20140101",
        help="Earliest snapshot date to request from Tushare in YYYYMMDD format.",
    )
    build_reference_staging_db.add_argument(
        "--end-date",
        default="",
        help="Latest snapshot date to request from Tushare in YYYYMMDD format. Defaults to today.",
    )
    build_reference_staging_db.add_argument(
        "--benchmark",
        action="append",
        default=[],
        help="Benchmark reference mapping in the form '<benchmark_id>=<index_code>'. Defaults to CSI 800=000906.SH.",
    )
    build_reference_staging_db.add_argument(
        "--industry-level",
        action="append",
        choices=["L1", "L2", "L3"],
        default=[],
        help="SW2021 industry levels to stage. Defaults to L1,L2,L3.",
    )
    build_reference_staging_db.add_argument(
        "--index-weight-window-months",
        type=int,
        default=1,
        help="Month window size used to chunk index_weight requests and avoid Tushare truncation.",
    )
    build_reference_staging_db.add_argument(
        "--token",
        default="",
        help="Optional explicit Tushare token. Falls back to TUSHARE_TOKEN or the legacy collector .env.",
    )

    build_trend_research_input = subparsers.add_parser(
        "build-trend-research-input",
        help="Build weekly trend_leadership research observations from the isolated V2 DuckDB.",
    )
    build_trend_research_input.add_argument(
        "--case",
        default="research/examples/trend_input_build_minimal/trend_leadership_core.toml",
        help="Path to the trend-research input build-case TOML file.",
    )

    build_fundamental_research_input = subparsers.add_parser(
        "build-fundamental-research-input",
        help="Build weekly fundamental_rerating research observations from the isolated V2 DuckDB.",
    )
    build_fundamental_research_input.add_argument(
        "--case",
        default="research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml",
        help="Path to the fundamental-research input build-case TOML file.",
    )

    build_benchmark_state = subparsers.add_parser(
        "build-benchmark-state",
        help="Build a PIT benchmark_state_history artifact from the isolated V2 DuckDB.",
    )
    build_benchmark_state.add_argument(
        "--case",
        default="research/examples/benchmark_state_build_minimal/csi800.toml",
        help="Path to the benchmark-state build-case TOML file.",
    )

    run_promotion_replay = subparsers.add_parser(
        "run-promotion-replay",
        help="Run a promotion replay case from persisted research artifacts.",
    )
    run_promotion_replay.add_argument(
        "--case",
        default="research/examples/promotion_replay_minimal/replay_case.toml",
        help="Path to the replay-case TOML file.",
    )

    build_executable_signal = subparsers.add_parser(
        "build-executable-signal",
        help="Build an executable signal package from a deployment case.",
    )
    build_executable_signal.add_argument(
        "--case",
        default="research/examples/deployment_minimal/executable_signal_case.toml",
        help="Path to the executable-signal case TOML file.",
    )

    evaluate_decay_watch = subparsers.add_parser(
        "evaluate-decay-watch",
        help="Evaluate a decay-watch case into a decay record.",
    )
    evaluate_decay_watch.add_argument(
        "--case",
        default="research/examples/deployment_minimal/decay_watch_case.toml",
        help="Path to the decay-watch case TOML file.",
    )

    # ---- Audit commands -------------------------------------------------

    audit_market_data_quality = subparsers.add_parser(
        "audit-market-data-quality",
        help="Write a JSON quality audit for a V2 research-source DuckDB.",
    )
    audit_market_data_quality.add_argument(
        "--source-db",
        default="output/research_source.duckdb",
        help="Path to the V2 research-source DuckDB file to audit.",
    )
    audit_market_data_quality.add_argument(
        "--output",
        default=f"output/audits/market_data_quality_{date.today().strftime('%Y%m%d')}.json",
        help="Path to write the market-data quality JSON audit.",
    )

    build_regime_overlay_observations = subparsers.add_parser(
        "build-regime-overlay-observations",
        help="Build a regime overlay observation history from PIT benchmark and daily-bar inputs.",
    )
    build_regime_overlay_observations.add_argument(
        "--case",
        default=(
            "research/examples/deployment_minimal/"
            "trend_live_candidate_overlay_observation_build.toml"
        ),
        help="Path to the regime-overlay observation build-case TOML file.",
    )

    build_run_manifest_cmd = subparsers.add_parser(
        "build-run-manifest",
        help="Build a run manifest from an executable-signal deployment case.",
    )
    build_run_manifest_cmd.add_argument(
        "--case",
        default="research/examples/deployment_minimal/run_manifest_case.toml",
        help="Path to the run-manifest case TOML file.",
    )

    run_portfolio_backtest = subparsers.add_parser(
        "run-portfolio-backtest",
        help="Run a daily portfolio-level backtest case from persisted research artifacts.",
    )
    run_portfolio_backtest.add_argument(
        "--case",
        default="research/examples/deployment_minimal/trend_live_candidate_portfolio_backtest.toml",
        help="Path to the portfolio-backtest case TOML file.",
    )

    validate_live_candidate_bundle = subparsers.add_parser(
        "validate-live-candidate-bundle",
        help="Validate a frozen live-candidate bundle for shadow-live use.",
    )
    validate_live_candidate_bundle.add_argument(
        "--path",
        default="research/examples/deployment_minimal/trend_leadership_live_candidate_v1.toml",
        help="Path to the live-candidate bundle TOML file.",
    )

    evaluate_shadow_live_journal_cmd = subparsers.add_parser(
        "evaluate-shadow-live-journal",
        help="Evaluate whether a shadow-live journal meets the live-readiness gate.",
    )
    evaluate_shadow_live_journal_cmd.add_argument(
        "--path",
        default="research/examples/deployment_minimal/shadow_live_journal_trend_leadership_v1.json",
        help="Path to the shadow-live journal JSON file.",
    )

    evaluate_multi_year_validation_audit_cmd = subparsers.add_parser(
        "evaluate-multi-year-validation-audit",
        help="Evaluate whether the frozen candidate has a sufficient multi-year audited validation window.",
    )
    evaluate_multi_year_validation_audit_cmd.add_argument(
        "--path",
        default="research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.json",
        help="Path to the multi-year validation audit JSON file.",
    )

    build_multi_year_validation_audit_cmd = subparsers.add_parser(
        "build-multi-year-validation-audit",
        help="Build a reproducible multi-year validation audit artifact from benchmark, trend, and replay evidence.",
    )
    build_multi_year_validation_audit_cmd.add_argument(
        "--case",
        default=(
            "research/examples/deployment_minimal/"
            "trend_leadership_multi_year_validation_audit_v1.toml"
        ),
        help="Path to the multi-year validation audit build-case TOML file.",
    )

    # ---- Data ingestion commands ----------------------------------------

    init_cmd = subparsers.add_parser(
        "init",
        help="Initialise the workspace: create .env, config/data_sources.toml, output/.",
    )
    init_cmd.add_argument(
        "--workspace",
        default=".",
        help="Workspace root directory (default: current directory).",
    )

    sync_cmd = subparsers.add_parser(
        "sync",
        help="Sync A-share data from Tushare (or fallback) into output/raw.duckdb.",
    )
    sync_cmd.add_argument(
        "--raw-db",
        default="output/raw.duckdb",
        help="Path to the raw DuckDB file (created if absent).",
    )
    sync_cmd.add_argument(
        "--config",
        default="config/data_sources.toml",
        help="Path to data_sources.toml.",
    )
    sync_cmd.add_argument(
        "--only",
        default="",
        help="Comma-separated dataset ids to sync (default: all enabled).",
    )
    sync_cmd.add_argument(
        "--reset",
        default="",
        help="Comma-separated dataset ids to reset (clear state + raw table) before sync.",
    )
    sync_cmd.add_argument(
        "--since",
        default="",
        help="Override start date YYYYMMDD.",
    )
    sync_cmd.add_argument(
        "--until",
        default="",
        help="End date YYYYMMDD (default: today).",
    )
    sync_cmd.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Plan sync without making any API calls.",
    )

    audit_cmd = subparsers.add_parser(
        "audit-data",
        help="Run data quality checks on output/raw.duckdb.",
    )
    audit_cmd.add_argument(
        "--raw-db",
        default="output/raw.duckdb",
        help="Path to the raw DuckDB file.",
    )
    audit_cmd.add_argument(
        "--out-dir",
        default="output/audit",
        help="Directory for audit reports.",
    )

    # ---- Factor evaluation commands (Stage 2) ---------------------------

    compute_desc_cmd = subparsers.add_parser(
        "compute-descriptor",
        help="Compute descriptor values from the research database.",
    )
    compute_desc_cmd.add_argument("--id", required=True, help="Descriptor id.")
    compute_desc_cmd.add_argument(
        "--research-db", default="output/research_source.duckdb",
        help="Path to research_source.duckdb.",
    )
    compute_desc_cmd.add_argument("--start", default="", help="Start date YYYYMMDD.")
    compute_desc_cmd.add_argument("--end", default="", help="End date YYYYMMDD.")
    compute_desc_cmd.add_argument("--out", default="", help="Optional output Parquet path.")
    compute_desc_cmd.add_argument("--universe", default="", help="Optional universe filter id.")

    evaluate_desc_cmd = subparsers.add_parser(
        "evaluate-descriptor",
        help="Evaluate a descriptor with IC, decile, and other metrics.",
    )
    evaluate_desc_cmd.add_argument("--id", required=True, help="Descriptor id.")
    evaluate_desc_cmd.add_argument(
        "--research-db", default="output/research_source.duckdb",
        help="Path to research_source.duckdb.",
    )
    evaluate_desc_cmd.add_argument("--raw-db", default="output/raw.duckdb",
                                   help="Path to raw.duckdb (for tradeability).")
    evaluate_desc_cmd.add_argument("--universe", default="csi800",
                                   help="Universe id: csi800 | investable_a_share_core.")
    evaluate_desc_cmd.add_argument("--start", default="", help="Start date YYYYMMDD.")
    evaluate_desc_cmd.add_argument("--end", default="", help="End date YYYYMMDD.")
    evaluate_desc_cmd.add_argument("--horizons", default="5,20,60",
                                   help="Comma-separated horizon days.")
    evaluate_desc_cmd.add_argument("--primary-horizon", type=int, default=20,
                                   help="Primary horizon for summary (default 20).")
    evaluate_desc_cmd.add_argument("--correlation-against", default="",
                                   help="Comma-separated descriptor ids for correlation.")
    evaluate_desc_cmd.add_argument("--cost-model", default="",
                                   help="Path to cost model TOML.")
    evaluate_desc_cmd.add_argument("--weighting", default="equal",
                                   choices=["equal", "rank"])
    evaluate_desc_cmd.add_argument("--include-untradeable", action="store_true",
                                   default=False)
    evaluate_desc_cmd.add_argument(
        "--out-dir", default="output/descriptor_evaluation",
        help="Directory for evaluation reports.",
    )

    list_reports_cmd = subparsers.add_parser(
        "list-evaluation-reports",
        help="List past descriptor evaluation reports.",
    )
    list_reports_cmd.add_argument("--id", default="", help="Filter to a specific descriptor id.")
    list_reports_cmd.add_argument(
        "--out-dir", default="output/descriptor_evaluation",
        help="Directory containing evaluation reports.",
    )

    # ---- Factor mining commands (Stage 3) ---------------------------

    mine_factors_cmd = subparsers.add_parser(
        "mine-factors",
        help="Run a factor mining session and produce a candidate run directory.",
    )
    mine_factors_cmd.add_argument("--research-db", required=True, help="Path to research_source.duckdb.")
    mine_factors_cmd.add_argument("--start", required=True, help="Start date YYYYMMDD.")
    mine_factors_cmd.add_argument("--end", required=True, help="End date YYYYMMDD.")
    mine_factors_cmd.add_argument("--config", required=True, help="Path to mining config TOML.")

    list_candidates_cmd = subparsers.add_parser(
        "list-factor-candidates",
        help="List past factor mining runs from the registry.",
    )
    list_candidates_cmd.add_argument("--family", default="", help="Filter by family (case-sensitive).")
    list_candidates_cmd.add_argument("--min-ic-ir", type=float, default=None, help="Filter by minimum mean OOS IC_IR.")

    inspect_cmd = subparsers.add_parser(
        "inspect-candidate",
        help="Run a full Stage 2 evaluation on a sandbox candidate.",
    )
    inspect_cmd.add_argument("run_id", help="Run id (directory name under output/factor_lab/runs/).")
    inspect_cmd.add_argument("expr_id", help="Candidate expr_id from candidates.jsonl.")
    inspect_cmd.add_argument("--research-db", default="output/research_source.duckdb", help="Path to research_source.duckdb.")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.command == "list-descriptors":
        descriptors = [path.stem for path in list_configs("descriptors") if path.stem != "template"]
        _dump_json(descriptors)
        return

    if args.command == "list-descriptor-sets":
        descriptor_sets = [
            path.stem for path in list_configs("descriptor_sets") if path.stem != "template"
        ]
        _dump_json(descriptor_sets)
        return

    if args.command == "list-cost-models":
        cost_models = [path.stem for path in list_configs("cost_models") if path.stem != "template"]
        _dump_json(cost_models)
        return

    if args.command == "list-execution-policies":
        execution_policies = [
            path.stem for path in list_configs("execution_policies") if path.stem != "template"
        ]
        _dump_json(execution_policies)
        return

    if args.command == "list-decay-monitors":
        decay_monitors = [
            path.stem for path in list_configs("decay_monitors") if path.stem != "template"
        ]
        _dump_json(decay_monitors)
        return

    if args.command == "list-portfolio-construction-models":
        construction_models = [
            path.stem
            for path in list_configs("portfolio_construction")
            if path.stem != "template"
        ]
        _dump_json(construction_models)
        return

    if args.command == "list-risk-models":
        risk_models = [path.stem for path in list_configs("risk_models") if path.stem != "template"]
        _dump_json(risk_models)
        return

    if args.command == "list-theses":
        theses = [path.stem for path in list_configs("theses") if path.stem != "template"]
        _dump_json(theses)
        return

    if args.command == "show-descriptor":
        descriptor = load_descriptor(Path(args.path))
        _dump_json(asdict(descriptor))
        return

    if args.command == "show-descriptor-set":
        descriptor_set = load_descriptor_set(Path(args.path))
        _dump_json(asdict(descriptor_set))
        return

    if args.command == "show-mandate":
        mandate = load_mandate(Path(args.path))
        _dump_json(asdict(mandate))
        return

    if args.command == "show-thesis":
        thesis = load_thesis(Path(args.path))
        _dump_json(asdict(thesis))
        return

    if args.command == "show-target":
        target = load_target(Path(args.path))
        _dump_json(asdict(target))
        return

    if args.command == "show-risk-model":
        risk_model = load_risk_model(Path(args.path))
        _dump_json(asdict(risk_model))
        return

    if args.command == "show-cost-model":
        cost_model = load_cost_model(Path(args.path))
        _dump_json(asdict(cost_model))
        return

    if args.command == "show-execution-policy":
        execution_policy = load_execution_policy(Path(args.path))
        _dump_json(asdict(execution_policy))
        return

    if args.command == "show-benchmark-state":
        benchmark_state = load_benchmark_state_artifact(Path(args.path))
        _dump_json(asdict(benchmark_state))
        return

    if args.command == "show-account-state":
        account_state = load_account_state_snapshot(Path(args.path))
        portfolio_state = account_state_to_portfolio_state(
            portfolio_id=args.portfolio_id,
            account_state=account_state,
        )
        _dump_json(
            {
                "account_state": asdict(account_state),
                "derived_portfolio_state": asdict(portfolio_state),
            }
        )
        return

    if args.command == "show-portfolio-state":
        portfolio_state = load_portfolio_state_snapshot(Path(args.path))
        _dump_json(asdict(portfolio_state))
        return

    if args.command == "show-decay-monitor":
        decay_monitor = load_decay_monitor(Path(args.path))
        _dump_json(asdict(decay_monitor))
        return

    if args.command == "show-sleeve":
        sleeve = load_sleeve(Path(args.path))
        _dump_json(asdict(sleeve))
        return

    if args.command == "show-portfolio":
        portfolio = load_portfolio(Path(args.path))
        _dump_json(asdict(portfolio))
        return

    if args.command == "show-portfolio-construction-model":
        construction_model = load_portfolio_construction_model(Path(args.path))
        _dump_json(asdict(construction_model))
        return

    if args.command == "show-promotion-gate":
        promotion_gate = load_promotion_gate(Path(args.path))
        _dump_json(asdict(promotion_gate))
        return

    if args.command == "show-sleeve-artifact":
        artifact = load_sleeve_artifact(Path(args.path))
        _dump_json(asdict(artifact))
        return

    if args.command == "build-sleeve-artifact":
        loaded_case = load_sleeve_artifact_build_case(Path(args.case))
        artifact = build_sleeve_artifact(loaded_case)
        output_path = write_sleeve_artifact(artifact, loaded_case.definition.output_path)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "output_path": str(output_path),
                "artifact": asdict(artifact),
            }
        )
        return

    if args.command == "build-research-source-db":
        result = build_research_source_db(
            source_db=Path(args.source_db),
            target_db=Path(args.target_db),
            supplemental_db=Path(args.supplemental_db) if args.supplemental_db else None,
        )
        _dump_json(result)
        return

    if args.command == "build-reference-staging-db":
        benchmark_values = args.benchmark or ["CSI 800=000906.SH"]
        benchmarks = [_parse_benchmark_reference(value) for value in benchmark_values]
        industry_levels = tuple(args.industry_level or ["L1", "L2", "L3"])
        end_date = args.end_date or date.today().strftime("%Y%m%d")
        result = build_tushare_reference_db(
            target_db=Path(args.target_db),
            benchmarks=benchmarks,
            start_date=args.start_date,
            end_date=end_date,
            token=args.token or None,
            industry_levels=industry_levels,
            index_weight_window_months=args.index_weight_window_months,
        )
        _dump_json(result)
        return

    if args.command == "build-trend-research-input":
        loaded_case = load_trend_research_input_build_case(Path(args.case))
        result = build_trend_research_observation_input(loaded_case)
        output_path = write_trend_research_observation_input(result, loaded_case.output_path)
        _dump_json(
            {
                "case_id": result.case_id,
                "description": result.description,
                "sleeve_id": result.sleeve_id,
                "descriptor_set_id": result.descriptor_set_id,
                "source_db_path": result.source_db_path,
                "warnings": result.warnings,
                "trade_dates": [step.trade_date for step in result.observation_input.steps],
                "step_count": len(result.observation_input.steps),
                "record_count": sum(
                    len(step.records) for step in result.observation_input.steps
                ),
                "output_path": str(output_path),
            }
        )
        return

    if args.command == "build-fundamental-research-input":
        loaded_case = load_fundamental_research_input_build_case(Path(args.case))
        result = build_fundamental_research_observation_input(loaded_case)
        output_path = write_fundamental_research_observation_input(
            result,
            loaded_case.output_path,
        )
        _dump_json(
            {
                "case_id": result.case_id,
                "description": result.description,
                "sleeve_id": result.sleeve_id,
                "descriptor_set_id": result.descriptor_set_id,
                "source_db_path": result.source_db_path,
                "warnings": result.warnings,
                "trade_dates": [step.trade_date for step in result.observation_input.steps],
                "step_count": len(result.observation_input.steps),
                "record_count": sum(
                    len(step.records) for step in result.observation_input.steps
                ),
                "output_path": str(output_path),
            }
        )
        return

    if args.command == "build-benchmark-state":
        loaded_case = load_benchmark_state_build_case(Path(args.case))
        artifact = build_benchmark_state_artifact(loaded_case)
        output_path = write_benchmark_state_artifact(artifact, loaded_case.definition.output_path)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "benchmark_id": artifact.benchmark_id,
                "classification": artifact.classification,
                "weighting_method": artifact.weighting_method,
                "trade_dates": [step.trade_date for step in artifact.steps],
                "step_count": len(artifact.steps),
                "output_path": str(output_path),
            }
        )
        return

    if args.command == "run-promotion-replay":
        loaded_case = load_portfolio_promotion_replay_case(Path(args.case))
        result = PortfolioPromotionReplay(
            mandate=loaded_case.mandate,
            construction_model=loaded_case.construction_model,
            default_cost_model=loaded_case.default_cost_model,
            gate=loaded_case.gate,
            cost_models=loaded_case.cost_models,
        ).replay(loaded_case.replay_input)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "decision": asdict(result.decision) if result.decision is not None else None,
                "snapshot": asdict(result.snapshot),
                "research_evidence": _promotion_replay_research_evidence_payload(result),
            }
        )
        return

    if args.command == "validate-live-candidate-bundle":
        from .live_readiness import load_live_candidate_bundle
        loaded_bundle = load_live_candidate_bundle(Path(args.path))
        _dump_json(
            {
                "candidate_id": loaded_bundle.definition.candidate_id,
                "version": loaded_bundle.definition.version,
                "status": loaded_bundle.definition.status,
                "thesis_id": loaded_bundle.thesis.id,
                "descriptor_set_id": loaded_bundle.descriptor_set.id,
                "sleeve_id": loaded_bundle.sleeve.id,
                "target_id": loaded_bundle.target.id,
                "portfolio_id": loaded_bundle.portfolio.id,
                "regime_overlay_id": loaded_bundle.definition.regime_overlay_id,
            }
        )
        return

    if args.command == "evaluate-shadow-live-journal":
        from .live_readiness import evaluate_shadow_live_journal
        evaluation = evaluate_shadow_live_journal(Path(args.path))
        _dump_json(
            {
                "candidate_id": evaluation.bundle.definition.candidate_id,
                "status": evaluation.bundle.definition.status,
                "summary": asdict(evaluation.summary),
            }
        )
        return

    if args.command == "evaluate-multi-year-validation-audit":
        from .live_readiness import evaluate_multi_year_validation_audit
        evaluation = evaluate_multi_year_validation_audit(Path(args.path))
        _dump_json(
            {
                "candidate_id": evaluation.definition.candidate_id,
                "as_of_date": evaluation.definition.as_of_date,
                "validation_window_start": evaluation.definition.validation_window_start,
                "validation_window_end": evaluation.definition.validation_window_end,
                "summary": asdict(evaluation.summary),
                "notes": list(evaluation.definition.notes),
            }
        )
        return

    if args.command == "build-multi-year-validation-audit":
        from .live_readiness import (
            build_multi_year_validation_audit,
            evaluate_multi_year_validation_audit,
            load_multi_year_validation_audit_build_case,
            write_multi_year_validation_audit,
        )
        loaded_case = load_multi_year_validation_audit_build_case(Path(args.case))
        definition = build_multi_year_validation_audit(loaded_case)
        output_path = write_multi_year_validation_audit(
            definition,
            loaded_case.definition.output_path,
        )
        evaluation = evaluate_multi_year_validation_audit(output_path)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "candidate_id": definition.candidate_id,
                "portfolio_id": loaded_case.portfolio.id,
                "benchmark_id": loaded_case.benchmark_state_artifact.benchmark_id,
                "output_path": str(output_path),
                "summary": asdict(evaluation.summary),
            }
        )
        return

    if args.command == "run-portfolio-backtest":
        from .portfolio_backtester import (
            load_portfolio_backtest_case,
            run_loaded_portfolio_backtest,
            write_portfolio_backtest_artifact,
        )
        loaded_case = load_portfolio_backtest_case(Path(args.case))
        result = run_loaded_portfolio_backtest(loaded_case)
        output_path = write_portfolio_backtest_artifact(
            case_id=loaded_case.definition.case_id,
            description=loaded_case.definition.description,
            result=result,
            path=loaded_case.definition.output_path,
        )
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "output_path": str(output_path),
                "summary": asdict(result.summary) if result.summary is not None else None,
            }
        )
        return

    if args.command == "build-run-manifest":
        loaded_case = load_run_manifest_case(Path(args.case))
        if loaded_case.executable_signal_case.definition.live_candidate_bundle_path:
            from .live_readiness import validate_live_candidate_signal_release
            validate_live_candidate_signal_release(
                bundle_path=loaded_case.executable_signal_case.definition.live_candidate_bundle_path,
                portfolio_path=loaded_case.executable_signal_case.definition.portfolio_path,
            )
        construction_step, package = _build_executable_signal_package(
            loaded_case.executable_signal_case
        )
        manifest = build_run_manifest(
            run_id=loaded_case.executable_signal_case.definition.case_id,
            package=package,
            benchmark_state_path=(
                loaded_case.executable_signal_case.definition.benchmark_state_path
                or loaded_case.executable_signal_case.definition.benchmark_industry_weights_path
            ),
            sleeve_artifact_paths=loaded_case.executable_signal_case.definition.artifact_paths,
            portfolio_path=loaded_case.executable_signal_case.definition.portfolio_path,
            account_state_path=loaded_case.executable_signal_case.definition.account_state_path,
            portfolio_state_path=loaded_case.executable_signal_case.definition.portfolio_state_path,
            data_version=loaded_case.definition.data_version,
            data_build_date=loaded_case.definition.data_build_date,
            operator_id=loaded_case.definition.operator_id,
            operator_timestamp=loaded_case.definition.operator_timestamp,
        )
        output_path = write_run_manifest(manifest, loaded_case.definition.output_path)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "output_path": str(output_path),
                "construction_step": asdict(construction_step),
                "package": asdict(package),
                "manifest": {
                    "schema_version": 1,
                    "artifact_type": "run_manifest",
                    **asdict(manifest),
                },
            }
        )
        return

    if args.command == "build-executable-signal":
        loaded_case = load_executable_signal_case(Path(args.case))
        if loaded_case.definition.live_candidate_bundle_path:
            from .live_readiness import validate_live_candidate_signal_release
            validate_live_candidate_signal_release(
                bundle_path=loaded_case.definition.live_candidate_bundle_path,
                portfolio_path=loaded_case.definition.portfolio_path,
            )
        construction_step, package = _build_executable_signal_package(loaded_case)
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "construction_step": asdict(construction_step),
                "package": asdict(package),
            }
        )
        return

    if args.command == "evaluate-decay-watch":
        loaded_case = load_decay_watch_case(Path(args.case))
        record = DecayMonitorEvaluator(loaded_case.decay_monitor).evaluate(
            portfolio=loaded_case.portfolio,
            evaluation_date=loaded_case.definition.evaluation_date,
            window_label=loaded_case.definition.window_label,
            promotion_snapshot=loaded_case.promotion_snapshot,
            realized_summary=loaded_case.realized_summary,
            run_manifest=loaded_case.run_manifest,
            manual_execution_outcome=loaded_case.manual_execution_outcome,
            realized_trading_window=loaded_case.realized_trading_window,
        )
        _dump_json(
            {
                "case_id": loaded_case.definition.case_id,
                "description": loaded_case.definition.description,
                "record": asdict(record),
            }
        )
        return

    if args.command == "audit-market-data-quality":
        from .market_data_quality import write_market_data_quality_audit
        result = write_market_data_quality_audit(
            source_db=Path(args.source_db),
            output_path=Path(args.output),
        )
        _dump_json(result)
        return

    if args.command == "build-regime-overlay-observations":
        from .regime_overlay_observation_builder import (
            build_regime_overlay_observation_history,
            load_regime_overlay_observation_build_case,
            write_regime_overlay_observation_history,
        )
        loaded_case = load_regime_overlay_observation_build_case(Path(args.case))
        result = build_regime_overlay_observation_history(loaded_case)
        output_path = write_regime_overlay_observation_history(
            result,
            loaded_case.definition.output_path,
        )
        _dump_json(
            {
                "case_id": result.case_id,
                "description": result.description,
                "overlay_id": result.overlay_id,
                "benchmark_id": result.benchmark_id,
                "trade_date_count": result.trade_date_count,
                "output_path": str(output_path),
                "state_counts": result.state_counts,
            }
        )
        return

    if args.command == "init":
        from .data_ingest.init_workspace import init_workspace as _init_workspace
        report = _init_workspace(Path(args.workspace))
        _dump_json(
            {
                "workspace": str(report.workspace),
                "actions": [
                    {"path": str(a.path), "action": a.action}
                    for a in report.actions
                ],
            }
        )
        return

    if args.command == "sync":
        import sys as _sys
        from .data_ingest.config_models import load_data_sources_config
        from .data_ingest.orchestrator import sync as _sync
        from .data_ingest.adapters.tushare_adapter import TushareAdapter
        from .data_ingest.adapters.akshare_adapter import AKShareAdapter
        from .data_ingest.adapters.baostock_adapter import BaostockAdapter

        config_path = Path(args.config)
        if not config_path.exists():
            _dump_json({"error": f"Config not found: {config_path}. Run 'alpha-find-v2 init' first."})
            _sys.exit(2)
            return

        config = load_data_sources_config(config_path)

        # Build adapter map from enabled adapters in config
        adapter_map = {}
        if config.adapters.get("tushare", None) and config.adapters["tushare"].enabled:
            try:
                from .data_ingest.adapters.tushare_adapter import TushareAdapter
                adapter_map["tushare"] = TushareAdapter()
            except (ImportError, Exception) as exc:
                print(f"[WARN] Could not initialise TushareAdapter: {exc}", file=_sys.stderr)
        if config.adapters.get("akshare", None) and config.adapters["akshare"].enabled:
            try:
                from .data_ingest.adapters.akshare_adapter import AKShareAdapter
                adapter_map["akshare"] = AKShareAdapter()
            except (ImportError, Exception) as exc:
                print(f"[WARN] Could not initialise AKShareAdapter: {exc}", file=_sys.stderr)
        if config.adapters.get("baostock", None) and config.adapters["baostock"].enabled:
            try:
                from .data_ingest.adapters.baostock_adapter import BaostockAdapter
                adapter_map["baostock"] = BaostockAdapter()
            except (ImportError, Exception) as exc:
                print(f"[WARN] Could not initialise BaostockAdapter: {exc}", file=_sys.stderr)

        report = _sync(
            raw_db_path=Path(args.raw_db),
            config=config,
            adapter_map=adapter_map,
            only=set(args.only.split(",")) if args.only else None,
            reset=set(args.reset.split(",")) if args.reset else None,
            since=args.since or None,
            until=args.until or None,
            dry_run=args.dry_run,
        )

        failed = report.failed_count()
        if failed:
            print(
                f"[WARN] {failed} dataset(s) failed to sync. See results for details.",
                file=_sys.stderr,
            )

        _dump_json(
            {
                "raw_db_path": report.raw_db_path,
                "started_at": report.started_at,
                "finished_at": report.finished_at,
                "success_count": report.success_count(),
                "failed_count": report.failed_count(),
                "results": [
                    {
                        "dataset_id": r.dataset_id,
                        "adapter": r.adapter,
                        "rows_added": r.rows_added,
                        "duration_seconds": round(r.duration_seconds, 2),
                        "status": r.status,
                        "error_message": r.error_message,
                    }
                    for r in report.results
                ],
            }
        )
        return

    if args.command == "audit-data":
        import sys as _sys
        from .data_ingest.audit import run_audit as _run_audit
        report = _run_audit(
            raw_db_path=Path(args.raw_db),
            out_dir=Path(args.out_dir),
        )
        _dump_json(
            {
                "raw_db_path": report.raw_db_path,
                "run_at": report.run_at,
                "overall_status": report.overall_status,
                "outcomes": [
                    {
                        "check_id": o.check_id,
                        "severity": o.severity,
                        "result": o.result,
                        "details": o.details,
                    }
                    for o in report.outcomes
                ],
            }
        )
        if report.overall_status == "blocking_failure":
            _sys.exit(1)
        return

    if args.command == "compute-descriptor":
        from .factor_evaluation import descriptor_stubs  # noqa: F401 — register stubs
        from .factor_evaluation.cli_handlers import handle_compute_descriptor
        sys.exit(handle_compute_descriptor(args))
        return

    if args.command == "evaluate-descriptor":
        from .factor_evaluation import descriptor_stubs  # noqa: F401
        from .factor_evaluation.cli_handlers import handle_evaluate_descriptor
        sys.exit(handle_evaluate_descriptor(args))
        return

    if args.command == "list-evaluation-reports":
        from .factor_evaluation.cli_handlers import handle_list_evaluation_reports
        sys.exit(handle_list_evaluation_reports(args))
        return

    if args.command == "mine-factors":
        from pathlib import Path as _Path
        from alpha_find_v2.factor_lab.run import execute_mining_run

        start = args.start
        end = args.end
        if len(start) != 8 or not start.isdigit():
            print(f"Error: --start must be YYYYMMDD format, got {start!r}", file=sys.stderr)
            sys.exit(2)
        if len(end) != 8 or not end.isdigit():
            print(f"Error: --end must be YYYYMMDD format, got {end!r}", file=sys.stderr)
            sys.exit(2)
        if start > end:
            print(f"Error: --start {start} must be <= --end {end}", file=sys.stderr)
            sys.exit(2)

        result = execute_mining_run(
            research_db=_Path(args.research_db),
            start=start,
            end=end,
            config_path=_Path(args.config),
        )
        print(json.dumps(result))
        return

    if args.command == "list-factor-candidates":
        from pathlib import Path as _Path
        from alpha_find_v2.factor_lab.registry import list_runs

        runs = list_runs(
            family=args.family if args.family else None,
            min_ic_ir=args.min_ic_ir,
            shortlist_dir_base=_Path("output/factor_lab"),
        )
        print(json.dumps(runs))
        return

    if args.command == "inspect-candidate":
        from pathlib import Path as _Path
        from alpha_find_v2.factor_lab.inspect import run_inspection

        run_inspection(
            run_id=args.run_id,
            expr_id=args.expr_id,
            output_root=_Path("output/factor_lab"),
            research_db=_Path(args.research_db),
        )
        return

    raise ValueError(f"Unsupported command: {args.command}")
