import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from alpha_find_v2.live_readiness import (
    build_multi_year_validation_audit,
    evaluate_shadow_live_journal,
    evaluate_multi_year_validation_audit,
    load_multi_year_validation_audit_build_case,
    load_live_candidate_bundle,
    write_multi_year_validation_audit,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "deployment_minimal"


class LiveReadinessTest(unittest.TestCase):
    def test_live_candidate_bundle_freezes_trend_only_shadow_live_candidate(self) -> None:
        loaded = load_live_candidate_bundle(
            EXAMPLE_ROOT / "trend_leadership_live_candidate_v1.toml"
        )

        self.assertEqual(loaded.definition.candidate_id, "trend_leadership_shadow_live_v1")
        self.assertEqual(loaded.definition.status, "shadow_live_eligible")
        self.assertEqual(loaded.portfolio.sleeves, ["trend_leadership_core"])
        self.assertEqual(loaded.portfolio.regime_overlay_id, "a_share_risk_overlay")
        self.assertAlmostEqual(
            loaded.definition.expected_turnover_budget,
            loaded.sleeve.turnover_budget,
        )

    def test_live_candidate_bundle_rejects_multi_sleeve_portfolio(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / "bundle.toml"
            text = (
                (EXAMPLE_ROOT / "trend_leadership_live_candidate_v1.toml")
                .read_text(encoding="utf-8")
                .replace(
                    "research/examples/deployment_minimal/trend_live_candidate_portfolio_with_overlay.toml",
                    "research/examples/deployment_minimal/candidate_portfolio_with_overlay.toml",
                )
            )
            temp_path.write_text(text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactly one sleeve"):
                load_live_candidate_bundle(temp_path)

    def test_shadow_live_journal_reports_current_gate_as_incomplete(self) -> None:
        evaluation = evaluate_shadow_live_journal(
            EXAMPLE_ROOT / "shadow_live_journal_trend_leadership_v1.json"
        )

        self.assertEqual(evaluation.bundle.definition.candidate_id, "trend_leadership_shadow_live_v1")
        self.assertEqual(evaluation.summary.cycle_count, 1)
        self.assertEqual(evaluation.summary.calendar_month_count, 1)
        self.assertEqual(evaluation.summary.consecutive_weekly_cycles, 1)
        self.assertFalse(evaluation.summary.shadow_live_gate_met)
        self.assertFalse(evaluation.summary.probation_preferred_gate_met)
        self.assertEqual(evaluation.summary.overlay_state_counts["cash_heavier"], 1)
        self.assertAlmostEqual(evaluation.summary.average_realized_turnover, 0.64)

    def test_shadow_live_journal_marks_gate_complete_after_12_weekly_cycles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            bundle_path = EXAMPLE_ROOT / "trend_leadership_live_candidate_v1.toml"
            journal_path = temp_root / "shadow_live_journal.json"

            cycle_entries = []
            for index in range(12):
                trade_day = 20 + (7 * index)
                month = 4 + ((trade_day - 1) // 28)
                day = ((trade_day - 1) % 28) + 1
                trade_date = f"2026-{month:02d}-{day:02d}"
                execution_day = day + 1 if day < 28 else 28
                execution_date = f"2026-{month:02d}-{execution_day:02d}"
                run_id = f"trend_live_candidate_next_open_{trade_date}"
                manifest_path = temp_root / f"run_manifest_{index}.json"
                outcome_path = temp_root / f"manual_execution_outcome_{index}.json"
                realized_path = temp_root / f"realized_trading_window_{index}.json"
                self._write_manifest_copy(
                    source=EXAMPLE_ROOT / "trend_live_candidate_run_manifest_2026_04_20.json",
                    target=manifest_path,
                    run_id=run_id,
                    trade_date=trade_date,
                    execution_date=execution_date,
                )
                self._write_outcome_copy(
                    source=EXAMPLE_ROOT / "trend_live_candidate_manual_execution_outcome_2026_04_21.json",
                    target=outcome_path,
                    run_id=run_id,
                    trade_date=trade_date,
                    execution_date=execution_date,
                )
                self._write_realized_copy(
                    source=EXAMPLE_ROOT / "trend_live_candidate_realized_trading_window_2026_05_18.json",
                    target=realized_path,
                    run_id=run_id,
                    evaluation_date=f"2026-{month:02d}-28",
                )
                cycle_entries.append(
                    {
                        "run_manifest_path": str(manifest_path),
                        "manual_execution_outcome_path": str(outcome_path),
                        "realized_trading_window_path": str(realized_path),
                    }
                )

            journal_payload = {
                "schema_version": 1,
                "artifact_type": "shadow_live_journal",
                "candidate_bundle_path": str(bundle_path),
                "started_at": "2026-04-21",
                "cycles": cycle_entries,
            }
            journal_path.write_text(
                json.dumps(journal_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

            evaluation = evaluate_shadow_live_journal(journal_path)
            self.assertEqual(evaluation.summary.cycle_count, 12)
            self.assertEqual(evaluation.summary.consecutive_weekly_cycles, 12)
            self.assertGreaterEqual(evaluation.summary.calendar_month_count, 3)
            self.assertTrue(evaluation.summary.shadow_live_gate_met)
            self.assertFalse(evaluation.summary.probation_preferred_gate_met)

    def test_cli_validate_live_candidate_bundle_reports_summary(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "validate-live-candidate-bundle",
                "--path",
                str(EXAMPLE_ROOT / "trend_leadership_live_candidate_v1.toml"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["candidate_id"], "trend_leadership_shadow_live_v1")
        self.assertEqual(payload["portfolio_id"], "trend_live_candidate_portfolio_with_overlay")
        self.assertEqual(payload["status"], "shadow_live_eligible")

    def test_cli_evaluate_shadow_live_journal_reports_gate_status(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "evaluate-shadow-live-journal",
                "--path",
                str(EXAMPLE_ROOT / "shadow_live_journal_trend_leadership_v1.json"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["candidate_id"], "trend_leadership_shadow_live_v1")
        self.assertEqual(payload["summary"]["cycle_count"], 1)
        self.assertFalse(payload["summary"]["shadow_live_gate_met"])

    def test_live_candidate_bundle_allows_blank_account_export_contract_for_paper_trade(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / "bundle.toml"
            text = (
                (EXAMPLE_ROOT / "trend_leadership_live_candidate_v1.toml")
                .read_text(encoding="utf-8")
                .replace(
                    'account_export_contract_path = "docs/operations/account-state-export-contract.md"',
                    'paper_trade_policy_path = "docs/operations/trend-leadership-live-candidate-v1.md"\naccount_export_contract_path = ""',
                )
            )
            temp_path.write_text(text, encoding="utf-8")

            loaded = load_live_candidate_bundle(temp_path)
            self.assertEqual(loaded.definition.account_export_contract_path, "")

    def test_multi_year_validation_audit_reports_current_example_as_release_ready(self) -> None:
        evaluation = evaluate_multi_year_validation_audit(
            EXAMPLE_ROOT / "trend_leadership_multi_year_validation_audit_v1.json"
        )

        self.assertEqual(evaluation.definition.candidate_id, "trend_leadership_shadow_live_v1")
        self.assertTrue(evaluation.summary.signal_release_gate_met)
        self.assertEqual(evaluation.summary.blockers, [])
        self.assertEqual(evaluation.definition.validation_window_start, "2021-03-05")
        self.assertEqual(evaluation.definition.validation_window_end, "2026-03-19")
        self.assertGreaterEqual(evaluation.summary.calendar_years_covered, 5.0)
        self.assertNotIn("anchored_walk_forward", evaluation.summary.blockers)
        self.assertNotIn("regime_split", evaluation.summary.blockers)
        self.assertNotIn("portfolio_evidence", evaluation.summary.blockers)

    def test_cli_evaluate_multi_year_validation_audit_reports_release_gate(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "evaluate-multi-year-validation-audit",
                "--path",
                str(EXAMPLE_ROOT / "trend_leadership_multi_year_validation_audit_v1.json"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["candidate_id"], "trend_leadership_shadow_live_v1")
        self.assertTrue(payload["summary"]["signal_release_gate_met"])
        self.assertEqual(payload["summary"]["blockers"], [])
        self.assertEqual(payload["validation_window_start"], "2021-03-05")
        self.assertEqual(payload["validation_window_end"], "2026-03-19")
        self.assertGreaterEqual(payload["summary"]["calendar_years_covered"], 5.0)
        joined_notes = "\n".join(payload["notes"])
        self.assertIn("security_code_alias_backfill", joined_notes)

    def test_build_multi_year_validation_audit_without_replay_keeps_gate_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            case_path = self._write_multi_year_audit_case(
                temp_root=temp_root,
                candidate_id="trend_shadow_candidate_test",
                portfolio_path=(
                    "research/examples/deployment_minimal/"
                    "trend_live_candidate_portfolio_with_overlay.toml"
                ),
            )

            loaded_case = load_multi_year_validation_audit_build_case(case_path)
            definition = build_multi_year_validation_audit(loaded_case)
            output_path = write_multi_year_validation_audit(
                definition,
                loaded_case.definition.output_path,
            )
            evaluation = evaluate_multi_year_validation_audit(output_path)

            self.assertTrue(definition.benchmark_membership_pit)
            self.assertTrue(definition.industry_classification_pit)
            self.assertTrue(definition.tradeability.t_plus_one)
            self.assertTrue(definition.tradeability.suspension)
            self.assertTrue(definition.tradeability.price_limits)
            self.assertTrue(definition.tradeability.lot_size)
            self.assertTrue(definition.tradeability.liquidity)
            self.assertTrue(definition.tradeability.slippage)
            self.assertFalse(definition.anchored_walk_forward_complete)
            self.assertFalse(definition.regime_split_complete)
            self.assertFalse(definition.portfolio_evidence_complete)
            self.assertFalse(evaluation.summary.signal_release_gate_met)
            self.assertIn("minimum_history_years", evaluation.summary.blockers)
            self.assertIn("anchored_walk_forward", evaluation.summary.blockers)
            self.assertIn("regime_split", evaluation.summary.blockers)
            self.assertIn("portfolio_evidence", evaluation.summary.blockers)
            joined_notes = "\n".join(definition.notes)
            self.assertIn("pipeline smoke check only", joined_notes)
            self.assertIn("No replay case is attached", joined_notes)

    def test_build_multi_year_validation_audit_with_overlay_replay_marks_portfolio_evidence_complete(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            replay_case_path, candidate_portfolio_path = self._write_overlay_replay_case(
                temp_root
            )
            case_path = self._write_multi_year_audit_case(
                temp_root=temp_root,
                candidate_id="overlay_candidate_test",
                portfolio_path=str(candidate_portfolio_path),
                replay_case_path=str(replay_case_path),
            )

            loaded_case = load_multi_year_validation_audit_build_case(case_path)
            definition = build_multi_year_validation_audit(loaded_case)
            output_path = write_multi_year_validation_audit(
                definition,
                loaded_case.definition.output_path,
            )
            evaluation = evaluate_multi_year_validation_audit(output_path)

            self.assertTrue(definition.anchored_walk_forward_complete)
            self.assertTrue(definition.regime_split_complete)
            self.assertTrue(definition.portfolio_evidence_complete)
            self.assertNotIn("anchored_walk_forward", evaluation.summary.blockers)
            self.assertNotIn("regime_split", evaluation.summary.blockers)
            self.assertNotIn("portfolio_evidence", evaluation.summary.blockers)
            joined_notes = "\n".join(definition.notes)
            self.assertNotIn("does not yet apply overlay gross-exposure changes", joined_notes)
            self.assertTrue(definition.market_state_coverage.fragile_leadership)
            self.assertNotIn(
                "fragile_leadership",
                evaluation.summary.missing_market_states,
            )

    def test_cli_build_multi_year_validation_audit_reports_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            case_path = self._write_multi_year_audit_case(
                temp_root=temp_root,
                candidate_id="cli_candidate_test",
                portfolio_path=(
                    "research/examples/deployment_minimal/"
                    "trend_live_candidate_portfolio_with_overlay.toml"
                ),
            )

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-multi-year-validation-audit",
                    "--case",
                    str(case_path),
                ],
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["candidate_id"], "cli_candidate_test")
            self.assertEqual(
                payload["portfolio_id"],
                "trend_live_candidate_portfolio_with_overlay",
            )
            self.assertFalse(payload["summary"]["signal_release_gate_met"])
            self.assertIn("minimum_history_years", payload["summary"]["blockers"])

    def _write_manifest_copy(
        self,
        *,
        source: Path,
        target: Path,
        run_id: str,
        trade_date: str,
        execution_date: str,
    ) -> None:
        payload = json.loads(source.read_text(encoding="utf-8"))
        payload["run_id"] = run_id
        payload["trade_date"] = trade_date
        payload["execution_date"] = execution_date
        payload["package_id"] = (
            f"{payload['portfolio_id']}:{trade_date}"
        )
        payload["operator_timestamp"] = f"{execution_date}T09:20:00+08:00"
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _write_outcome_copy(
        self,
        *,
        source: Path,
        target: Path,
        run_id: str,
        trade_date: str,
        execution_date: str,
    ) -> None:
        payload = json.loads(source.read_text(encoding="utf-8"))
        payload["run_id"] = run_id
        payload["execution_date"] = execution_date
        payload["package_id"] = (
            f"{payload['portfolio_id']}:{trade_date}"
        )
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _write_realized_copy(
        self,
        *,
        source: Path,
        target: Path,
        run_id: str,
        evaluation_date: str,
    ) -> None:
        payload = json.loads(source.read_text(encoding="utf-8"))
        payload["run_id"] = run_id
        payload["evaluation_date"] = evaluation_date
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _write_multi_year_audit_case(
        self,
        *,
        temp_root: Path,
        candidate_id: str,
        portfolio_path: str,
        replay_case_path: str = "",
    ) -> Path:
        benchmark_case_path = self._write_benchmark_build_case(temp_root)
        trend_case_path = self._write_trend_build_case(temp_root)
        output_path = temp_root / "multi_year_audit.json"
        case_path = temp_root / "multi_year_audit_case.toml"
        lines = [
            "schema_version = 1",
            'artifact_type = "multi_year_validation_audit_build_case"',
            'case_id = "multi_year_audit_test_case"',
            'description = "Build a test audit from local benchmark and trend artifacts."',
            f'candidate_id = "{candidate_id}"',
            f'portfolio_path = "{portfolio_path}"',
            f'benchmark_state_build_case_path = "{benchmark_case_path}"',
            f'trend_research_input_build_case_path = "{trend_case_path}"',
            f'output_path = "{output_path}"',
            "minimum_calendar_years = 5.0",
            'as_of_date = "2026-04-26"',
        ]
        if replay_case_path:
            lines.append(f'replay_case_path = "{replay_case_path}"')
        lines.extend(
            [
                "notes = [",
                '  "Test audit build case."',
                "]",
                "",
            ]
        )
        case_path.write_text("\n".join(lines), encoding="utf-8")
        return case_path

    def _write_benchmark_build_case(self, temp_root: Path) -> Path:
        benchmark_artifact_path = temp_root / "benchmark_state_history.json"
        benchmark_case_path = temp_root / "benchmark_case.toml"
        source_db_path = temp_root / "source.duckdb"
        benchmark_artifact = {
            "schema_version": 1,
            "artifact_type": "benchmark_state_history",
            "benchmark_id": "CSI 800",
            "classification": "sw2021_l1",
            "weighting_method": "provider_weight",
            "steps": [
                {
                    "trade_date": "2026-04-06",
                    "effective_at": "2026-04-06T15:00:00+08:00",
                    "available_at": "2026-04-06T15:30:00+08:00",
                    "industry_weights": {"tech": 0.6, "bank": 0.4},
                    "constituents": [
                        {"asset_id": "AAA", "weight": 0.6, "industry": "tech"},
                        {"asset_id": "BBB", "weight": 0.4, "industry": "bank"},
                    ],
                },
                {
                    "trade_date": "2026-04-13",
                    "effective_at": "2026-04-13T15:00:00+08:00",
                    "available_at": "2026-04-13T15:30:00+08:00",
                    "industry_weights": {"tech": 0.55, "bank": 0.45},
                    "constituents": [
                        {"asset_id": "AAA", "weight": 0.55, "industry": "tech"},
                        {"asset_id": "CCC", "weight": 0.45, "industry": "bank"},
                    ],
                },
                {
                    "trade_date": "2026-04-20",
                    "effective_at": "2026-04-20T15:00:00+08:00",
                    "available_at": "2026-04-20T15:30:00+08:00",
                    "industry_weights": {"tech": 0.5, "industrial": 0.5},
                    "constituents": [
                        {"asset_id": "DDD", "weight": 0.5, "industry": "tech"},
                        {"asset_id": "EEE", "weight": 0.5, "industry": "industrial"},
                    ],
                },
            ],
        }
        benchmark_artifact_path.write_text(
            json.dumps(benchmark_artifact, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        benchmark_case_path.write_text(
            "\n".join(
                [
                    "schema_version = 1",
                    'artifact_type = "benchmark_state_build_case"',
                    'case_id = "benchmark_case"',
                    'description = "Test benchmark state build case."',
                    f'source_db_path = "{source_db_path}"',
                    f'output_path = "{benchmark_artifact_path}"',
                    'benchmark_id = "CSI 800"',
                    'industry_schema = "sw2021_l1"',
                    'start_date = "2026-04-06"',
                    'end_date = "2026-04-20"',
                    'weighting_method = "provider_weight"',
                    'effective_time = "15:00:00+08:00"',
                    'available_time = "15:30:00+08:00"',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return benchmark_case_path

    def _write_trend_build_case(self, temp_root: Path) -> Path:
        trend_input_path = temp_root / "trend_input.json"
        trend_case_path = temp_root / "trend_case.toml"
        source_db_path = temp_root / "source.duckdb"
        trend_input = {
            "schema_version": 1,
            "artifact_type": "sleeve_research_observation_input",
            "steps": [
                {
                    "trade_date": "2026-04-06",
                    "records": [
                        {
                            "asset_id": "AAA",
                            "rank": 1,
                            "score": 9.1,
                            "target_weight": 0.5,
                            "entry_open": 10.0,
                            "exit_open": 10.4,
                            "industry": "tech",
                            "entry_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                        {
                            "asset_id": "BBB",
                            "rank": 2,
                            "score": 8.7,
                            "target_weight": 0.5,
                            "entry_open": 8.0,
                            "exit_open": 8.1,
                            "industry": "bank",
                            "entry_state": {
                                "suspended": False,
                                "limit_locked": True,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                    ],
                },
                {
                    "trade_date": "2026-04-13",
                    "records": [
                        {
                            "asset_id": "AAA",
                            "rank": 1,
                            "score": 9.0,
                            "target_weight": 0.5,
                            "entry_open": 10.0,
                            "exit_open": 10.1,
                            "industry": "tech",
                            "entry_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                        {
                            "asset_id": "CCC",
                            "rank": 2,
                            "score": 8.5,
                            "target_weight": 0.5,
                            "entry_open": 9.0,
                            "exit_open": 8.8,
                            "industry": "bank",
                            "entry_state": {
                                "suspended": True,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                    ],
                },
                {
                    "trade_date": "2026-04-20",
                    "records": [
                        {
                            "asset_id": "DDD",
                            "rank": 1,
                            "score": 8.9,
                            "target_weight": 0.5,
                            "entry_open": 11.0,
                            "exit_open": 11.5,
                            "industry": "tech",
                            "entry_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                        {
                            "asset_id": "EEE",
                            "rank": 2,
                            "score": 8.1,
                            "target_weight": 0.5,
                            "entry_open": 7.5,
                            "exit_open": 7.8,
                            "industry": "industrial",
                            "entry_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                            "exit_state": {
                                "suspended": False,
                                "limit_locked": False,
                                "liquidity_pass": True,
                            },
                        },
                    ],
                },
            ],
        }
        trend_input_path.write_text(
            json.dumps(trend_input, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        trend_case_path.write_text(
            "\n".join(
                [
                    "schema_version = 1",
                    'artifact_type = "trend_research_input_build_case"',
                    'case_id = "trend_case"',
                    'description = "Test trend build case."',
                    'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                    f'source_db_path = "{source_db_path}"',
                    f'output_path = "{trend_input_path}"',
                    'start_date = "2026-04-06"',
                    'end_date = "2026-04-20"',
                    "min_listing_days = 120",
                    "lookback_days = 60",
                    "short_window_days = 20",
                    "turnover_window_days = 20",
                    "rebalance_stride = 5",
                    'industry_label_source = "industry_classification_pit"',
                    'industry_schema = "sw2021_l1"',
                    'limit_lock_mode = "cn_a_directional_open_lock"',
                    'residualization_mode = "non_residual_target"',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return trend_case_path

    def _write_overlay_replay_case(self, temp_root: Path) -> tuple[Path, Path]:
        baseline_portfolio_path = temp_root / "baseline_portfolio.toml"
        candidate_portfolio_path = temp_root / "candidate_portfolio_with_overlay.toml"
        replay_case_path = temp_root / "overlay_replay_case.toml"
        benchmark_state_path = temp_root / "benchmark_state_history.json"
        baseline_portfolio_path.write_text(
            "\n".join(
                [
                    'id = "overlay_test_baseline"',
                    'name = "Overlay Test Baseline"',
                    'mandate_id = "a_share_long_only_eod"',
                    'benchmark = "CSI 800"',
                    'rebalance_policy = "weekly"',
                    'description = "Baseline portfolio for overlay replay audit testing."',
                    'construction_model_id = "a_share_core_blend"',
                    'promotion_gate_id = "research_example_replay_gate"',
                    'execution_policy_id = "a_share_next_open_v1"',
                    'decay_monitor_id = "a_share_core_watch"',
                    'sleeves = ["trend_leadership_core"]',
                    "",
                    "[allocation]",
                    "trend_leadership_core = 1.0",
                    "",
                    "[constraints]",
                    "max_names = 16",
                    "max_single_name_weight = 0.08",
                    "max_industry_overweight = 0.10",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        candidate_portfolio_path.write_text(
            "\n".join(
                [
                    'id = "overlay_test_candidate"',
                    'name = "Overlay Test Candidate"',
                    'mandate_id = "a_share_long_only_eod"',
                    'benchmark = "CSI 800"',
                    'rebalance_policy = "weekly"',
                    'description = "Overlay candidate portfolio for audit testing."',
                    'construction_model_id = "a_share_core_blend"',
                    'promotion_gate_id = "research_example_replay_gate"',
                    'regime_overlay_id = "a_share_risk_overlay"',
                    'execution_policy_id = "a_share_next_open_v1"',
                    'decay_monitor_id = "a_share_core_watch"',
                    'sleeves = ["trend_leadership_core"]',
                    "",
                    "[allocation]",
                    "trend_leadership_core = 1.0",
                    "",
                    "[constraints]",
                    "max_names = 16",
                    "max_single_name_weight = 0.08",
                    "max_industry_overweight = 0.10",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        replay_case_path.write_text(
            "\n".join(
                [
                    "schema_version = 1",
                    'artifact_type = "portfolio_promotion_replay_case"',
                    'case_id = "overlay_replay_case"',
                    'description = "Replay a multi-sleeve overlay candidate for audit testing."',
                    f'baseline_portfolio_path = "{baseline_portfolio_path}"',
                    f'candidate_portfolio_path = "{candidate_portfolio_path}"',
                    'default_cost_model_path = "config/cost_models/base_a_share_cash.toml"',
                    'additional_cost_model_paths = ["config/cost_models/high_a_share_cash.toml"]',
                    f'benchmark_state_path = "{benchmark_state_path}"',
                    'regime_overlay_observation_path = "research/examples/promotion_replay_minimal/regime_overlay_observations.json"',
                    'artifact_paths = [',
                    '  "research/examples/promotion_replay_minimal/sleeve_artifacts/trend_leadership_core.json",',
                    ']',
                    "periods_per_year = 52",
                    "max_component_correlation = 0.35",
                    "correlation_to_existing_portfolio = 0.25",
                    "turnover_budget = 0.70",
                    "",
                    "[[walk_forward_splits]]",
                    'split_id = "anchor_2026_04_06"',
                    'start_trade_date = "2026-04-06"',
                    "",
                    "[[walk_forward_splits]]",
                    'split_id = "anchor_2026_04_13"',
                    'start_trade_date = "2026-04-13"',
                    "",
                    "[cost_scenario_pass]",
                    "base = true",
                    "",
                    "[regime_pass]",
                    "bull = true",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return replay_case_path, candidate_portfolio_path


if __name__ == "__main__":
    unittest.main()
