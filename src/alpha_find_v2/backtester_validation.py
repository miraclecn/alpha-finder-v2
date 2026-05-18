from __future__ import annotations

from dataclasses import dataclass, field
import os
import re
import subprocess
import sys

from .config_loader import PROJECT_ROOT


@dataclass(slots=True)
class BacktesterValidationResult:
    success: bool
    tests_run: int
    failures: int
    errors: int
    skipped: int
    covered_capabilities: tuple[str, ...] = field(default_factory=tuple)
    suite_pattern: str = "test_portfolio_backtester.py"
    output: str = ""


def run_backtester_validation_suite() -> BacktesterValidationResult:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    src_path = str(PROJECT_ROOT / "src")
    env["PYTHONPATH"] = (
        src_path
        if not existing_pythonpath
        else f"{src_path}:{existing_pythonpath}"
    )
    completed = subprocess.run(
        [
            sys.executable or "python3",
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_portfolio_backtester.py",
            "-v",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = completed.stdout if completed.stdout else completed.stderr
    tests_run = _parse_tests_run(output)
    return BacktesterValidationResult(
        success=completed.returncode == 0,
        tests_run=tests_run,
        failures=_count_result_lines(output, "FAIL:"),
        errors=_count_result_lines(output, "ERROR:"),
        skipped=_count_result_lines(output, "skipped "),
        covered_capabilities=(
            "t_plus_one",
            "lot_size_and_cash_ledger",
            "limit_and_suspension_handling",
            "trade_state_entry_exit_blocks",
            "participation_cap_partial_fills",
            "corporate_actions",
            "market_data_fallback_diagnostics",
            "benchmark_active_metrics",
            "staggered_rebalance_behavior",
        ),
        output=output,
    )


def _parse_tests_run(output: str) -> int:
    match = re.search(r"Ran\s+(\d+)\s+tests?", output)
    if match is None:
        return 0
    return int(match.group(1))


def _count_result_lines(output: str, token: str) -> int:
    return sum(1 for line in output.splitlines() if token in line)
