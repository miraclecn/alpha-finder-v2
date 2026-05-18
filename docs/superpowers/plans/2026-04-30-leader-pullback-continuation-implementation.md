# Leader Pullback Continuation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a new versioned `leader_pullback_continuation_v1` research object that is executable end-to-end in the V2 trend path, with explicit `industry_relative_strength` support, fail-closed industry dependency checks, and admission-first sleeve constraints.

**Architecture:** Keep the existing V2 object chain unchanged and add one new trend lane on top of it. The implementation is intentionally narrow: register a new thesis / descriptor set / sleeve / example-case chain, then teach `trend_research_input_builder.py` to support `industry_relative_strength`, descriptor-specific normalization, and `single_industry_name_cap` without changing the older frozen trend bundles into hard failures.

**Tech Stack:** Python stdlib, DuckDB-backed trend input builder, TOML configs, existing `alpha_find_v2` CLI, `unittest`, optional `pytest`, no new dependencies.

---

## File Structure

### New config files

- Create: `config/theses/leader_pullback_continuation.toml`
- Create: `config/descriptor_sets/leader_pullback_continuation_v1.toml`
- Create: `config/sleeves/leader_pullback_continuation_v1.toml`

Responsibility:

- thesis defines the new mechanism and validation intent
- descriptor set binds the four approved descriptors and weights
- sleeve encodes `biweekly`, `22` names, lower turnover, liquidity floor, and
  `single_industry_name_cap`

### Builder + tests

- Modify: `src/alpha_find_v2/trend_research_input_builder.py`
- Modify: `tests/test_trend_research_input_builder.py`

Responsibility:

- support `industry_relative_strength` in executable scoring
- fail closed when a descriptor set requires industry labels but the case omits
  them
- preserve existing old-sleeve behavior where industry labels are still optional
- enforce `single_industry_name_cap` during final selection

### Registry / example tests

- Modify: `tests/test_config_loader.py`

Responsibility:

- register the new thesis id in the test expectations
- assert the new descriptor set and sleeve load with the approved weights and
  constraints
- assert the new example chain points to the new sleeve instead of reusing old
  trend configs

### Example chain

- Create: `research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml`
- Create: `research/examples/artifact_build_minimal/leader_pullback_continuation_v1_output.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/baseline_portfolio.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/candidate_portfolio.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/replay_case.toml`
- Create: `research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.toml`

Responsibility:

- provide a complete, versioned example chain for future artifact generation
- keep the old failed trend bundle frozen and separate

## Task 1: Register The New Config Chain

**Files:**

- Create: `config/theses/leader_pullback_continuation.toml`
- Create: `config/descriptor_sets/leader_pullback_continuation_v1.toml`
- Create: `config/sleeves/leader_pullback_continuation_v1.toml`
- Modify: `tests/test_config_loader.py`

- [ ] **Step 1: Write the failing config-loader tests**

```python
def test_core_thesis_registry_contains_current_research_lanes(self) -> None:
    thesis_ids = {
        load_thesis(path).id
        for path in list_configs("theses")
        if path.stem != "template"
    }

    self.assertEqual(
        thesis_ids,
        {
            "crowding_anti_consensus",
            "earnings_underreaction",
            "flow_liquidity_reversal",
            "fundamental_rerating",
            "leader_pullback_continuation",
            "trend_leadership",
        },
    )


def test_leader_pullback_sleeve_binds_descriptor_set_and_constraints(self) -> None:
    sleeve = load_sleeve(CONFIG_ROOT / "sleeves" / "leader_pullback_continuation_v1.toml")
    descriptor_set = load_descriptor_set(
        CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml"
    )

    weights = {
        component.descriptor_id: component.weight
        for component in descriptor_set.components
    }
    self.assertEqual(sleeve.thesis_id, "leader_pullback_continuation")
    self.assertEqual(sleeve.rebalance_frequency, "biweekly")
    self.assertEqual(sleeve.construction["holding_count"], 22)
    self.assertEqual(sleeve.constraints["single_industry_name_cap"], 3)
    self.assertEqual(
        weights,
        {
            "medium_term_relative_strength": 0.35,
            "industry_relative_strength": 0.30,
            "trend_stability": 0.25,
            "turnover_confirmation": 0.10,
        },
    )
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_core_thesis_registry_contains_current_research_lanes tests.test_config_loader.ConfigLoaderTest.test_leader_pullback_sleeve_binds_descriptor_set_and_constraints -v
```

Expected:

- the registry test fails because `leader_pullback_continuation` is missing
- the sleeve-load test fails because the new config files do not exist yet

- [ ] **Step 3: Create the thesis config**

```toml
id = "leader_pullback_continuation"
name = "Leader Pullback Continuation"
family = "medium_alpha"
mechanism = "Stocks with medium-term absolute strength, within-industry leadership, and orderly pullback continuation often keep outperforming over the next multi-week holding window."
why_a_share = "A-share leaders often extend in a second leg when industry sponsorship remains strong and capital rotation keeps rewarding liquid leaders after shallow pullbacks."
expected_sign = "positive"
expected_horizon_days = [10, 30]
required_data = [
  "medium_term_relative_strength",
  "industry_relative_strength",
  "trend_stability",
  "turnover_confirmation",
  "tradeability_state",
]
portfolio_role = "candidate medium-horizon alpha sleeve"

[validation]
primary_target = "20d_executable_net_return_interim"
cost_scenarios = ["base", "high"]
must_pass_regimes = ["bull", "bear", "high_dispersion", "low_dispersion"]
```

- [ ] **Step 4: Create the descriptor set and sleeve configs**

```toml
# config/descriptor_sets/leader_pullback_continuation_v1.toml
id = "leader_pullback_continuation_v1"
name = "Leader Pullback Continuation V1 Descriptor Set"
thesis_id = "leader_pullback_continuation"
target_id = "open_t1_to_open_t20_net_cost"
required_data = [
  "medium_term_relative_strength",
  "industry_relative_strength",
  "trend_stability",
  "turnover_confirmation",
]
selection_logic = "cross_sectional_rank_with_industry_binding"

[[components]]
descriptor_id = "medium_term_relative_strength"
role = "absolute_leadership"
weight = 0.35
transform = "cross_sectional_zscore"

[[components]]
descriptor_id = "industry_relative_strength"
role = "within_industry_leadership"
weight = 0.30
transform = "industry_bucket_zscore"

[[components]]
descriptor_id = "trend_stability"
role = "pullback_quality"
weight = 0.25
transform = "cross_sectional_zscore"

[[components]]
descriptor_id = "turnover_confirmation"
role = "tradeability_confirmation"
weight = 0.10
transform = "cross_sectional_zscore"
```

```toml
# config/sleeves/leader_pullback_continuation_v1.toml
id = "leader_pullback_continuation_v1"
name = "Leader Pullback Continuation V1 Sleeve"
mandate_id = "a_share_long_only_eod"
thesis_id = "leader_pullback_continuation"
descriptor_set_id = "leader_pullback_continuation_v1"
target_id = "open_t1_to_open_t20_net_cost"
universe = "investable_a_share_core"
rebalance_frequency = "biweekly"
target_holding_days = 20
turnover_budget = 0.10
execution_rule = "next_day_open"
neutralization = ["industry"]

[construction]
selection = "rank_then_cap_weight"
holding_count = 22
weight_cap = 0.06

[constraints]
min_median_daily_turnover_cny_mn = 120
exclude_price_limit_lock = true
single_industry_name_cap = 3
```

- [ ] **Step 5: Run the focused tests to verify they pass**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_core_thesis_registry_contains_current_research_lanes tests.test_config_loader.ConfigLoaderTest.test_leader_pullback_sleeve_binds_descriptor_set_and_constraints -v
```

Expected:

- both tests pass

- [ ] **Step 6: Commit the config chain**

```bash
git add config/theses/leader_pullback_continuation.toml config/descriptor_sets/leader_pullback_continuation_v1.toml config/sleeves/leader_pullback_continuation_v1.toml tests/test_config_loader.py
git commit -m "Register the leader pullback continuation config chain"
```

## Task 2: Teach The Trend Builder To Execute `industry_relative_strength`

**Files:**

- Modify: `src/alpha_find_v2/trend_research_input_builder.py`
- Modify: `tests/test_trend_research_input_builder.py`

- [ ] **Step 1: Add failing tests for industry-required execution and descriptor-specific scoring**

```python
def test_builder_requires_industry_labels_for_leader_pullback_descriptor_set(self) -> None:
    from alpha_find_v2.trend_research_input_builder import load_trend_research_input_build_case

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        source_db = temp_root / "research_source.duckdb"
        _create_research_source_db(source_db, _trading_days(date(2024, 1, 2), 95))
        case_path = temp_root / "build_case.toml"
        case_path.write_text(
            "\n".join(
                [
                    'schema_version = 1',
                    'artifact_type = "trend_research_input_build_case"',
                    'case_id = "leader_pullback_missing_industry_case"',
                    'description = "Fail closed when industry binding is required."',
                    'sleeve_path = "config/sleeves/leader_pullback_continuation_v1.toml"',
                    f'source_db_path = "{source_db}"',
                    f'output_path = "{temp_root / "trend_input.json"}"',
                    'start_date = "20240326"',
                    'end_date = "20240408"',
                    'lookback_days = 60',
                    'short_window_days = 20',
                    'turnover_window_days = 20',
                    'rebalance_stride = 10',
                    'industry_label_source = "omit"',
                    'limit_lock_mode = "disabled"',
                    'residualization_mode = "non_residual_target"',
                    "",
                ]
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "requires industry_label_source='industry_classification_pit'"):
            load_trend_research_input_build_case(case_path)


def test_industry_relative_strength_ranks_within_industry_leaders(self) -> None:
    from alpha_find_v2.trend_research_input_builder import _CandidateRow, _score_candidates

    common = {
        "trade_date": "20240102",
        "list_date": "20200102",
        "entry_open": 10.0,
        "exit_open": 11.0,
        "median_turnover_cny": 200_000_000.0,
        "turnover_baseline_cny": 180_000_000.0,
        "entry_suspended": False,
        "exit_suspended": False,
        "entry_liquidity_pass": True,
        "exit_liquidity_pass": True,
        "entry_limit_locked": False,
        "exit_limit_locked": False,
        "short_return_vol": 0.02,
    }
    scored = _score_candidates(
        candidates=[
            _CandidateRow(security_id="BANK_LEADER", ret_short=0.05, ret_long=0.05, **common),
            _CandidateRow(security_id="BANK_LAGGARD", ret_short=0.04, ret_long=0.04, **common),
            _CandidateRow(security_id="TECH_LEADER", ret_short=0.50, ret_long=0.50, **common),
            _CandidateRow(security_id="TECH_LAGGARD", ret_short=0.49, ret_long=0.49, **common),
        ],
        descriptor_weights={"industry_relative_strength": 1.0},
        industry_by_asset={
            "BANK_LEADER": "bank",
            "BANK_LAGGARD": "bank",
            "TECH_LEADER": "tech",
            "TECH_LAGGARD": "tech",
        },
    )

    ranked_ids = [item["candidate"].security_id for item in scored]
    self.assertLess(ranked_ids.index("BANK_LEADER"), ranked_ids.index("BANK_LAGGARD"))
    self.assertLess(ranked_ids.index("TECH_LEADER"), ranked_ids.index("TECH_LAGGARD"))
```

- [ ] **Step 2: Run the focused trend-builder tests and verify they fail**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_builder_requires_industry_labels_for_leader_pullback_descriptor_set tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_industry_relative_strength_ranks_within_industry_leaders -v
```

Expected:

- the missing-industry test fails because the new sleeve/config is not enforced yet
- the scoring test fails because `_score_candidates()` does not recognize `industry_relative_strength`

- [ ] **Step 3: Add the minimal builder support**

```python
SUPPORTED_DESCRIPTOR_IDS = {
    "medium_term_relative_strength",
    "industry_relative_strength",
    "trend_stability",
    "turnover_confirmation",
}


def _requires_industry_labels(descriptor_weights: dict[str, float]) -> bool:
    return "industry_relative_strength" in descriptor_weights


def _descriptor_values(
    *,
    descriptor_id: str,
    candidates: list[_CandidateRow],
    industry_by_asset: dict[str, str] | None,
) -> dict[str, float]:
    absolute_strength = {
        candidate.security_id: 0.5 * candidate.ret_short + 0.5 * candidate.ret_long
        for candidate in candidates
    }
    if descriptor_id == "medium_term_relative_strength":
        return absolute_strength
    if descriptor_id == "industry_relative_strength":
        if not industry_by_asset or not all(industry_by_asset.values()):
            raise ValueError("industry_relative_strength requires industry labels for every asset.")
        grouped = {}
        for asset_id, value in absolute_strength.items():
            grouped.setdefault(industry_by_asset[asset_id], []).append(value)
        industry_means = {
            industry: sum(values) / len(values)
            for industry, values in grouped.items()
        }
        return {
            asset_id: value - industry_means[industry_by_asset[asset_id]]
            for asset_id, value in absolute_strength.items()
        }
```

```python
def _descriptor_zscores(
    *,
    descriptor_id: str,
    values_by_asset: dict[str, float],
    industry_by_asset: dict[str, str] | None,
) -> dict[str, float]:
    if descriptor_id == "industry_relative_strength":
        return zscore_map(values_by_asset)
    return zscore_map(values_by_asset)
```

Implementation notes for the real edit:

- gate `industry_label_source='industry_classification_pit'` in
  `load_trend_research_input_build_case()` only when the loaded descriptor set
  contains `industry_relative_strength`
- do not turn the old `trend_leadership_core` sleeve into a hard failure
- keep `industry_relative_branch_blocked` warnings only for the older optional
  omit-label path

- [ ] **Step 4: Run the focused tests to verify they pass**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_builder_requires_industry_labels_for_leader_pullback_descriptor_set tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_industry_relative_strength_ranks_within_industry_leaders -v
```

Expected:

- both tests pass

- [ ] **Step 5: Commit the builder support**

```bash
git add src/alpha_find_v2/trend_research_input_builder.py tests/test_trend_research_input_builder.py
git commit -m "Execute industry-relative strength in the trend builder"
```

## Task 3: Enforce `single_industry_name_cap` And Validate The New Selection Behavior

**Files:**

- Modify: `src/alpha_find_v2/trend_research_input_builder.py`
- Modify: `tests/test_trend_research_input_builder.py`

- [ ] **Step 1: Add a failing selection-cap test**

```python
def test_builder_limits_selected_names_per_industry(self) -> None:
    from alpha_find_v2.trend_research_input_builder import _CandidateRow, _select_with_industry_cap

    common = {
        "trade_date": "20240102",
        "list_date": "20200102",
        "entry_open": 10.0,
        "exit_open": 11.0,
        "median_turnover_cny": 200_000_000.0,
        "turnover_baseline_cny": 180_000_000.0,
        "entry_suspended": False,
        "exit_suspended": False,
        "entry_liquidity_pass": True,
        "exit_liquidity_pass": True,
        "entry_limit_locked": False,
        "exit_limit_locked": False,
        "short_return_vol": 0.02,
    }
    selected = _select_with_industry_cap(
        scored=[
            {"candidate": _CandidateRow(security_id="BANK_1", ret_short=0.10, ret_long=0.10, **common), "score": 4.0},
            {"candidate": _CandidateRow(security_id="BANK_2", ret_short=0.09, ret_long=0.09, **common), "score": 3.0},
            {"candidate": _CandidateRow(security_id="TECH_1", ret_short=0.08, ret_long=0.08, **common), "score": 2.0},
        ],
        industry_by_asset={"BANK_1": "bank", "BANK_2": "bank", "TECH_1": "tech"},
        holding_count=2,
        single_industry_name_cap=1,
    )

    self.assertEqual(
        [item["candidate"].security_id for item in selected],
        ["BANK_1", "TECH_1"],
    )
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_builder_limits_selected_names_per_industry -v
```

Expected:

- the test fails because no such selection helper or cap logic exists yet

- [ ] **Step 3: Implement the smallest possible cap-aware selector**

```python
def _select_with_industry_cap(
    *,
    scored: list[dict[str, object]],
    industry_by_asset: dict[str, str],
    holding_count: int,
    single_industry_name_cap: int,
) -> list[dict[str, object]]:
    if single_industry_name_cap <= 0:
        return scored[:holding_count]

    selected = []
    counts_by_industry: dict[str, int] = {}
    for item in scored:
        candidate = item["candidate"]
        industry = industry_by_asset.get(candidate.security_id, "").strip()
        if industry and counts_by_industry.get(industry, 0) >= single_industry_name_cap:
            continue
        selected.append(item)
        if industry:
            counts_by_industry[industry] = counts_by_industry.get(industry, 0) + 1
        if len(selected) >= holding_count:
            break
    return selected
```

Implementation notes for the real edit:

- read `single_industry_name_cap` from `loaded_case` or `sleeve.constraints`
- apply the cap after scoring and before target-weight generation
- if the cap reduces the sleeve below `holding_count`, keep the smaller set and
  do not backfill by weakening constraints

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder.TrendResearchInputBuilderTest.test_builder_limits_selected_names_per_industry -v
```

Expected:

- the test passes

- [ ] **Step 5: Commit the cap logic**

```bash
git add src/alpha_find_v2/trend_research_input_builder.py tests/test_trend_research_input_builder.py
git commit -m "Cap selected names by industry in the leader pullback sleeve"
```

## Task 4: Add The Versioned Example Chain And Smoke-Cover It

**Files:**

- Create: `research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml`
- Create: `research/examples/artifact_build_minimal/leader_pullback_continuation_v1_output.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/baseline_portfolio.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/candidate_portfolio.toml`
- Create: `research/examples/promotion_replay_real_output_leader_pullback/replay_case.toml`
- Create: `research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.toml`
- Modify: `tests/test_config_loader.py`

- [ ] **Step 1: Add failing example-chain tests**

```python
def test_leader_pullback_example_chain_points_to_new_sleeve(self) -> None:
    trend_input_case = tomllib.loads(
        (PROJECT_ROOT / "research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml").read_text(
            encoding="utf-8"
        )
    )
    replay_case = tomllib.loads(
        (PROJECT_ROOT / "research/examples/promotion_replay_real_output_leader_pullback/replay_case.toml").read_text(
            encoding="utf-8"
        )
    )
    audit_case = tomllib.loads(
        (PROJECT_ROOT / "research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.toml").read_text(
            encoding="utf-8"
        )
    )

    self.assertEqual(
        trend_input_case["sleeve_path"],
        "config/sleeves/leader_pullback_continuation_v1.toml",
    )
    self.assertEqual(
        replay_case["artifact_paths"],
        ["output/leader_pullback_continuation_v1_artifact.json"],
    )
    self.assertEqual(
        audit_case["trend_research_input_build_case_path"],
        "research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml",
    )
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_leader_pullback_example_chain_points_to_new_sleeve -v
```

Expected:

- the test fails because the example chain files do not exist yet

- [ ] **Step 3: Create the minimal example files**

```toml
# research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml
schema_version = 1
artifact_type = "trend_research_input_build_case"
case_id = "leader_pullback_continuation_v1_duckdb"
description = "Build leader-pullback continuation observation inputs from the isolated V2 research-source DuckDB."
sleeve_path = "config/sleeves/leader_pullback_continuation_v1.toml"
source_db_path = "output/research_source.duckdb"
output_path = "output/leader_pullback_continuation_v1_input.json"
start_date = "20210305"
end_date = "20260423"
min_listing_days = 120
lookback_days = 60
short_window_days = 20
turnover_window_days = 20
rebalance_stride = 10
industry_label_source = "industry_classification_pit"
industry_schema = "sw2021_l1"
limit_lock_mode = "cn_a_directional_open_lock"
residualization_mode = "non_residual_target"
exclude_boards = ["beijing"]
```

```toml
# research/examples/artifact_build_minimal/leader_pullback_continuation_v1_output.toml
schema_version = 1
artifact_type = "sleeve_artifact_build_case"
case_id = "leader_pullback_continuation_v1_output_build"
description = "Build the leader-pullback continuation sleeve artifact from the generated observation input."
sleeve_path = "config/sleeves/leader_pullback_continuation_v1.toml"
input_path = "output/leader_pullback_continuation_v1_input.json"
output_path = "output/leader_pullback_continuation_v1_artifact.json"
```

```toml
# research/examples/promotion_replay_real_output_leader_pullback/replay_case.toml
schema_version = 1
artifact_type = "portfolio_promotion_replay_case"
case_id = "leader_pullback_continuation_real_output_replay"
description = "Replay a baseline leader-pullback book against the same book for versioned evidence and later portfolio comparison."
baseline_portfolio_path = "research/examples/promotion_replay_real_output_leader_pullback/baseline_portfolio.toml"
candidate_portfolio_path = "research/examples/promotion_replay_real_output_leader_pullback/candidate_portfolio.toml"
default_cost_model_path = "config/cost_models/base_a_share_cash.toml"
additional_cost_model_paths = ["config/cost_models/high_a_share_cash.toml"]
benchmark_state_path = "output/csi800_benchmark_state_history.json"
artifact_paths = ["output/leader_pullback_continuation_v1_artifact.json"]
periods_per_year = 26
max_component_correlation = 0.60
correlation_to_existing_portfolio = 0.55
turnover_budget = 0.10
market_data_source_db_path = "output/research_source.duckdb"
market_data_quality_audit_path = "output/audits/market_data_quality_20260429.json"
```

```toml
# research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.toml
schema_version = 1
artifact_type = "multi_year_validation_audit_build_case"
case_id = "leader_pullback_continuation_v1_audit_build"
description = "Rebuild the leader-pullback continuation multi-year audit from the versioned observation and replay chain."
candidate_id = "leader_pullback_continuation_v1"
portfolio_path = "research/examples/promotion_replay_real_output_leader_pullback/candidate_portfolio.toml"
benchmark_state_build_case_path = "research/examples/benchmark_state_build_minimal/csi800.toml"
trend_research_input_build_case_path = "research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml"
replay_case_path = "research/examples/promotion_replay_real_output_leader_pullback/replay_case.toml"
portfolio_backtest_result_path = "output/leader_pullback_continuation_v1_daily_backtest.json"
minimum_calendar_years = 5.0
strategy_min_active_ir = 0.30
strategy_max_drawdown = 0.18
strategy_max_turnover = 12.0
output_path = "research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.json"
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_leader_pullback_example_chain_points_to_new_sleeve -v
```

Expected:

- the test passes

- [ ] **Step 5: Run end-to-end verification for the touched areas**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_config_loader -v
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder -v
git diff --check
```

Expected:

- both test modules pass
- `git diff --check` reports no whitespace or patch-format issues

- [ ] **Step 6: Commit the example chain and final verification**

```bash
git add research/examples/trend_input_build_minimal/leader_pullback_continuation_v1.toml research/examples/artifact_build_minimal/leader_pullback_continuation_v1_output.toml research/examples/promotion_replay_real_output_leader_pullback research/examples/deployment_minimal/leader_pullback_continuation_multi_year_validation_audit_v1.toml tests/test_config_loader.py
git commit -m "Add the leader pullback continuation example chain"
```

## Self-Review Checklist

- Spec coverage:
  - new thesis / descriptor set / sleeve: Task 1
  - executable `industry_relative_strength`: Task 2
  - fail-closed industry dependency: Task 2
  - `single_industry_name_cap`: Task 3
  - example replay / audit chain: Task 4
- Placeholder scan:
  - no `TODO`, `TBD`, or “similar to above” shortcuts remain
- Type consistency:
  - use `leader_pullback_continuation` for the thesis id
  - use `leader_pullback_continuation_v1` for descriptor set, sleeve, and
    example artifact ids

## Execution Notes

- Run this plan in an isolated worktree because the current workspace already
  contains unrelated dirty docs.
- Do not retrofit the frozen `trend_leadership_shadow_live_v1` bundle to use the
  new descriptor set.
- Keep changes small and reversible; avoid widening the replay / audit scope
  beyond the new object chain.
