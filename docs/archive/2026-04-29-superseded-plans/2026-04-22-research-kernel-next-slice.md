# Research Kernel Next Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the minimum V2 research-kernel objects needed to connect an economic thesis to an executable target and a portfolio promotion decision.

**Architecture:** Keep V2 thesis-first and config-first. Add explicit config registries for descriptors, descriptor sets, executable residual targets, and promotion gates; then wire sleeves and portfolio recipes to reference those objects through typed models and loader functions. Verification stays at the registry/linkage layer only, not yet at the simulator or optimizer layer.

**Tech Stack:** Python 3.11, stdlib `tomllib`, stdlib `unittest`, TOML config registries, argparse CLI

---

### Task 1: Define the new research object files

**Files:**
- Create: `config/descriptors/sector_relative_valuation.toml`
- Create: `config/descriptors/profitability_quality.toml`
- Create: `config/descriptors/accrual_quality.toml`
- Create: `config/descriptors/leverage_conservatism.toml`
- Create: `config/descriptors/estimate_revision_breadth.toml`
- Create: `config/descriptors/post_earnings_drift_signal.toml`
- Create: `config/descriptor_sets/fundamental_rerating_core.toml`
- Create: `config/descriptor_sets/earnings_drift_core.toml`
- Create: `config/targets/open_t1_to_open_t20_residual_net_cost.toml`
- Create: `config/targets/open_t1_to_open_t05_residual_net_cost.toml`
- Create: `config/promotion_gates/a_share_core_portfolio_gate.toml`
- Modify: `config/sleeves/fundamental_rerating_core.toml`
- Modify: `config/sleeves/earnings_drift_core.toml`
- Modify: `config/portfolio/a_share_core.toml`

- [ ] **Step 1: Add a failing test that expects the new registries and links to exist**

```python
def test_sleeve_references_descriptor_set_and_target(self) -> None:
    sleeve = load_sleeve(CONFIG_ROOT / "sleeves" / "fundamental_rerating_core.toml")
    descriptor_set = load_descriptor_set(CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml")
    target = load_target(CONFIG_ROOT / "targets" / f"{sleeve.target_id}.toml")

    self.assertEqual(descriptor_set.thesis_id, sleeve.thesis_id)
    self.assertEqual(target.trade_entry, "next_day_open")
```

- [ ] **Step 2: Run the focused test and confirm it fails because the new loader/model functions do not exist yet**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_sleeve_references_descriptor_set_and_target -v`
Expected: FAIL with missing loader function or missing model attributes.

- [ ] **Step 3: Add the TOML registries with finance-first semantics**

```toml
id = "open_t1_to_open_t20_residual_net_cost"
name = "Next-Day Open to 20-Day Open Residual Return Net Cost"
horizon_days = 20
trade_entry = "next_day_open"
trade_exit = "open_on_horizon"
return_basis = "open_to_open"
cost_model = "base_a_share_cash"
residualization = ["benchmark", "industry", "size", "beta"]
eligibility = ["not_suspended", "not_limit_locked", "liquidity_pass"]
```

- [ ] **Step 4: Re-run the focused test and confirm it still fails at the missing Python objects, not at missing files**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_sleeve_references_descriptor_set_and_target -v`
Expected: FAIL with missing loader function or dataclass field, while file-not-found errors are gone.

### Task 2: Wire typed models, loaders, and CLI commands

**Files:**
- Modify: `src/alpha_find_v2/models.py`
- Modify: `src/alpha_find_v2/config_loader.py`
- Modify: `src/alpha_find_v2/cli.py`

- [ ] **Step 1: Add a failing test for CLI and portfolio gate loading**

```python
def test_portfolio_links_to_promotion_gate(self) -> None:
    portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
    gate = load_promotion_gate(CONFIG_ROOT / "promotion_gates" / f"{portfolio.promotion_gate_id}.toml")

    self.assertEqual(gate.scope, "portfolio")
    self.assertIn("max_component_correlation", gate.correlation_limits)
```

- [ ] **Step 2: Run the focused test and confirm it fails before implementation**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_portfolio_links_to_promotion_gate -v`
Expected: FAIL with missing loader function or missing `promotion_gate_id`.

- [ ] **Step 3: Add minimal typed models and loaders**

```python
@dataclass(slots=True)
class DescriptorSet:
    id: str
    thesis_id: str
    descriptor_ids: list[str] = field(default_factory=list)
    combination: JsonMap = field(default_factory=dict)

def load_descriptor_set(path: Path | str) -> DescriptorSet:
    return DescriptorSet.from_toml(_read_toml(path))
```

- [ ] **Step 4: Extend the CLI with inspect commands for the new registries**

```python
subparsers.add_parser("list-descriptor-sets", help="List descriptor-set config files.")
show_target = subparsers.add_parser("show-target", help="Show an executable target config.")
show_gate = subparsers.add_parser("show-promotion-gate", help="Show a promotion gate config.")
```

- [ ] **Step 5: Run the focused tests and CLI inspection commands**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest tests.test_config_loader.ConfigLoaderTest.test_portfolio_links_to_promotion_gate -v`
Expected: PASS

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m alpha_find_v2 show-target --path config/targets/open_t1_to_open_t20_residual_net_cost.toml`
Expected: JSON containing `trade_entry`, `horizon_days`, and `residualization`.

### Task 3: Lock the registry contract with regression tests

**Files:**
- Modify: `tests/test_config_loader.py`
- Optionally modify: `README.md`

- [ ] **Step 1: Add a failing test that asserts the two initial descriptor sets, target linkage, and portfolio gate linkage are internally consistent**

```python
def test_descriptor_set_members_match_thesis_data_needs(self) -> None:
    descriptor_set = load_descriptor_set(CONFIG_ROOT / "descriptor_sets" / "fundamental_rerating_core.toml")
    thesis = load_thesis(CONFIG_ROOT / "theses" / "fundamental_rerating.toml")

    self.assertTrue(set(descriptor_set.required_data).issubset(set(thesis.required_data)))
    self.assertEqual(descriptor_set.target_id, "open_t1_to_open_t20_residual_net_cost")
```

- [ ] **Step 2: Run the full suite and confirm the new test fails for the expected missing linkage before the final edit**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest discover -s tests -v`
Expected: FAIL in the newly added linkage test only.

- [ ] **Step 3: Make the minimal final config/test adjustments so all registry tests pass**

```python
self.assertEqual(portfolio.promotion_gate_id, "a_share_core_portfolio_gate")
self.assertEqual(
    {path.stem for path in list_configs("descriptor_sets")},
    {"earnings_drift_core", "fundamental_rerating_core"},
)
```

- [ ] **Step 4: Run complete verification**

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m unittest discover -s tests -v`
Expected: PASS

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m alpha_find_v2 list-descriptor-sets`
Expected: JSON array with `earnings_drift_core` and `fundamental_rerating_core`

Run: `cd /home/nan/alpha-find-v2 && PYTHONPATH=src python3 -m alpha_find_v2 show-promotion-gate --path config/promotion_gates/a_share_core_portfolio_gate.toml`
Expected: JSON containing gate thresholds and correlation constraints.
