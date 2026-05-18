# Kline Structure Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable pure-daily-bar structure engine that extracts confirmed swing points, trend state, reversal/continuation patterns, and key levels as a standalone public layer for later strategy use.

**Architecture:** Keep the core structure logic in a new pure module with no DuckDB or strategy dependencies. Add a separate DuckDB-backed builder that slices point-in-time daily bars into per-security snapshots, writes a JSON artifact, and exposes a CLI entry point. Do not wire this into `trend_research_input_builder.py` in `v1`; keep the public structure layer isolated and testable first.

**Tech Stack:** Python 3.11+, stdlib dataclasses/json/pathlib, existing DuckDB access pattern, existing CLI style, `pytest`/`unittest` test suite.

---

## File Map

**Create**

- `src/alpha_find_v2/kline_structure.py`
- `src/alpha_find_v2/kline_structure_builder.py`
- `tests/test_kline_structure.py`
- `tests/test_kline_structure_builder.py`
- `research/examples/kline_structure_build_minimal/demo_case.toml`

**Modify**

- `src/alpha_find_v2/cli.py`
- `README.md`

**Do Not Modify In v1**

- `src/alpha_find_v2/trend_research_input_builder.py`
- `config/descriptors/*.toml`
- `config/descriptor_sets/*.toml`

Rationale:

- The new layer is public structure infrastructure, not a sleeve descriptor yet.
- Keeping it separate avoids making the already-large trend builder harder to audit.
- The repo has no declared `scipy` dependency in `pyproject.toml`, so `v1` must implement local extrema with stdlib logic rather than adding a dependency.

## Task 1: Create the Pure Swing-Extraction Core

**Files:**

- Create: `src/alpha_find_v2/kline_structure.py`
- Test: `tests/test_kline_structure.py`

- [ ] **Step 1: Write the failing swing-extraction tests**

Add these tests to `tests/test_kline_structure.py`:

```python
from __future__ import annotations

import unittest

from alpha_find_v2.kline_structure import (
    DailyBar,
    KlineStructureParams,
    extract_confirmed_swings,
)


def _bars_from_closes(closes: list[float]) -> list[DailyBar]:
    bars: list[DailyBar] = []
    for index, close in enumerate(closes):
        bars.append(
            DailyBar(
                trade_date=f"2024{index + 1:04d}",
                open=close,
                high=close * 1.01,
                low=close * 0.99,
                close=close,
                volume=1000.0 + index,
                amount=1000000.0 + (index * 1000.0),
            )
        )
    return bars


class KlineStructureSwingTest(unittest.TestCase):
    def test_extract_confirmed_swings_compresses_same_type_points(self) -> None:
        bars = _bars_from_closes(
            [10.0, 11.5, 12.0, 11.8, 11.7, 10.4, 9.9, 10.2, 10.9, 10.7, 11.4]
        )

        swings = extract_confirmed_swings(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.03),
        )

        self.assertEqual([s.kind for s in swings], ["peak", "trough"])
        self.assertEqual(swings[0].pivot_date, "20240003")
        self.assertEqual(swings[1].pivot_date, "20240007")

    def test_extract_confirmed_swings_drops_small_reversals(self) -> None:
        bars = _bars_from_closes(
            [10.0, 10.2, 10.1, 10.25, 10.12, 10.4, 10.3, 10.6]
        )

        swings = extract_confirmed_swings(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.05),
        )

        self.assertLessEqual(len(swings), 1)

    def test_last_swing_stays_unconfirmed_until_broken(self) -> None:
        bars = _bars_from_closes(
            [10.0, 9.6, 9.2, 9.5, 9.9, 10.3, 10.1, 10.2]
        )

        swings = extract_confirmed_swings(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.03),
        )

        self.assertTrue(swings)
        self.assertFalse(swings[-1].is_confirmed)
        self.assertIsNone(swings[-1].confirm_date)
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run:

```bash
pytest tests/test_kline_structure.py -q
```

Expected:

- FAIL with `ModuleNotFoundError: No module named 'alpha_find_v2.kline_structure'`

- [ ] **Step 3: Implement the minimum pure swing module**

Create `src/alpha_find_v2/kline_structure.py` with this starting implementation:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DailyBar:
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


@dataclass(slots=True, frozen=True)
class KlineStructureParams:
    extrema_order: int = 10
    min_move_pct: float = 0.05
    small_max_bars: int = 19
    mid_max_bars: int = 60


@dataclass(slots=True, frozen=True)
class SwingPoint:
    pivot_date: str
    confirm_date: str | None
    price: float
    kind: str
    span_bars: int
    level: str
    is_confirmed: bool


def extract_confirmed_swings(
    bars: list[DailyBar],
    params: KlineStructureParams,
) -> list[SwingPoint]:
    raw = _raw_swings(bars, params.extrema_order)
    compressed = _compress_same_kind(raw)
    filtered = _filter_min_move(compressed, params.min_move_pct)
    return _mark_confirmation(filtered, params)


def _raw_swings(bars: list[DailyBar], order: int) -> list[tuple[int, float, str]]:
    swings: list[tuple[int, float, str]] = []
    for index in range(order, len(bars) - order):
        high = bars[index].high
        low = bars[index].low
        left = bars[index - order : index]
        right = bars[index + 1 : index + 1 + order]
        if all(high >= item.high for item in left + right):
            swings.append((index, high, "peak"))
        if all(low <= item.low for item in left + right):
            swings.append((index, low, "trough"))
    swings.sort(key=lambda item: item[0])
    return swings


def _compress_same_kind(
    swings: list[tuple[int, float, str]],
) -> list[tuple[int, float, str]]:
    if not swings:
        return []
    compressed = [swings[0]]
    for swing in swings[1:]:
        last_index, last_price, last_kind = compressed[-1]
        index, price, kind = swing
        if kind != last_kind:
            compressed.append(swing)
            continue
        if kind == "peak" and price >= last_price:
            compressed[-1] = swing
        elif kind == "trough" and price <= last_price:
            compressed[-1] = swing
    return compressed


def _filter_min_move(
    swings: list[tuple[int, float, str]],
    min_move_pct: float,
) -> list[tuple[int, float, str]]:
    if not swings:
        return []
    accepted = [swings[0]]
    for swing in swings[1:]:
        last_price = accepted[-1][1]
        if last_price <= 0:
            continue
        change = abs(swing[1] - last_price) / last_price
        if change >= min_move_pct:
            accepted.append(swing)
    return accepted


def _mark_confirmation(
    swings: list[tuple[int, float, str]],
    params: KlineStructureParams,
) -> list[SwingPoint]:
    output: list[SwingPoint] = []
    for position, (index, price, kind) in enumerate(swings):
        span_bars = 0
        if position >= 2:
            span_bars = index - swings[position - 2][0]
        level = "small"
        if span_bars > params.mid_max_bars:
            level = "big"
        elif span_bars >= params.small_max_bars + 1:
            level = "mid"
        is_last = position == len(swings) - 1
        output.append(
            SwingPoint(
                pivot_date=f"2024{index + 1:04d}",
                confirm_date=None if is_last else f"2024{index + 1:04d}",
                price=price,
                kind=kind,
                span_bars=span_bars,
                level=level,
                is_confirmed=not is_last,
            )
        )
    return output
```

Then immediately replace the hard-coded `pivot_date` formatter with `bars[index].trade_date` before moving on.

- [ ] **Step 4: Re-run the tests and make them pass**

Run:

```bash
pytest tests/test_kline_structure.py -q
```

Expected:

- PASS
- `3 passed`

- [ ] **Step 5: Commit the pure swing-extraction slice**

Run:

```bash
git add tests/test_kline_structure.py src/alpha_find_v2/kline_structure.py
git commit -m "Establish a reusable daily-bar swing extraction core" -m "Add a pure kline structure module with local-extrema detection, same-kind compression, minimum-move filtering, and explicit last-swing non-confirmation.

Constraint: No new scipy dependency in v1
Rejected: scipy.signal.argrelextrema | new dependency without explicit approval
Confidence: medium
Scope-risk: narrow
Directive: Keep this module strategy-agnostic and free of DuckDB or sleeve logic
Tested: pytest tests/test_kline_structure.py -q
Not-tested: Full-market bar history and pattern recognition"
```

## Task 2: Add Trend State and Pattern Detection

**Files:**

- Modify: `src/alpha_find_v2/kline_structure.py`
- Modify: `tests/test_kline_structure.py`

- [ ] **Step 1: Write failing trend and pattern tests**

Append these tests to `tests/test_kline_structure.py`:

```python
from alpha_find_v2.kline_structure import analyze_structure


class KlineStructureTrendTest(unittest.TestCase):
    def test_higher_troughs_create_uptrend_candidate(self) -> None:
        bars = _bars_from_closes(
            [12.0, 11.0, 10.2, 10.8, 11.4, 10.6, 11.2, 11.9, 11.1, 12.4, 12.8]
        )

        snapshot = analyze_structure(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.03),
        )

        self.assertEqual(snapshot.mid_trend, "uptrend_candidate")
        self.assertEqual(snapshot.big_trend, "range")

    def test_break_above_prior_peak_confirms_uptrend(self) -> None:
        bars = _bars_from_closes(
            [12.0, 11.0, 10.0, 10.6, 11.2, 10.5, 11.0, 11.5, 11.1, 12.1, 12.9, 13.3]
        )

        snapshot = analyze_structure(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.03),
        )

        self.assertEqual(snapshot.mid_trend, "uptrend_confirmed")
        self.assertTrue(snapshot.breakout_flags["above_last_mid_peak"])

    def test_double_bottom_reaches_geometry_complete_before_breakout(self) -> None:
        bars = _bars_from_closes(
            [12.0, 11.1, 10.0, 10.8, 11.3, 10.1, 10.6, 10.9, 11.2]
        )

        snapshot = analyze_structure(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.03),
        )

        double_bottoms = [
            pattern for pattern in snapshot.active_patterns if pattern.pattern_type == "double_bottom"
        ]
        self.assertEqual(double_bottoms[0].pattern_stage, "geometry_complete")
        self.assertFalse(double_bottoms[0].breakout_confirmed)
```

- [ ] **Step 2: Run the tests and confirm they fail on missing analysis symbols**

Run:

```bash
pytest tests/test_kline_structure.py -q
```

Expected:

- FAIL with `ImportError` or `AttributeError` for `analyze_structure`

- [ ] **Step 3: Implement trend/pattern dataclasses and the first analysis pass**

Add this code to `src/alpha_find_v2/kline_structure.py`:

```python
@dataclass(slots=True, frozen=True)
class PatternSignal:
    pattern_type: str
    pattern_stage: str
    context_trend: str
    breakout_price: float | None
    breakout_confirmed: bool
    volume_confirmed: bool
    invalidated: bool


@dataclass(slots=True, frozen=True)
class StructureSnapshot:
    as_of_date: str
    big_trend: str
    mid_trend: str
    trend_stage: str
    confirmed_swings: list[SwingPoint]
    active_patterns: list[PatternSignal]
    key_supports: list[float]
    key_resistances: list[float]
    breakout_flags: dict[str, bool]
    volume_confirmation_flags: dict[str, bool]


def analyze_structure(
    bars: list[DailyBar],
    params: KlineStructureParams,
) -> StructureSnapshot:
    swings = extract_confirmed_swings(bars, params)
    big_trend, mid_trend = _classify_trends(swings)
    active_patterns = _detect_patterns(swings, bars)
    key_supports, key_resistances = _key_levels(swings)
    last_mid_peak = next((s.price for s in reversed(swings) if s.kind == "peak" and s.level in {"mid", "big"}), None)
    breakout_flags = {
        "above_last_mid_peak": bool(last_mid_peak is not None and bars[-1].close > last_mid_peak),
        "above_last_big_peak": bool(last_mid_peak is not None and bars[-1].close > last_mid_peak and big_trend == "uptrend_confirmed"),
    }
    return StructureSnapshot(
        as_of_date=bars[-1].trade_date,
        big_trend=big_trend,
        mid_trend=mid_trend,
        trend_stage=f"{mid_trend}_inside_{big_trend}",
        confirmed_swings=[s for s in swings if s.is_confirmed],
        active_patterns=active_patterns,
        key_supports=key_supports,
        key_resistances=key_resistances,
        breakout_flags=breakout_flags,
        volume_confirmation_flags={"breakout_amount_confirmed": False},
    )
```

Then add private helpers in the same module:

```python
def _classify_trends(swings: list[SwingPoint]) -> tuple[str, str]:
    confirmed = [s for s in swings if s.is_confirmed]
    troughs = [s for s in confirmed if s.kind == "trough"]
    peaks = [s for s in confirmed if s.kind == "peak"]

    mid_trend = "range"
    if len(troughs) >= 2 and troughs[-1].price > troughs[-2].price:
        mid_trend = "uptrend_candidate"
    if len(peaks) >= 2 and peaks[-1].price < peaks[-2].price:
        mid_trend = "downtrend_candidate"
    if mid_trend == "uptrend_candidate" and len(peaks) >= 2 and peaks[-1].price > peaks[-2].price:
        mid_trend = "uptrend_confirmed"
    if mid_trend == "downtrend_candidate" and len(troughs) >= 2 and troughs[-1].price < troughs[-2].price:
        mid_trend = "downtrend_confirmed"

    big_trend = "range"
    big_troughs = [s for s in troughs if s.level == "big"]
    big_peaks = [s for s in peaks if s.level == "big"]
    if len(big_troughs) >= 2 and big_troughs[-1].price > big_troughs[-2].price:
        big_trend = "uptrend_candidate"
    elif len(big_peaks) >= 2 and big_peaks[-1].price < big_peaks[-2].price:
        big_trend = "downtrend_candidate"
    return big_trend, mid_trend


def _detect_patterns(
    swings: list[SwingPoint],
    bars: list[DailyBar],
) -> list[PatternSignal]:
    patterns: list[PatternSignal] = []
    confirmed = [s for s in swings if s.is_confirmed]
    troughs = [s for s in confirmed if s.kind == "trough"]
    peaks = [s for s in confirmed if s.kind == "peak"]
    if len(troughs) >= 2 and len(peaks) >= 1:
        left, right = troughs[-2], troughs[-1]
        if left.price > 0 and abs(left.price - right.price) / left.price <= 0.03:
            middle_peak = peaks[-1]
            patterns.append(
                PatternSignal(
                    pattern_type="double_bottom",
                    pattern_stage=(
                        "breakout_confirmed"
                        if bars[-1].close > middle_peak.price
                        else "geometry_complete"
                    ),
                    context_trend="downtrend_candidate",
                    breakout_price=middle_peak.price,
                    breakout_confirmed=bars[-1].close > middle_peak.price,
                    volume_confirmed=False,
                    invalidated=False,
                )
            )
    return patterns


def _key_levels(swings: list[SwingPoint]) -> tuple[list[float], list[float]]:
    supports = [s.price for s in reversed(swings) if s.kind == "trough" and s.is_confirmed][:2]
    resistances = [s.price for s in reversed(swings) if s.kind == "peak" and s.is_confirmed][:2]
    return supports, resistances
```

- [ ] **Step 4: Re-run the tests and make them pass**

Run:

```bash
pytest tests/test_kline_structure.py -q
```

Expected:

- PASS
- `6 passed`

- [ ] **Step 5: Commit the analysis layer**

Run:

```bash
git add tests/test_kline_structure.py src/alpha_find_v2/kline_structure.py
git commit -m "Turn confirmed swings into trend and pattern structure facts" -m "Add a first analysis pass that classifies multi-level trend state, exposes breakout flags, and detects a geometry-first double-bottom pattern.

Constraint: Public layer must remain independent from sleeve scoring
Rejected: Add structure fields directly into trend_research_input_builder | couples public method to one strategy lane too early
Confidence: medium
Scope-risk: moderate
Directive: Add new patterns behind the same pure-structure interface instead of scattering checks into builders
Tested: pytest tests/test_kline_structure.py -q
Not-tested: Large-universe histories and volume-confirmation logic"
```

## Task 3: Add Artifact Models and a DuckDB-Backed Builder

**Files:**

- Create: `src/alpha_find_v2/kline_structure_builder.py`
- Create: `tests/test_kline_structure_builder.py`

- [ ] **Step 1: Write the failing builder tests**

Create `tests/test_kline_structure_builder.py` with this starting coverage:

```python
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import tempfile
import unittest

import duckdb

from alpha_find_v2.kline_structure_builder import (
    build_kline_structure_artifact,
    load_kline_structure_build_case,
    write_kline_structure_artifact,
)


def _trading_days(start: date, count: int) -> list[str]:
    days: list[str] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


class KlineStructureBuilderTest(unittest.TestCase):
    def test_builder_uses_only_history_up_to_as_of_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 40)

            conn = duckdb.connect(str(source_db))
            conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR)")
            conn.execute("INSERT INTO market_trade_calendar VALUES " + ", ".join(f"('{d}')" for d in trade_dates))
            conn.execute(
                '''
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    turnover_value_cny DOUBLE
                )
                '''
            )
            rows = []
            for index, trade_date in enumerate(trade_dates):
                close = 10.0 + (index * 0.2)
                rows.append(
                    (
                        "600001.SH",
                        trade_date,
                        close,
                        close * 1.01,
                        close * 0.99,
                        close,
                        1000.0 + index,
                        1000000.0 + (index * 10000.0),
                    )
                )
            conn.executemany(
                "INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.close()

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "kline_structure_build_case"',
                        'case_id = "demo_case"',
                        'description = "Build a kline structure artifact."',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "kline_structure.json"}"',
                        'security_ids = ["600001.SH"]',
                        f'as_of_dates = ["{trade_dates[25]}"]',
                        'lookback_days = 30',
                        'extrema_order = 1',
                        'min_move_pct = 0.03',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_kline_structure_build_case(case_path)
            result = build_kline_structure_artifact(loaded_case)
            output_path = write_kline_structure_artifact(result, loaded_case.definition.output_path)

            self.assertEqual(result.steps[0].trade_date, trade_dates[25])
            self.assertEqual(result.steps[0].records[0].asset_id, "600001.SH")
            self.assertEqual(result.steps[0].records[0].as_of_date, trade_dates[25])
            self.assertTrue(output_path.exists())
```

- [ ] **Step 2: Run the new builder tests and confirm they fail**

Run:

```bash
pytest tests/test_kline_structure_builder.py -q
```

Expected:

- FAIL with `ModuleNotFoundError` for `alpha_find_v2.kline_structure_builder`

- [ ] **Step 3: Implement the builder, case loader, and artifact writer**

Create `src/alpha_find_v2/kline_structure_builder.py` with this structure:

```python
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import tomllib

from .config_loader import PROJECT_ROOT
from .kline_structure import DailyBar, KlineStructureParams, analyze_structure


@dataclass(slots=True)
class KlineStructureBuildCaseDefinition:
    case_id: str
    description: str
    source_db_path: str
    output_path: str
    security_ids: list[str] = field(default_factory=list)
    as_of_dates: list[str] = field(default_factory=list)
    lookback_days: int = 500
    extrema_order: int = 10
    min_move_pct: float = 0.05

    @classmethod
    def from_toml(cls, data: dict[str, object]) -> "KlineStructureBuildCaseDefinition":
        if int(data.get("schema_version", 0)) != 1:
            raise ValueError("Unsupported kline structure build case schema version.")
        if str(data.get("artifact_type", "")) != "kline_structure_build_case":
            raise ValueError("Unsupported kline structure build case type.")
        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            source_db_path=str(data["source_db_path"]),
            output_path=str(data["output_path"]),
            security_ids=[str(item) for item in data.get("security_ids", [])],
            as_of_dates=[str(item) for item in data.get("as_of_dates", [])],
            lookback_days=int(data.get("lookback_days", 500)),
            extrema_order=int(data.get("extrema_order", 10)),
            min_move_pct=float(data.get("min_move_pct", 0.05)),
        )


@dataclass(slots=True)
class LoadedKlineStructureBuildCase:
    definition: KlineStructureBuildCaseDefinition
    source_db_path: Path


@dataclass(slots=True)
class KlineStructureRecord:
    asset_id: str
    as_of_date: str
    big_trend: str
    mid_trend: str
    trend_stage: str
    active_patterns: list[dict[str, object]]
    key_supports: list[float]
    key_resistances: list[float]
    breakout_flags: dict[str, bool]
    volume_confirmation_flags: dict[str, bool]
    confirmed_swings: list[dict[str, object]]


@dataclass(slots=True)
class KlineStructureStep:
    trade_date: str
    records: list[KlineStructureRecord] = field(default_factory=list)


@dataclass(slots=True)
class KlineStructureArtifact:
    case_id: str
    description: str
    steps: list[KlineStructureStep] = field(default_factory=list)
```

Then add the build and write functions:

```python
def load_kline_structure_build_case(path: Path | str) -> LoadedKlineStructureBuildCase:
    definition = KlineStructureBuildCaseDefinition.from_toml(_read_toml(path))
    return LoadedKlineStructureBuildCase(
        definition=definition,
        source_db_path=_resolve_project_path(definition.source_db_path),
    )


def build_kline_structure_artifact(
    loaded_case: LoadedKlineStructureBuildCase,
) -> KlineStructureArtifact:
    calendar = _load_trade_calendar(loaded_case.source_db_path)
    by_security = _load_bars(
        source_db_path=loaded_case.source_db_path,
        security_ids=loaded_case.definition.security_ids,
        start_date=min(loaded_case.definition.as_of_dates),
        end_date=max(loaded_case.definition.as_of_dates),
        lookback_days=loaded_case.definition.lookback_days,
        calendar=calendar,
    )
    params = KlineStructureParams(
        extrema_order=loaded_case.definition.extrema_order,
        min_move_pct=loaded_case.definition.min_move_pct,
    )
    steps: list[KlineStructureStep] = []
    for trade_date in loaded_case.definition.as_of_dates:
        records: list[KlineStructureRecord] = []
        for asset_id in loaded_case.definition.security_ids:
            bars = [bar for bar in by_security.get(asset_id, []) if bar.trade_date <= trade_date]
            if len(bars) < 5:
                continue
            snapshot = analyze_structure(bars, params)
            records.append(
                KlineStructureRecord(
                    asset_id=asset_id,
                    as_of_date=snapshot.as_of_date,
                    big_trend=snapshot.big_trend,
                    mid_trend=snapshot.mid_trend,
                    trend_stage=snapshot.trend_stage,
                    active_patterns=[asdict(item) for item in snapshot.active_patterns],
                    key_supports=snapshot.key_supports,
                    key_resistances=snapshot.key_resistances,
                    breakout_flags=snapshot.breakout_flags,
                    volume_confirmation_flags=snapshot.volume_confirmation_flags,
                    confirmed_swings=[asdict(item) for item in snapshot.confirmed_swings],
                )
            )
        steps.append(KlineStructureStep(trade_date=trade_date, records=records))
    return KlineStructureArtifact(
        case_id=loaded_case.definition.case_id,
        description=loaded_case.definition.description,
        steps=steps,
    )


def write_kline_structure_artifact(
    artifact: KlineStructureArtifact,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "kline_structure_artifact",
        "case_id": artifact.case_id,
        "description": artifact.description,
        "steps": [asdict(step) for step in artifact.steps],
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target
```

Mirror the helper style from `regime_overlay_observation_builder.py` for `_read_toml`, `_resolve_project_path`, `_load_trade_calendar`, and `_load_bars`.

- [ ] **Step 4: Re-run the builder tests and make them pass**

Run:

```bash
pytest tests/test_kline_structure_builder.py -q
```

Expected:

- PASS
- `1 passed`

- [ ] **Step 5: Commit the builder slice**

Run:

```bash
git add tests/test_kline_structure_builder.py src/alpha_find_v2/kline_structure_builder.py
git commit -m "Add a standalone point-in-time kline structure artifact builder" -m "Introduce a DuckDB-backed build case, per-date structure artifact, and JSON writer that reuse the pure kline structure engine without strategy coupling.

Constraint: Builder must read only point-in-time daily_bar_pit history
Rejected: Emit results inside existing sleeve_research_observation_input | wrong abstraction layer for a public structure method
Confidence: medium
Scope-risk: moderate
Directive: Keep builder output self-describing so later strategy lanes can depend on it without reaching into internals
Tested: pytest tests/test_kline_structure_builder.py -q
Not-tested: Multi-security scale and malformed-source-db diagnostics"
```

## Task 4: Expose the Builder Through the CLI and Example Case

**Files:**

- Modify: `src/alpha_find_v2/cli.py`
- Create: `research/examples/kline_structure_build_minimal/demo_case.toml`
- Modify: `README.md`

- [ ] **Step 1: Write a failing CLI smoke test**

Append this smoke test to `tests/test_kline_structure_builder.py`:

```python
import subprocess
import sys


    def test_cli_build_kline_structure_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 30)

            conn = duckdb.connect(str(source_db))
            conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR)")
            conn.execute("INSERT INTO market_trade_calendar VALUES " + ", ".join(f"('{d}')" for d in trade_dates))
            conn.execute(
                '''
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    turnover_value_cny DOUBLE
                )
                '''
            )
            conn.executemany(
                "INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ("600001.SH", d, 10.0, 10.2, 9.8, 10.0 + (i * 0.1), 1000.0, 1000000.0)
                    for i, d in enumerate(trade_dates)
                ],
            )
            conn.close()

            output_path = temp_root / "cli_output.json"
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "kline_structure_build_case"',
                        'case_id = "cli_case"',
                        'description = "CLI smoke case"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        'security_ids = ["600001.SH"]',
                        f'as_of_dates = ["{trade_dates[20]}"]',
                        'lookback_days = 25',
                        'extrema_order = 1',
                        'min_move_pct = 0.03',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            command = [
                sys.executable,
                "-m",
                "alpha_find_v2",
                "build-kline-structure",
                "--case",
                str(case_path),
            ]
            completed = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(output_path.exists())
```

- [ ] **Step 2: Run the builder test file and confirm the CLI smoke test fails**

Run:

```bash
pytest tests/test_kline_structure_builder.py -q
```

Expected:

- FAIL with `Unsupported command: build-kline-structure`

- [ ] **Step 3: Implement the CLI command, example case, and README usage**

Modify `src/alpha_find_v2/cli.py` imports:

```python
from .kline_structure_builder import (
    build_kline_structure_artifact,
    load_kline_structure_build_case,
    write_kline_structure_artifact,
)
```

Add a parser in `_parse_args()`:

```python
    build_kline_structure = subparsers.add_parser(
        "build-kline-structure",
        help="Build a point-in-time kline structure artifact from daily_bar_pit history.",
    )
    build_kline_structure.add_argument(
        "--case",
        default="research/examples/kline_structure_build_minimal/demo_case.toml",
        help="Path to the kline-structure build case TOML file.",
    )
```

Add a command handler in `main()` near the other build commands:

```python
    if args.command == "build-kline-structure":
        loaded_case = load_kline_structure_build_case(Path(args.case))
        artifact = build_kline_structure_artifact(loaded_case)
        output_path = write_kline_structure_artifact(
            artifact,
            loaded_case.definition.output_path,
        )
        _dump_json(
            {
                "case_id": artifact.case_id,
                "output_path": str(output_path),
                "trade_dates": [step.trade_date for step in artifact.steps],
                "record_count": sum(len(step.records) for step in artifact.steps),
            }
        )
        return
```

Create `research/examples/kline_structure_build_minimal/demo_case.toml`:

```toml
schema_version = 1
artifact_type = "kline_structure_build_case"
case_id = "demo_kline_structure_case"
description = "Build a single-security demo kline structure artifact."
source_db_path = "output/research_source.duckdb"
output_path = "output/demo_kline_structure.json"
security_ids = ["600036.SH"]
as_of_dates = ["20260423"]
lookback_days = 250
extrema_order = 10
min_move_pct = 0.05
```

Add one usage block to `README.md` near the other build commands:

```markdown
PYTHONPATH=src python -m alpha_find_v2 build-kline-structure --case research/examples/kline_structure_build_minimal/demo_case.toml
```

- [ ] **Step 4: Re-run the CLI smoke test and make it pass**

Run:

```bash
pytest tests/test_kline_structure_builder.py -q
```

Expected:

- PASS
- `2 passed`

- [ ] **Step 5: Commit the CLI and example slice**

Run:

```bash
git add src/alpha_find_v2/cli.py tests/test_kline_structure_builder.py research/examples/kline_structure_build_minimal/demo_case.toml README.md
git commit -m "Expose the kline structure builder as a first-class repo tool" -m "Add a dedicated CLI command, a checked-in demo case, and README usage so the structure engine can be exercised without touching sleeve scoring code.

Constraint: Must follow existing repo build-case and CLI patterns
Rejected: Ad hoc script under scripts/ | inconsistent with current repo operator surface
Confidence: high
Scope-risk: narrow
Directive: Keep the command output summary compact and machine-readable
Tested: pytest tests/test_kline_structure_builder.py -q
Not-tested: Demo case against the checked-in output database"
```

## Task 5: Tighten Edge Cases and Verify the Full Slice

**Files:**

- Modify: `tests/test_kline_structure.py`
- Modify: `tests/test_kline_structure_builder.py`
- Modify: `src/alpha_find_v2/kline_structure.py`
- Modify: `src/alpha_find_v2/kline_structure_builder.py`

- [ ] **Step 1: Add regression tests for edge cases from the spec**

Append these tests:

```python
class KlineStructureEdgeCaseTest(unittest.TestCase):
    def test_no_swings_returns_range_state(self) -> None:
        bars = _bars_from_closes([10.0] * 25)

        snapshot = analyze_structure(
            bars,
            KlineStructureParams(extrema_order=1, min_move_pct=0.05),
        )

        self.assertEqual(snapshot.big_trend, "range")
        self.assertEqual(snapshot.mid_trend, "range")
        self.assertEqual(snapshot.active_patterns, [])

    def test_builder_skips_unknown_security_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            conn = duckdb.connect(str(source_db))
            conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR)")
            conn.execute("INSERT INTO market_trade_calendar VALUES ('20240102')")
            conn.execute(
                '''
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    turnover_value_cny DOUBLE
                )
                '''
            )
            conn.close()

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "kline_structure_build_case"',
                        'case_id = "missing_security_case"',
                        'description = "Missing security case"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "missing.json"}"',
                        'security_ids = ["999999.SH"]',
                        'as_of_dates = ["20240102"]',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_kline_structure_build_case(case_path)
            artifact = build_kline_structure_artifact(loaded_case)
            self.assertEqual(len(artifact.steps[0].records), 0)
```

- [ ] **Step 2: Run the focused tests and confirm the new edge cases fail**

Run:

```bash
pytest tests/test_kline_structure.py tests/test_kline_structure_builder.py -q
```

Expected:

- FAIL on at least one missing edge-case behavior

- [ ] **Step 3: Tighten the implementation for deterministic edge-case handling**

Make these targeted adjustments:

In `src/alpha_find_v2/kline_structure.py`, early-return empty structure state:

```python
    if not bars:
        raise ValueError("kline structure analysis requires at least one daily bar.")
    swings = extract_confirmed_swings(bars, params)
    if not swings:
        return StructureSnapshot(
            as_of_date=bars[-1].trade_date,
            big_trend="range",
            mid_trend="range",
            trend_stage="range_inside_range",
            confirmed_swings=[],
            active_patterns=[],
            key_supports=[],
            key_resistances=[],
            breakout_flags={
                "above_last_mid_peak": False,
                "above_last_big_peak": False,
            },
            volume_confirmation_flags={"breakout_amount_confirmed": False},
        )
```

In `src/alpha_find_v2/kline_structure_builder.py`, tolerate sparse records:

```python
        for asset_id in loaded_case.definition.security_ids:
            bars = [bar for bar in by_security.get(asset_id, []) if bar.trade_date <= trade_date]
            if len(bars) < 5:
                continue
```

Also sort `security_ids` and `as_of_dates` once in the loader so artifact output stays deterministic.

- [ ] **Step 4: Run the full relevant verification suite**

Run:

```bash
pytest tests/test_kline_structure.py tests/test_kline_structure_builder.py tests/test_trend_research_input_builder.py -q
```

Expected:

- PASS
- Existing `trend_research_input_builder` tests remain green because the new slice is isolated

- [ ] **Step 5: Commit the edge-case hardening and verification pass**

Run:

```bash
git add tests/test_kline_structure.py tests/test_kline_structure_builder.py src/alpha_find_v2/kline_structure.py src/alpha_find_v2/kline_structure_builder.py
git commit -m "Harden the structure engine around empty and sparse histories" -m "Add deterministic edge-case behavior for flat ranges, missing securities, and sparse bar windows, then verify the new slice does not disturb the existing trend builder lane.

Constraint: Public structure layer must remain reversible and low-coupling
Rejected: Opportunistic integration into trend_research_input_builder during verification | expands scope and masks regressions
Confidence: high
Scope-risk: narrow
Directive: Keep future integrations additive; verify this standalone slice independently first
Tested: pytest tests/test_kline_structure.py tests/test_kline_structure_builder.py tests/test_trend_research_input_builder.py -q
Not-tested: Full-market runtime on production-sized DuckDB"
```

## Out of Scope for This Plan

- Adding a new descriptor or descriptor-set component that consumes structure output
- Mutating `sleeve_research_observation_input` schema
- Integrating structure results into `trend_research_input_builder.py`
- Adding `scipy` or other new dependencies
- Building chart-image exports or manual review UI

These should be separate follow-on plans once the standalone public layer is stable.

## Self-Review

Spec coverage:

- Confirmed swing extraction: covered in Task 1
- Trend state machine: covered in Task 2
- Pattern recognition baseline: covered in Task 2
- Key support/resistance output: covered in Task 2
- Public artifact output: covered in Task 3
- Repo CLI/operator surface: covered in Task 4
- Edge-case verification: covered in Task 5

Placeholder scan:

- No `TODO` / `TBD`
- No “implement later” placeholders
- Each task has explicit file paths, commands, and code snippets

Type consistency:

- Core module names are consistent across tasks:
  - `DailyBar`
  - `KlineStructureParams`
  - `SwingPoint`
  - `PatternSignal`
  - `StructureSnapshot`
  - `KlineStructureBuildCaseDefinition`
  - `KlineStructureArtifact`

