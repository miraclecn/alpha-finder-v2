# SW Industry PIT Official L1 Rebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore pre-cutover `sw2021_l1` coverage in `industry_classification_pit` by materializing official Shenwan old-code history from `StockClassifyUse_stock.xls` instead of dropping those rows as unresolved.

**Architecture:** Keep the existing official packet import shape, but narrow the fix to `L1`. Parse the official `2014to2021.xlsx` crosswalk as a hierarchy, derive `old_raw_code -> old_l1_code -> new_l1_code`, and use that mapping as a conservative L1 fallback while leaving current `L2`/`L3` behavior unchanged. Lock the change with a regression test that proves a pre-`2021-07-30` old code survives staging.

**Tech Stack:** Python, pandas, DuckDB, unittest, pytest

---

### Task 1: Lock The L1 Regression

**Files:**
- Modify: `tests/test_reference_data_staging.py`
- Test: `tests/test_reference_data_staging.py`

- [ ] **Step 1: Extend the official import regression fixture**

```python
pd.DataFrame(
    [
        {"股票代码": 3, "计入日期": "2018-06-19 16:10:00", "行业代码": "330104", "更新日期": "2026-04-27 09:00:00"},
        {"股票代码": 3, "计入日期": "2021-07-30", "行业代码": "330301", "更新日期": "2026-04-27 09:00:00"},
        {"股票代码": 4, "计入日期": "2018-06-19 16:10:00", "行业代码": "330104", "更新日期": "2026-04-27 09:00:00"},
        {"股票代码": 4, "计入日期": "2021-07-30", "行业代码": "630702", "更新日期": "2026-04-27 09:00:00"},
    ]
).to_excel(stock_file, index=False)
```

- [ ] **Step 2: Assert L1 history is preserved while L2/L3 stay conservative**

```python
self.assertEqual(summary["industry_rows"], 20)
self.assertEqual(summary["quarantined_unresolved_rows"]["L1"], 0)
self.assertEqual(summary["quarantined_unresolved_rows"]["L2"], 2)
self.assertEqual(summary["quarantined_unresolved_rows"]["L3"], 2)

self.assertIn(
    ("000003.SZ", "sw2021_l1", "330000", "2018-06-19 16:10:00", "2021-07-30 00:00:00"),
    rows,
)
self.assertIn(
    ("000004.SZ", "sw2021_l1", "330000", "2018-06-19 16:10:00", "2021-07-30 00:00:00"),
    rows,
)
```

- [ ] **Step 3: Run the targeted test and confirm it fails on current code**

Run: `PYTHONPATH=src pytest tests/test_reference_data_staging.py -k official_sw_industry_reference_db -v`

Expected: FAIL because current importer leaves `330104` unresolved at `L1` and only writes the `2021-07-30` rows.

### Task 2: Implement Conservative L1 Fallback Mapping

**Files:**
- Modify: `src/alpha_find_v2/reference_data_staging.py`
- Test: `tests/test_reference_data_staging.py`

- [ ] **Step 1: Read the official crosswalk sheet as raw rows**

```python
frame = pd.read_excel(path, sheet_name="新旧对比版本2", header=None)
if frame.shape[1] < 8:
    raise ValueError("Official SW crosswalk sheet 新旧对比版本2 requires at least 8 columns.")
```

- [ ] **Step 2: Track old L1 ancestry and build a conservative L1 mapping**

```python
current_old_l1_code = ""
for row in frame.iloc[:, :8].itertuples(index=False):
    old_code = _normalize_official_sw_code(row[3])
    new_code = _normalize_official_sw_code(row[7])
    if old_code.endswith("0000") and old_code:
        current_old_l1_code = old_code
        if new_code.endswith("0000") and new_code in hierarchy_by_code:
            level_maps["L1"][old_code] = hierarchy_by_code[new_code]["L1"]
    if old_code and current_old_l1_code:
        old_code_to_old_l1[old_code] = current_old_l1_code
```

- [ ] **Step 3: Keep existing exact new-code projection for `L2`/`L3`, but backfill missing `L1` rows from the old L1 mapping**

```python
for old_code, old_l1_code in old_code_to_old_l1.items():
    new_l1_code = old_l1_to_new_l1.get(old_l1_code)
    if new_l1_code:
        level_maps["L1"].setdefault(old_code, new_l1_code)
```

- [ ] **Step 4: Run the targeted test and confirm it passes**

Run: `PYTHONPATH=src pytest tests/test_reference_data_staging.py -k official_sw_industry_reference_db -v`

Expected: PASS with `L1` pre-cutover rows preserved for old codes like `330104`.

### Task 3: Verify Real Official Coverage

**Files:**
- Modify: none
- Test: `src/alpha_find_v2/reference_data_staging.py`, `src/alpha_find_v2/sw_industry_pit_audit.py`

- [ ] **Step 1: Materialize a temporary official-only PIT DB with the rebuilt importer**

Run: `PYTHONPATH=src python3 -m alpha_find_v2.cli import-official-sw-industry-pit --target-db /tmp/official_sw_l1_rebuild.duckdb --industry-level L1`

Expected: JSON summary with non-zero `industry_rows` and `official_shenwan_packet` output.

- [ ] **Step 2: Audit the rebuilt DB against the existing research source and cached provider intervals**

Run: `PYTHONPATH=src python3 -m alpha_find_v2.sw_industry_pit_audit --reference-db /tmp/official_sw_l1_rebuild.duckdb --research-db output/research_source.duckdb --benchmark-id "CSI 800" --industry-schema sw2021_l1 --industry-level L1 --start-date 20140221 --end-date 20260423 --output-json /tmp/official_sw_l1_rebuild_audit.json --output-gap-csv /tmp/official_sw_l1_rebuild_gap_rows.csv`

Expected: `staging_sync_gap` collapses toward the direct official packet baseline and the remaining residual set is tiny rather than hundreds of left-edge omissions.

- [ ] **Step 3: Run the full reference staging test file**

Run: `PYTHONPATH=src pytest tests/test_reference_data_staging.py -v`

Expected: PASS
