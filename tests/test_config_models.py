"""Tests for config_models.py — data_sources.toml parsing."""

import tempfile
from pathlib import Path

import pytest

import alpha_find_v2.data_ingest.templates as _tpkg
from alpha_find_v2.data_ingest.config_models import (
    DataSourcesConfig,
    load_data_sources_config,
)

_TEMPLATE_PATH = Path(_tpkg.__file__).parent / "data_sources.toml.template"

EXPECTED_DATASETS = {
    "stock_basic", "trade_cal", "namechange",
    "daily", "daily_basic",
    "adj_factor", "daily_qfq",
    "suspend_d", "stk_limit",
    "index_daily", "index_weight", "index_member_all",
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
}
FIVE_K_DATASETS = {
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express"
}


def test_template_loads_18_datasets_and_3_adapters():
    config = load_data_sources_config(_TEMPLATE_PATH)
    assert set(config.datasets.keys()) == EXPECTED_DATASETS, (
        f"Expected 18 datasets; got {sorted(config.datasets.keys())}"
    )
    assert set(config.adapters.keys()) == {"tushare", "akshare", "baostock"}


def test_5000_credit_datasets_default_disabled():
    config = load_data_sources_config(_TEMPLATE_PATH)
    for ds_id in FIVE_K_DATASETS:
        assert not config.datasets[ds_id].enabled, (
            f"{ds_id} should default to enabled=false"
        )


def test_priority_returns_all_enabled_adapters_in_order():
    """priority('daily') returns all 3 when all adapters are enabled."""
    toml = """
schema_version = 1

[adapter.tushare]
enabled = true
calls_per_minute = 490

[adapter.akshare]
enabled = true
calls_per_minute = 60

[adapter.baostock]
enabled = true
calls_per_minute = 60

[datasets.daily]
enabled = true
credit_tier = 120
priority = ["tushare", "akshare", "baostock"]
"""
    with tempfile.NamedTemporaryFile(suffix=".toml", mode="w", delete=False,
                                     encoding="utf-8") as f:
        f.write(toml)
        path = Path(f.name)

    config = load_data_sources_config(path)
    assert config.priority("daily") == ("tushare", "akshare", "baostock")


def test_malformed_config_unknown_adapter_in_priority_raises():
    toml = """
schema_version = 1

[adapter.tushare]
enabled = true
calls_per_minute = 490

[datasets.daily]
enabled = true
credit_tier = 120
priority = ["tushare", "nonexistent_adapter"]
"""
    with tempfile.NamedTemporaryFile(suffix=".toml", mode="w", delete=False,
                                     encoding="utf-8") as f:
        f.write(toml)
        path = Path(f.name)

    with pytest.raises(ValueError, match="nonexistent_adapter"):
        load_data_sources_config(path)


def test_schema_version_not_1_raises():
    with tempfile.NamedTemporaryFile(suffix=".toml", mode="w", delete=False,
                                     encoding="utf-8") as f:
        f.write("schema_version = 2\n")
        path = Path(f.name)

    with pytest.raises(ValueError, match="schema_version"):
        load_data_sources_config(path)


def test_invalid_credit_tier_raises():
    toml = """
schema_version = 1

[adapter.tushare]
enabled = true
calls_per_minute = 490

[datasets.daily]
enabled = true
credit_tier = 999
priority = ["tushare"]
"""
    with tempfile.NamedTemporaryFile(suffix=".toml", mode="w", delete=False,
                                     encoding="utf-8") as f:
        f.write(toml)
        path = Path(f.name)

    with pytest.raises(ValueError, match="credit_tier"):
        load_data_sources_config(path)
