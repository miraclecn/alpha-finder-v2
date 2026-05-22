"""Verification tests for config_models.py and the packaged templates.

Covers:
- Load the packaged template; assert 18 datasets, 3 adapters
- Malformed config (unknown adapter in priority) raises ValueError with helpful message
- 5000-credit datasets default enabled=false in template
- priority() returns only enabled adapters
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import alpha_find_v2.data_ingest.templates as _tpkg
from alpha_find_v2.data_ingest.config_models import load_data_sources_config

_TEMPLATE_PATH = Path(_tpkg.__file__).parent / "data_sources.toml.template"
_ENV_TEMPLATE_PATH = Path(_tpkg.__file__).parent / ".env.template"

EXPECTED_DATASETS = {
    "stock_basic", "trade_cal", "namechange",
    "daily", "daily_basic",
    "adj_factor", "daily_qfq",
    "suspend_d", "stk_limit",
    "index_daily", "index_weight", "index_member_all",
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
}
FIVE_K_DATASETS = {
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
}


def test_template_loads_18_datasets_and_3_adapters():
    """Packaged template contains exactly 18 datasets and 3 adapters."""
    config = load_data_sources_config(_TEMPLATE_PATH)
    assert set(config.datasets.keys()) == EXPECTED_DATASETS, (
        f"Expected 18 datasets; got {sorted(config.datasets.keys())}"
    )
    assert set(config.adapters.keys()) == {"tushare", "akshare", "baostock"}, (
        f"Expected 3 adapters; got {sorted(config.adapters.keys())}"
    )


def test_malformed_config_unknown_adapter_raises_with_helpful_message():
    """Unknown adapter in priority raises ValueError naming the bad adapter."""
    toml = """\
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


def test_5000_credit_datasets_default_disabled():
    """Every 5000-credit dataset defaults to enabled=false in the template."""
    config = load_data_sources_config(_TEMPLATE_PATH)
    for ds_id in FIVE_K_DATASETS:
        assert not config.datasets[ds_id].enabled, (
            f"Dataset '{ds_id}' (5000-credit) should default to enabled=false"
        )


def test_priority_returns_only_enabled_adapters():
    """priority() omits adapters marked enabled=false."""
    toml = """\
schema_version = 1

[adapter.tushare]
enabled = true
calls_per_minute = 490

[adapter.akshare]
enabled = true
calls_per_minute = 60

[adapter.baostock]
enabled = false
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
    result = config.priority("daily")
    assert result == ("tushare", "akshare"), (
        f"Expected ('tushare', 'akshare'); got {result}"
    )
    assert "baostock" not in result


def test_env_template_exists():
    """.env.template is present in the templates package directory."""
    assert _ENV_TEMPLATE_PATH.exists(), (
        f".env.template not found at {_ENV_TEMPLATE_PATH}"
    )


def test_env_template_contains_tushare_token_placeholder():
    """The .env.template has a TUSHARE_TOKEN= line."""
    content = _ENV_TEMPLATE_PATH.read_text(encoding="utf-8")
    assert "TUSHARE_TOKEN=" in content
