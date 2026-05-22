"""Isolation enforcement tests for factor_lab.

Verifies:
- R8.1: No import or call of descriptor_compute.register in factor_lab code.
- R8.1: No direct REGISTRY mutations outside walk_forward.py.
- R8.5: assert_output_root_safe exits 6 for non-output/factor_lab paths.
- R9.4: _temporary_registration cleans up the REGISTRY even when an exception occurs.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest


FACTOR_LAB_DIR = Path("src/alpha_find_v2/factor_lab")


# ---------------------------------------------------------------------------
# R8.1: AST scan — no import or call of descriptor_compute.register
# ---------------------------------------------------------------------------


def test_no_register_import_in_factor_lab():
    """R8.1: factor_lab must never import or call descriptor_compute.register."""
    violations = []
    for py_file in FACTOR_LAB_DIR.rglob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))
        for node in ast.walk(tree):
            # Check for: from descriptor_compute import register
            if isinstance(node, ast.ImportFrom):
                if "descriptor_compute" in (node.module or ""):
                    for alias in node.names:
                        if alias.name == "register":
                            violations.append(
                                (str(py_file), node.lineno, "ImportFrom register")
                            )
            # Check for: descriptor_compute.register(...)
            if isinstance(node, ast.Call):
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "register"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "descriptor_compute"
                ):
                    violations.append(
                        (str(py_file), node.lineno, "Call descriptor_compute.register")
                    )

    assert not violations, f"R8.1 violated: {violations}"


# ---------------------------------------------------------------------------
# R8.1: AST scan — no direct REGISTRY[...] = ... outside walk_forward.py
# ---------------------------------------------------------------------------


def test_no_direct_registry_mutation_in_factor_lab():
    """R8.1: REGISTRY must only be mutated inside walk_forward.py (via _temporary_registration)."""
    violations = []
    for py_file in FACTOR_LAB_DIR.rglob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Subscript)
                and isinstance(node.value, ast.Name)
                and node.value.id == "REGISTRY"
                and isinstance(node.ctx, ast.Store)
            ):
                if "walk_forward" not in str(py_file):
                    violations.append(
                        (str(py_file), node.lineno, "REGISTRY direct mutation")
                    )

    assert not violations, f"R8.1 violated: {violations}"


# ---------------------------------------------------------------------------
# R8.5: assert_output_root_safe exits 6 for non-output/factor_lab paths
# (integration reference — primary coverage is in tests/test_factor_lab_isolation.py)
# ---------------------------------------------------------------------------


def test_assert_output_root_safe_exits_6(tmp_path):
    """R8.5: assert_output_root_safe must exit with code 6 for a path outside output/factor_lab."""
    from alpha_find_v2.factor_lab.isolation import assert_output_root_safe

    with pytest.raises(SystemExit) as exc:
        assert_output_root_safe(tmp_path)
    assert exc.value.code == 6


# ---------------------------------------------------------------------------
# R9.4: _temporary_registration cleans up even on exception
# ---------------------------------------------------------------------------


def test_temporary_registration_cleans_up_on_exception():
    """R9.4: _temporary_registration must remove spec from REGISTRY even if exception occurs."""
    from alpha_find_v2.factor_evaluation.descriptor_compute import REGISTRY
    from alpha_find_v2.factor_lab.dsl.grammar import Leaf
    from alpha_find_v2.factor_lab.walk_forward import _make_adhoc_spec, _temporary_registration

    ast_node = Leaf(field="close_adj")
    spec = _make_adhoc_spec(ast_node)
    spec_id = spec.descriptor_id

    assert spec_id not in REGISTRY, "spec should not be registered before test"

    try:
        with _temporary_registration(spec):
            assert spec_id in REGISTRY, "spec should be registered inside context"
            raise RuntimeError("simulated pipeline failure")
    except RuntimeError:
        pass

    assert spec_id not in REGISTRY, (
        "R9.4: spec must be removed from REGISTRY after exception"
    )
