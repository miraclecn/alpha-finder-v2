import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from alpha_find_v2.research_artifact_builder import build_sleeve_artifact, write_sleeve_artifact
from alpha_find_v2.research_artifact_loader import (
    load_sleeve_artifact,
    load_sleeve_artifact_build_case,
)


class SleeveArtifactBuilderTest(unittest.TestCase):
    def test_build_case_emits_artifact_with_residual_return_and_trade_constraints(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            input_path = temp_root / "research_input.json"
            output_path = temp_root / "artifact.json"
            case_path = temp_root / "build_case.toml"

            input_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "artifact_type": "sleeve_research_observation_input",
                        "steps": [
                            {
                                "trade_date": "2026-04-06",
                                "factor_returns": {
                                    "benchmark": 0.0100,
                                    "industry:bank": 0.0050,
                                    "industry:tech": 0.0040,
                                    "size": 0.0040,
                                    "beta": 0.0025,
                                },
                                "records": [
                                    {
                                        "asset_id": "AAA",
                                        "rank": 1,
                                        "score": 9.0,
                                        "target_weight": 0.60,
                                        "entry_open": 10.0,
                                        "exit_open": 10.5,
                                        "industry": "bank",
                                        "exposures": {
                                            "benchmark": 1.0,
                                            "industry:bank": 1.0,
                                            "size": -0.5,
                                            "beta": 1.2,
                                        },
                                    },
                                    {
                                        "asset_id": "BBB",
                                        "rank": 2,
                                        "score": 8.0,
                                        "target_weight": 0.40,
                                        "entry_open": 10.0,
                                        "exit_open": 10.2,
                                        "industry": "tech",
                                        "entry_state": {
                                            "limit_locked": True,
                                        },
                                        "exit_state": {
                                            "suspended": True,
                                        },
                                        "exposures": {
                                            "benchmark": 1.0,
                                            "industry:tech": 1.0,
                                            "size": 0.0,
                                            "beta": 1.0,
                                        },
                                    },
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "sleeve_artifact_build_case"',
                        'case_id = "trend_leadership_build_case"',
                        'description = "Build a sleeve artifact from normalized research observations."',
                        'sleeve_path = "config/sleeves/fundamental_rerating_core.toml"',
                        f'input_path = "{input_path}"',
                        f'output_path = "{output_path}"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_sleeve_artifact_build_case(case_path)
            artifact = build_sleeve_artifact(loaded_case)
            write_sleeve_artifact(artifact, loaded_case.definition.output_path)
            roundtrip = load_sleeve_artifact(output_path)

            self.assertEqual(roundtrip.sleeve_id, "fundamental_rerating_core")
            self.assertEqual(roundtrip.target_id, "open_t1_to_open_t20_residual_net_cost")
            self.assertEqual(roundtrip.trade_dates(), ["2026-04-06"])
            self.assertAlmostEqual(roundtrip.steps[0].records[0].realized_return, 0.0316)
            self.assertTrue(roundtrip.steps[0].records[0].trade_state.can_enter)
            self.assertTrue(roundtrip.steps[0].records[0].trade_state.can_exit)
            self.assertFalse(roundtrip.steps[0].records[1].trade_state.can_enter)
            self.assertFalse(roundtrip.steps[0].records[1].trade_state.can_exit)

    def test_build_case_emits_artifact_with_net_return_for_non_residual_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            input_path = temp_root / "research_input.json"
            output_path = temp_root / "artifact.json"
            case_path = temp_root / "build_case.toml"

            input_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "artifact_type": "sleeve_research_observation_input",
                        "steps": [
                            {
                                "trade_date": "2026-04-06",
                                "records": [
                                    {
                                        "asset_id": "BBB",
                                        "rank": 1,
                                        "score": 9.2,
                                        "target_weight": 0.55,
                                        "entry_open": 10.0,
                                        "exit_open": 10.224,
                                        "industry": "tech",
                                    },
                                    {
                                        "asset_id": "DDD",
                                        "rank": 2,
                                        "score": 8.4,
                                        "target_weight": 0.45,
                                        "entry_open": 10.0,
                                        "exit_open": 10.324,
                                        "industry": "industrial",
                                    },
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "sleeve_artifact_build_case"',
                        'case_id = "trend_leadership_build_case"',
                        'description = "Build a sleeve artifact from normalized research observations."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'input_path = "{input_path}"',
                        f'output_path = "{output_path}"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_sleeve_artifact_build_case(case_path)
            artifact = build_sleeve_artifact(loaded_case)
            write_sleeve_artifact(artifact, loaded_case.definition.output_path)
            roundtrip = load_sleeve_artifact(output_path)

            self.assertEqual(roundtrip.sleeve_id, "trend_leadership_core")
            self.assertEqual(roundtrip.target_id, "open_t1_to_open_t20_net_cost")
            self.assertAlmostEqual(roundtrip.steps[0].records[0].realized_return, 0.0200)
            self.assertAlmostEqual(roundtrip.steps[0].records[1].realized_return, 0.0300)
            self.assertTrue(roundtrip.steps[0].records[0].trade_state.can_enter)
            self.assertTrue(roundtrip.steps[0].records[0].trade_state.can_exit)

    def test_cli_build_sleeve_artifact_writes_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            input_path = temp_root / "research_input.json"
            output_path = temp_root / "artifact.json"
            case_path = temp_root / "build_case.toml"

            input_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "artifact_type": "sleeve_research_observation_input",
                        "steps": [
                            {
                                "trade_date": "2026-04-06",
                                "factor_returns": {
                                    "benchmark": 0.0100,
                                    "industry:bank": 0.0050,
                                    "size": 0.0040,
                                    "beta": 0.0025,
                                },
                                "records": [
                                    {
                                        "asset_id": "AAA",
                                        "rank": 1,
                                        "score": 9.0,
                                        "target_weight": 1.0,
                                        "entry_open": 10.0,
                                        "exit_open": 10.5,
                                        "industry": "bank",
                                        "exposures": {
                                            "benchmark": 1.0,
                                            "industry:bank": 1.0,
                                            "size": -0.5,
                                            "beta": 1.2,
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "sleeve_artifact_build_case"',
                        'case_id = "trend_leadership_build_case"',
                        'description = "Build a sleeve artifact from normalized research observations."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'input_path = "{input_path}"',
                        f'output_path = "{output_path}"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-sleeve-artifact",
                    "--case",
                    str(case_path),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["artifact_type"], "sleeve_research_artifact")
            self.assertEqual(payload["sleeve_id"], "trend_leadership_core")


if __name__ == "__main__":
    unittest.main()
