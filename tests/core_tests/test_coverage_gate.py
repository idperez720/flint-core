"""Fractional release thresholds must compare the measured percentage precisely."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_fractional_coverage_gate_accepts_above_baseline_and_rejects_below(tmp_path):
    # 25 covered statements out of 28 = 89.2857%; integer rounding loses the gate.
    module = tmp_path / "fractional.py"
    module.write_text("value = 0\n" * 24 + "if value < 0:\n" + "    value += 1\n" * 3, encoding="utf-8")
    common = [sys.executable, "-m", "coverage"]
    options = ["--rcfile", str(ROOT / "pyproject.toml"), "--data-file", str(tmp_path / "coverage-data")]
    measured = subprocess.run([*common, "run", *options, str(module)], capture_output=True, text=True, check=False)
    assert measured.returncode == 0, measured.stderr
    accepted = subprocess.run(
        [*common, "report", *options, "--fail-under=89.16"], capture_output=True, text=True, check=False
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert "89.29%" in accepted.stdout
    rejected = subprocess.run(
        [*common, "report", *options, "--fail-under=89.30"], capture_output=True, text=True, check=False
    )
    assert rejected.returncode == 2, rejected.stdout + rejected.stderr
