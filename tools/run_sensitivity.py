"""Run five paired protocol-mutant sensitivity experiments and save the evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from raftlight.sensitivity import run_sensitivity


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "results/sensitivity_summary.json")
    args = parser.parse_args()
    summary = run_sensitivity()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(json.dumps(dict(output=str(args.output), mutants_total=summary["mutants_total"],
                          detected_total=summary["detected_total"],
                          correct_controls_passed=summary["correct_controls_passed"]), indent=2))
    return int(summary["detected_total"] != 5 or summary["correct_controls_passed"] != 5)


if __name__ == "__main__":
    raise SystemExit(main())
