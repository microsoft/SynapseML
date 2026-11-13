"""Run the generated CU live suite against CI's shared Foundry resource."""

import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
    for name in (
        "CONTENT_UNDERSTANDING_ENDPOINT",
        "CONTENT_UNDERSTANDING_API_KEY",
    ):
        value = os.environ.get(name, "").strip()
        if not value or value.startswith("$("):
            print(f"{name} must be configured for CU live validation.", file=sys.stderr)
            return 2

    root = Path(__file__).resolve().parents[2]
    tests = (
        root / "cognitive" / "target" / "scala-2.12" / "generated" / "test" / "python"
    )
    report = root / "target" / "python-test-content-understanding-live.xml"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "synapsemltest/services/test_ContentUnderstandingE2E.py",
            f"--junitxml={report}",
            "--tb=short",
        ],
        cwd=tests,
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
