# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

"""Run the offline release-agent rehearsal; never accepts production inputs."""

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


def test_counts(path):
    cases = ET.parse(path).getroot().findall(".//testcase")
    failed = sum(
        case.find("failure") is not None or case.find("error") is not None
        for case in cases
    )
    skipped = sum(case.find("skipped") is not None for case in cases)
    return {
        "total": len(cases),
        "passed": len(cases) - failed - skipped,
        "failed": failed,
        "skipped": skipped,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        help="Create a new local JSON report; existing files are never overwritten",
    )
    args = parser.parse_args(argv)
    report = {
        "mode": "offline-rehearsal",
        "status": "error",
        "live_services_validated": False,
        "publication_authorized": False,
        "tests": None,
    }
    code = 2
    try:
        with ExitStack() as stack:
            output = (
                stack.enter_context(args.report.open("x", encoding="utf-8"))
                if args.report is not None
                else None
            )
            directory = Path(
                stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="synapseml-release-rehearsal-")
                )
            )
            junit = directory / "results.xml"
            tests = Path(__file__).resolve().with_name("test_release_rehearsal.py")
            environment = {
                key: value
                for key, value in os.environ.items()
                if key not in {"PYTEST_ADDOPTS", "PYTEST_PLUGINS"}
            }
            environment.update(
                PYTHONDONTWRITEBYTECODE="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1"
            )
            result = None
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        str(tests),
                        "-q",
                        "--tb=short",
                        "-p",
                        "no:cacheprovider",
                        "-o",
                        "addopts=",
                        f"--junitxml={junit}",
                    ],
                    cwd=tests.parents[2],
                    env=environment,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=300,
                    check=False,
                )
                report["tests"] = counts = test_counts(junit)
                passed = (
                    result.returncode == 0
                    and counts["total"] > 0
                    and counts["failed"] == 0
                    and counts["skipped"] == 0
                )
                report["status"] = "passed" if passed else "failed"
                code = 0 if passed else 1
                if not passed:
                    print(
                        "Offline rehearsal failed or did not execute every case.\n"
                        + result.stdout
                        + result.stderr,
                        file=sys.stderr,
                    )
            except (OSError, ET.ParseError, subprocess.TimeoutExpired) as error:
                print(
                    f"error: offline rehearsal could not complete: {error}",
                    file=sys.stderr,
                )
                if result is not None:
                    print(result.stdout + result.stderr, file=sys.stderr)
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
            if output is not None:
                output.write(encoded)
                output.flush()
            print(encoded, end="")
            return code
    except OSError as error:
        print(
            f"error: cannot create or retain rehearsal report: {error}", file=sys.stderr
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
