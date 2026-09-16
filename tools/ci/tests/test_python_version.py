# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "tools" / "ci" / "get_python_version.sh"


def _bash_path(path: Path) -> str:
    if path.drive == "":
        return str(path)
    drive = path.drive.rstrip(":").lower()
    return f"/mnt/{drive}/{path.as_posix().split(':', 1)[1].lstrip('/')}"


def _run(environment_file: Path | None = None):
    args = [] if environment_file is None else [_bash_path(environment_file)]
    return subprocess.run(
        ["bash", _bash_path(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_script_avoids_bash_four_only_mapfile():
    assert "mapfile" not in SCRIPT.read_text()


def test_extracts_pinned_python_version(tmp_path):
    environment_file = tmp_path / "environment.yml"
    environment_file.write_text(
        "dependencies:\n  - python=3.11.8\n  - requests=2.32.5\n"
    )

    result = _run(environment_file)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "3.11.8\n"


@pytest.mark.parametrize("version", ["3", "3.13.*", "3.13.0.1", "3.13rc1", ">=3.13"])
def test_rejects_unpinned_or_malformed_python_version(tmp_path, version):
    environment_file = tmp_path / "environment.yml"
    environment_file.write_text(f"dependencies:\n  - python={version}\n")

    result = _run(environment_file)

    assert result.returncode != 0
    assert "exactly one pinned" in result.stderr


def test_extracts_minor_series_python_version(tmp_path):
    environment_file = tmp_path / "environment.yml"
    environment_file.write_text("dependencies:\n  - python=3.13\n")

    result = _run(environment_file)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "3.13\n"


def test_extracts_repository_python_version_for_docker():
    result = _run()

    assert result.returncode == 0, result.stderr
    assert result.stdout == "3.13\n"


def test_rejects_multiple_python_versions(tmp_path):
    environment_file = tmp_path / "environment.yml"
    environment_file.write_text("dependencies:\n  - python=3.11.8\n  - python=3.12.4\n")

    result = _run(environment_file)

    assert result.returncode != 0
    assert "exactly one pinned" in result.stderr
