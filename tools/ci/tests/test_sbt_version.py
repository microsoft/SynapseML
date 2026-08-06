# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import shlex
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "tools" / "ci" / "get_sbt_version.sh"


def _bash_path(path: Path) -> str:
    if path.drive == "":
        return str(path)
    drive = path.drive.rstrip(":").lower()
    return f"/mnt/{drive}/{path.as_posix().split(':', 1)[1].lstrip('/')}"


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body, newline="\n")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IRWXU)


def test_extracts_version_from_sbt_output(tmp_path):
    fake_sbt = tmp_path / "fake_sbt.sh"
    _write_exec(
        fake_sbt,
        """#!/usr/bin/env bash
echo '[info] loading project'
printf '\\033[32m[info] 1.2.3-SNAPSHOT\\033[0m\\n'
""",
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            "SBT_VERSION_SBT_CMD={} {}".format(
                shlex.quote(_bash_path(fake_sbt)),
                shlex.quote(_bash_path(SCRIPT)),
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "1.2.3-SNAPSHOT\n"


def test_rejects_missing_version(tmp_path):
    fake_sbt = tmp_path / "fake_sbt.sh"
    _write_exec(fake_sbt, "#!/usr/bin/env bash\necho '[info]'\n")

    result = subprocess.run(
        [
            "bash",
            "-c",
            "SBT_VERSION_SBT_CMD={} {}".format(
                shlex.quote(_bash_path(fake_sbt)),
                shlex.quote(_bash_path(SCRIPT)),
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Unable to resolve" in result.stderr
