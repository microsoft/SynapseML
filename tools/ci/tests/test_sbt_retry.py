"""Deterministic tests for tools/ci/sbt_retry.sh.

These exercise the stagger/backoff/retry logic with a fake ``sbt`` executable so
the wrapper's behaviour is verified without contacting Maven Central and without
real sleeps. Run with: ``python -m pytest tools/ci/tests/test_sbt_retry.py``.
"""
import os
import stat
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "tools" / "ci" / "sbt_retry.sh"


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IRWXU)


def _fake_sbt(tmp_path: Path, fail_count: int) -> Path:
    """A fake ``sbt`` that fails its first ``fail_count`` invocations then passes.

    Records the invocation count and the args of each call to files in tmp_path.
    """
    calls = tmp_path / "sbt_calls"
    args_log = tmp_path / "sbt_args"
    fake = tmp_path / "fake_sbt.sh"
    _write_exec(
        fake,
        f"""#!/usr/bin/env bash
n=0
if [ -f "{calls}" ]; then n=$(cat "{calls}"); fi
n=$((n + 1))
echo "$n" > "{calls}"
echo "$@" >> "{args_log}"
if [ "$n" -le "{fail_count}" ]; then
  echo "fake sbt: simulated failure #$n" >&2
  exit 1
fi
echo "fake sbt: success on call #$n"
exit 0
""",
    )
    return fake


def _fake_sleep(tmp_path: Path) -> Path:
    """A fake ``sleep`` that records each requested duration instead of waiting."""
    log = tmp_path / "sleep_log"
    fake = tmp_path / "fake_sleep.sh"
    _write_exec(
        fake,
        f"""#!/usr/bin/env bash
echo "$1" >> "{log}"
exit 0
""",
    )
    return fake


def _run(tmp_path, *args, env_overrides=None, sbt_cmd=None, sleep_cmd=None):
    env = dict(os.environ)
    # Deterministic + fast defaults: no real stagger/backoff, no timeout binary.
    env.update(
        {
            "SBT_SETUP_MAX_STAGGER_SECONDS": "0",
            "SBT_SETUP_BASE_BACKOFF_SECONDS": "0",
            "SBT_SETUP_TIMEOUT": "",
            "SBT_SETUP_RANDOM": "0",
            "SBT_SETUP_MAX_ATTEMPTS": "5",
        }
    )
    if sbt_cmd:
        env["SBT_SETUP_SBT_CMD"] = str(sbt_cmd)
    if sleep_cmd:
        env["SBT_SETUP_SLEEP_CMD"] = str(sleep_cmd)
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        env=env,
        capture_output=True,
        text=True,
    )


def test_script_exists_and_is_executable():
    assert SCRIPT.exists(), f"missing {SCRIPT}"
    assert os.access(SCRIPT, os.X_OK), "sbt_retry.sh must be executable"


def test_succeeds_first_attempt(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=0)
    r = _run(tmp_path, "setup", sbt_cmd=sbt)
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "sbt_calls").read_text().strip() == "1"


def test_retries_then_succeeds(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=2)
    r = _run(tmp_path, "setup", sbt_cmd=sbt)
    assert r.returncode == 0, r.stderr
    # Two failures + one success = three invocations.
    assert (tmp_path / "sbt_calls").read_text().strip() == "3"


def test_exhausts_and_fails_visibly(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=99)  # always fails
    r = _run(
        tmp_path, "setup", sbt_cmd=sbt, env_overrides={"SBT_SETUP_MAX_ATTEMPTS": "4"}
    )
    assert r.returncode != 0, "must fail (no success fallback masking)"
    assert (tmp_path / "sbt_calls").read_text().strip() == "4"
    assert "exhausted 4 attempts" in (r.stdout + r.stderr)


def test_forwards_all_args_to_sbt(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=0)
    r = _run(
        tmp_path,
        "-J--add-opens=java.prefs/java.util.prefs=ALL-UNNAMED",
        "setup",
        sbt_cmd=sbt,
    )
    assert r.returncode == 0, r.stderr
    logged = (tmp_path / "sbt_args").read_text().strip()
    assert logged == "-J--add-opens=java.prefs/java.util.prefs=ALL-UNNAMED setup"


def test_no_args_is_error(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=0)
    r = _run(tmp_path, sbt_cmd=sbt)
    assert r.returncode == 2


def test_exponential_backoff_schedule(tmp_path):
    """With base=1s and jitter=0, delays must follow 1,2,4 (capped) between 4 attempts."""
    sbt = _fake_sbt(tmp_path, fail_count=99)
    sleep = _fake_sleep(tmp_path)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        sleep_cmd=sleep,
        env_overrides={
            "SBT_SETUP_MAX_ATTEMPTS": "4",
            "SBT_SETUP_BASE_BACKOFF_SECONDS": "1",
            "SBT_SETUP_MAX_BACKOFF_SECONDS": "100",
            "SBT_SETUP_MAX_STAGGER_SECONDS": "0",
            "SBT_SETUP_RANDOM": "0",
        },
    )
    assert r.returncode != 0
    delays = (tmp_path / "sleep_log").read_text().split()
    # 3 backoff waits between the 4 attempts (no wait after the final failure).
    assert delays == ["1", "2", "4"], delays


def test_backoff_is_capped(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=99)
    sleep = _fake_sleep(tmp_path)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        sleep_cmd=sleep,
        env_overrides={
            "SBT_SETUP_MAX_ATTEMPTS": "5",
            "SBT_SETUP_BASE_BACKOFF_SECONDS": "10",
            "SBT_SETUP_MAX_BACKOFF_SECONDS": "25",
            "SBT_SETUP_MAX_STAGGER_SECONDS": "0",
            "SBT_SETUP_RANDOM": "0",
        },
    )
    assert r.returncode != 0
    delays = [int(x) for x in (tmp_path / "sleep_log").read_text().split()]
    # base*2^(n-1) = 10,20,40,80 -> capped at 25 -> 10,20,25,25
    assert delays == [10, 20, 25, 25], delays


def test_stagger_uses_sleep_when_enabled(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=0)
    sleep = _fake_sleep(tmp_path)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        sleep_cmd=sleep,
        env_overrides={"SBT_SETUP_MAX_STAGGER_SECONDS": "30", "SBT_SETUP_RANDOM": "7"},
    )
    assert r.returncode == 0, r.stderr
    # rand_below(31) with RANDOM=7 -> 7 % 31 = 7
    delays = (tmp_path / "sleep_log").read_text().split()
    assert delays == ["7"], delays
