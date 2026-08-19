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


def _fake_sbt_unresolved(
    tmp_path: Path,
    fail_count: int,
    coordinate: str = "com.globalmentor#hadoop-bare-naked-local-fs;0.1.0",
    error_prefix: str = "",
) -> Path:
    """A fake ``sbt`` whose first ``fail_count`` calls emit an Ivy resolve error."""
    calls = tmp_path / "sbt_calls"
    fake = tmp_path / "fake_sbt_unresolved.sh"
    _write_exec(
        fake,
        f"""#!/usr/bin/env bash
n=0
if [ -f "{calls}" ]; then n=$(cat "{calls}"); fi
n=$((n + 1))
echo "$n" > "{calls}"
if [ "$n" -le "{fail_count}" ]; then
  echo "[warn] 	module not found: {coordinate}"
  echo "[error] {error_prefix}sbt.librarymanagement.ResolveException: unresolved dependency:\
 {coordinate}: not found" >&2
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


def _fake_curl(tmp_path: Path, http_code: str = "200") -> Path:
    """A fake ``curl`` that records requested URLs and returns a fixed status."""
    log = tmp_path / "curl_log"
    args_log = tmp_path / "curl_args"
    fake = tmp_path / f"fake_curl_{http_code}.sh"
    _write_exec(
        fake,
        f"""#!/usr/bin/env bash
echo "$@" >> "{args_log}"
for a in "$@"; do
  case "$a" in
    https://*) echo "$a" >> "{log}" ;;
  esac
done
printf '%s' "{http_code}"
exit 0
""",
    )
    return fake


def _run(tmp_path, *args, env_overrides=None, sbt_cmd=None, sleep_cmd=None):
    env = dict(os.environ)
    # Deterministic + fast defaults: no real stagger/backoff, no timeout binary,
    # and no real network probe.
    env.update(
        {
            "SBT_SETUP_MAX_STAGGER_SECONDS": "0",
            "SBT_SETUP_BASE_BACKOFF_SECONDS": "0",
            "SBT_SETUP_TIMEOUT": "",
            "SBT_SETUP_RANDOM": "0",
            "SBT_SETUP_MAX_ATTEMPTS": "5",
            "SBT_SETUP_CURL_CMD": str(_fake_curl(tmp_path)),
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


def _ivy_layout(tmp_path):
    """Build an Ivy/Coursier cache holding one incomplete module plus a neighbour."""
    ivy = tmp_path / "ivy2"
    coursier = tmp_path / "coursier"
    incomplete = ivy / "cache" / "com.globalmentor" / "hadoop-bare-naked-local-fs"
    incomplete.mkdir(parents=True)
    (incomplete / "ivydata-0.1.0.properties").write_text("partially restored")
    coursier_entries = []
    for host in ("repo1.maven.org", "repo.maven.apache.org"):
        entry = (
            coursier
            / "v1"
            / "https"
            / host
            / "maven2"
            / "com"
            / "globalmentor"
            / "hadoop-bare-naked-local-fs"
            / "0.1.0"
        )
        entry.mkdir(parents=True)
        coursier_entries.append(entry)
    neighbour = ivy / "cache" / "org.apache.spark" / "spark-core"
    neighbour.mkdir(parents=True)
    env = {
        "SBT_SETUP_IVY_HOME": str(ivy),
        "SBT_SETUP_COURSIER_CACHE": str(coursier),
    }
    return env, incomplete, coursier_entries, neighbour


def test_sbt_output_stays_visible(tmp_path):
    """tee must not swallow sbt output (it is the only CI diagnostic)."""
    sbt = _fake_sbt(tmp_path, fail_count=0)
    r = _run(tmp_path, "setup", sbt_cmd=sbt)
    assert r.returncode == 0, r.stderr
    assert "fake sbt: success on call #1" in r.stdout


def test_exit_code_survives_the_tee_pipeline(tmp_path):
    """PIPESTATUS[0], not tee's status, must decide success."""
    sbt = _fake_sbt(tmp_path, fail_count=99)
    r = _run(
        tmp_path, "setup", sbt_cmd=sbt, env_overrides={"SBT_SETUP_MAX_ATTEMPTS": "1"}
    )
    assert r.returncode == 1, (r.returncode, r.stdout, r.stderr)


def test_evicts_incomplete_module_then_succeeds(tmp_path):
    env, incomplete, coursier_entries, neighbour = _ivy_layout(tmp_path)
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert not incomplete.exists(), "incomplete Ivy module must be evicted"
    assert all(
        not entry.exists() for entry in coursier_entries
    ), "matching Coursier entries must be evicted"
    assert neighbour.exists(), "unrelated modules must be left alone"
    assert "evicting cache entry for unresolved module" in (r.stdout + r.stderr)


def test_no_eviction_when_failure_is_not_a_resolution_error(tmp_path):
    env, incomplete, coursier_entries, neighbour = _ivy_layout(tmp_path)
    sbt = _fake_sbt(tmp_path, fail_count=1)  # generic failure, no resolve error
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert incomplete.exists(), "unrelated failures must not evict caches"
    assert all(entry.exists() for entry in coursier_entries)
    assert neighbour.exists()
    assert "evicting cache entry for unresolved module" not in (r.stdout + r.stderr)


def test_eviction_is_tolerant_of_absent_cache_dirs(tmp_path):
    """A resolve error with nothing cached locally must still retry cleanly."""
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        env_overrides={
            "SBT_SETUP_IVY_HOME": str(tmp_path / "missing-ivy"),
            "SBT_SETUP_COURSIER_CACHE": str(tmp_path / "missing-coursier"),
        },
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "sbt_calls").read_text().strip() == "2"


def test_home_unset_disables_default_cache_eviction(tmp_path):
    """Missing HOME must never turn the defaults into root-level cache paths."""
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        env_overrides={
            "HOME": "",
            "SBT_SETUP_IVY_HOME": "",
            "SBT_SETUP_COURSIER_CACHE": "",
        },
    )
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout + r.stderr
    assert "Ivy eviction disabled: HOME is unset" in out
    assert "Coursier eviction disabled: HOME is unset" in out
    assert "evicting cache entry for unresolved module: /.ivy2" not in out
    assert "evicting cache entry for unresolved module: /.cache" not in out


def test_unsafe_cache_roots_are_skipped(tmp_path):
    """Cache roots must be absolute, non-root paths without traversal segments."""
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        env_overrides={
            "SBT_SETUP_IVY_HOME": "/",
            "SBT_SETUP_COURSIER_CACHE": str(tmp_path / "cache" / ".." / "coursier"),
        },
    )
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout + r.stderr
    assert "skipping unsafe Ivy cache root: /" in out
    assert "skipping unsafe Coursier cache root:" in out


def test_missing_revision_never_evicts_a_module(tmp_path):
    """Malformed coordinates cannot broaden Coursier eviction to all versions."""
    env, incomplete, coursier_entries, neighbour = _ivy_layout(tmp_path)
    sbt = _fake_sbt_unresolved(
        tmp_path,
        fail_count=1,
        coordinate="com.globalmentor#hadoop-bare-naked-local-fs;",
    )
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert incomplete.exists()
    assert all(entry.exists() for entry in coursier_entries)
    assert neighbour.exists()
    assert "skipping unsafe unresolved coordinate" in (r.stdout + r.stderr)
    assert not (tmp_path / "curl_log").exists()


def _state_dependent_sbt(tmp_path: Path, blocking_dir: Path) -> Path:
    """A fake ``sbt`` that fails for as long as ``blocking_dir`` exists.

    This models the local-state failure path: the command's outcome is a pure
    function of the cache directory rather than of time or attempt count.
    """
    calls = tmp_path / "sbt_calls"
    fake = tmp_path / "state_dependent_sbt.sh"
    _write_exec(
        fake,
        f"""#!/usr/bin/env bash
n=0
if [ -f "{calls}" ]; then n=$(cat "{calls}"); fi
n=$((n + 1))
echo "$n" > "{calls}"
if [ -d "{blocking_dir}" ]; then
  echo "[error] sbt.librarymanagement.ResolveException: unresolved dependency:\
 com.globalmentor#hadoop-bare-naked-local-fs;0.1.0: not found" >&2
  exit 1
fi
echo "fake sbt: resolved after eviction"
exit 0
""",
    )
    return fake


def test_retry_succeeds_because_eviction_clears_the_blocking_state(tmp_path):
    """The regression this fix exists for: retries only help once state is cleared."""
    env, incomplete, _, _ = _ivy_layout(tmp_path)
    sbt = _state_dependent_sbt(tmp_path, incomplete)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "sbt_calls").read_text().strip() == "2"
    assert "fake sbt: resolved after eviction" in r.stdout


def test_without_eviction_the_same_state_exhausts_every_attempt(tmp_path):
    """Control: if eviction cannot reach the directory, retrying is futile.

    This is the pre-fix production behaviour - identical fast failures until the
    attempt budget is gone - and it guards against the eviction silently
    becoming a no-op.
    """
    env, incomplete, _, _ = _ivy_layout(tmp_path)
    sbt = _state_dependent_sbt(tmp_path, incomplete)
    env = dict(env)
    env["SBT_SETUP_IVY_HOME"] = str(tmp_path / "decoy-ivy")
    env["SBT_SETUP_MAX_ATTEMPTS"] = "3"
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode != 0
    assert (tmp_path / "sbt_calls").read_text().strip() == "3"
    assert incomplete.exists()


def test_probe_reports_maven_central_status_on_resolution_failure(tmp_path):
    """Ivy collapses 404/429/DNS into 'not found'; the probe must record which."""
    env, _, _, _ = _ivy_layout(tmp_path)
    env = dict(env)
    env["SBT_SETUP_CURL_CMD"] = str(_fake_curl(tmp_path, http_code="429"))
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout + r.stderr
    assert "Maven Central probe: HTTP 429" in out
    probed = (tmp_path / "curl_log").read_text().strip().splitlines()
    assert probed == [
        "https://repo1.maven.org/maven2/com/globalmentor/"
        "hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom",
        "https://repo.maven.apache.org/maven2/com/globalmentor/"
        "hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom",
    ], probed
    curl_args = (tmp_path / "curl_args").read_text().strip().splitlines()
    assert len(curl_args) == 2
    assert all("--max-time 15" in args for args in curl_args)


def test_parser_accepts_real_prefixed_scala_bridge_coordinate(tmp_path):
    """Keep the second coordinate and `(core / update)` log shape CI produced."""
    coordinate = "org.scala-lang#scala2-sbt-bridge;2.13.17"
    sbt = _fake_sbt_unresolved(
        tmp_path,
        fail_count=1,
        coordinate=coordinate,
        error_prefix="(core / update) ",
    )
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        env_overrides={
            "SBT_SETUP_IVY_HOME": str(tmp_path / "missing-ivy"),
            "SBT_SETUP_COURSIER_CACHE": str(tmp_path / "missing-coursier"),
        },
    )
    assert r.returncode == 0, r.stdout + r.stderr
    probed = (tmp_path / "curl_log").read_text().strip().splitlines()
    assert len(probed) == 2
    assert all(
        url.endswith(
            "/org/scala-lang/scala2-sbt-bridge/2.13.17/" "scala2-sbt-bridge-2.13.17.pom"
        )
        for url in probed
    )


def test_probe_does_not_run_without_a_resolution_failure(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=1)  # generic failure
    r = _run(tmp_path, "setup", sbt_cmd=sbt)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "Maven Central probe" not in (r.stdout + r.stderr)
    assert not (tmp_path / "curl_log").exists()


def test_probe_failure_never_changes_the_outcome(tmp_path):
    """A broken/absent curl must not turn a recoverable retry into a failure."""
    env, _, _, _ = _ivy_layout(tmp_path)
    env = dict(env)
    env["SBT_SETUP_CURL_CMD"] = str(tmp_path / "no-such-curl")
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr


def test_probe_runs_once_per_coordinate_across_retries(tmp_path):
    """Diagnostics must not amplify an outage with one request per attempt."""
    env, _, _, _ = _ivy_layout(tmp_path)
    env = dict(env)
    env["SBT_SETUP_CURL_CMD"] = str(_fake_curl(tmp_path, http_code="429"))
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=3)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    probed = (tmp_path / "curl_log").read_text().strip().splitlines()
    assert len(probed) == 2, probed


def test_mktemp_failure_stops_before_sbt_runs(tmp_path):
    sbt = _fake_sbt(tmp_path, fail_count=0)
    r = _run(
        tmp_path,
        "setup",
        sbt_cmd=sbt,
        env_overrides={"TMPDIR": str(tmp_path / "missing-temp-dir")},
    )
    assert r.returncode == 2
    assert "unable to create attempt log" in (r.stdout + r.stderr)
    assert not (tmp_path / "sbt_calls").exists()


def test_recursive_removal_uses_end_of_options_marker():
    assert 'rm -rf -- "$target"' in SCRIPT.read_text()


def test_evicted_entry_contents_are_logged(tmp_path):
    """The listing is what distinguishes a poisoned marker from a missing jar."""
    env, incomplete, _, _ = _ivy_layout(tmp_path)
    sbt = _fake_sbt_unresolved(tmp_path, fail_count=1)
    r = _run(tmp_path, "setup", sbt_cmd=sbt, env_overrides=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "ivydata-0.1.0.properties" in (r.stdout + r.stderr)
