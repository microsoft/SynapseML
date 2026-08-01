"""Validation for the sbt-bootstrap CI hardening wiring in pipeline.yaml.

Ensures the durable fix is actually wired in: every sbt-running job restores the
shared bootstrap cache, the prewarm job exists, the cache keys invalidate on the
bootstrap inputs, and the duplicated inline retry blocks were replaced by the
shared helper. Run with: ``python -m pytest tools/ci/tests/test_pipeline_yaml.py``.
"""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
PIPELINE = REPO_ROOT / "pipeline.yaml"
SBT_CACHE_TPL = REPO_ROOT / "templates" / "sbt_cache.yml"
SBT_RETRY = REPO_ROOT / "tools" / "ci" / "sbt_retry.sh"


def _pipeline_text():
    return PIPELINE.read_text()


def _jobs(node):
    if isinstance(node, dict):
        if "job" in node:
            yield node
        for value in node.values():
            yield from _jobs(value)
    elif isinstance(node, list):
        for value in node:
            yield from _jobs(value)


def test_pipeline_and_templates_parse():
    assert yaml.safe_load(PIPELINE.read_text()) is not None
    for tpl in (REPO_ROOT / "templates").glob("*.yml"):
        assert yaml.safe_load(tpl.read_text()) is not None, f"{tpl} failed to parse"


def test_sbt_cache_template_exists_and_parses():
    assert SBT_CACHE_TPL.exists()
    data = yaml.safe_load(SBT_CACHE_TPL.read_text())
    steps = data["steps"]
    tasks = [s for s in steps if s.get("task", "").startswith("Cache@2")]
    assert len(tasks) == 3, "expected boot + ivy + Coursier cache steps"
    keys = [t["inputs"]["key"] for t in tasks]
    paths = [t["inputs"]["path"] for t in tasks]
    # Boot cache invalidates on the pinned sbt version.
    assert any("build.properties" in k and "sbtboot" in k for k in keys)
    # Ivy cache invalidates on plugins + build definitions too.
    assert any("plugins.sbt" in k and "build.sbt" in k for k in keys)
    assert any(".sbt/boot" in p for p in paths)
    assert any(".ivy2/cache" in p for p in paths)
    assert any(".cache/coursier" in p for p in paths)
    assert all(t["inputs"].get("cacheHitVar") for t in tasks)
    # Cache miss/corruption must fall back safely, not fail the job.
    for t in tasks:
        assert t.get("continueOnError") is True
    fallback_scripts = [
        s.get("bash", "")
        for s in steps
        if isinstance(s, dict)
        and s.get("displayName") == "Configure sbt cold-cache fallback"
    ]
    assert len(fallback_scripts) == 1
    fallback_script = fallback_scripts[0]
    assert "SBT_SETUP_MAX_STAGGER_SECONDS" in fallback_script
    for cache_hit_var in (
        "SBT_BOOT_CACHE_RESTORED",
        "SBT_IVY_CACHE_RESTORED",
        "SBT_COURSIER_CACHE_RESTORED",
    ):
        assert f'[ "$({cache_hit_var})" = "true" ]' in fallback_script


def test_sbt_retry_script_referenced_and_exists():
    assert SBT_RETRY.exists()
    txt = _pipeline_text()
    assert "tools/ci/sbt_retry.sh" in txt


def test_no_dormant_ivy_cache_placeholders_remain():
    txt = _pipeline_text()
    assert "ivy_cache" not in txt, "dormant ivy_cache placeholders should be replaced"


def test_no_duplicated_inline_setup_retry_remains():
    txt = _pipeline_text()
    # Old duplicated idioms removed in favour of the shared helper.
    assert "retry_sbt_setup()" not in txt
    assert '(timeout 5m sbt setup) || (echo "retrying"' not in txt


def test_prewarm_job_present():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    assert "BuildAndCacheSbt" in jobs
    assert "BuildAndCacheCondaEnv" in jobs  # existing prewarm preserved
    prewarm = jobs["BuildAndCacheSbt"]
    assert "condition" not in prewarm, "prewarm must run whenever sbt jobs can run"
    warm_steps = [
        step
        for step in prewarm["steps"]
        if isinstance(step, dict) and "sbt_retry.sh update" in step.get("bash", "")
    ]
    assert len(warm_steps) == 1
    assert warm_steps[0].get("continueOnError") is not True


def test_every_sbt_running_job_waits_for_the_prewarm_cache():
    """Each sbt job must restore the cache after the required prewarm job."""
    data = yaml.safe_load(_pipeline_text())

    def flatten(obj):
        out = []
        if isinstance(obj, dict):
            for v in obj.values():
                out += flatten(v)
        elif isinstance(obj, list):
            for v in obj:
                out += flatten(v)
        elif isinstance(obj, str):
            out.append(obj)
        return out

    offenders = []
    for job in _jobs(data["jobs"]):
        if job["job"] == "BuildAndCacheSbt":
            continue
        steps = job.get("steps", [])
        texts = flatten(steps)
        runs_sbt = any(
            "sbt " in t or t.strip().startswith("sbt") or "sbt_retry.sh" in t
            for t in texts
        )
        templates = [
            s.get("template")
            for s in steps
            if isinstance(s, dict) and s.get("template")
        ]
        uses_cache = "templates/sbt_cache.yml" in templates
        depends_on = job.get("dependsOn", [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        condition = job.get("condition")
        gated_by_success = condition is None or "succeeded()" in condition
        if runs_sbt and (
            not uses_cache
            or "BuildAndCacheSbt" not in depends_on
            or not gated_by_success
        ):
            offenders.append(job.get("job"))
    assert not offenders, f"sbt jobs missing required cache gate: {offenders}"
