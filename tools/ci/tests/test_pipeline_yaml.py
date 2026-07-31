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


def test_pipeline_and_templates_parse():
    assert yaml.safe_load(PIPELINE.read_text()) is not None
    for tpl in (REPO_ROOT / "templates").glob("*.yml"):
        assert yaml.safe_load(tpl.read_text()) is not None, f"{tpl} failed to parse"


def test_sbt_cache_template_exists_and_parses():
    assert SBT_CACHE_TPL.exists()
    data = yaml.safe_load(SBT_CACHE_TPL.read_text())
    steps = data["steps"]
    tasks = [s for s in steps if s.get("task", "").startswith("Cache@2")]
    assert len(tasks) == 2, "expected boot + ivy cache steps"
    keys = [t["inputs"]["key"] for t in tasks]
    paths = [t["inputs"]["path"] for t in tasks]
    # Boot cache invalidates on the pinned sbt version.
    assert any("build.properties" in k and "sbtboot" in k for k in keys)
    # Ivy cache invalidates on plugins + build definitions too.
    assert any("plugins.sbt" in k and "build.sbt" in k for k in keys)
    assert any(".sbt/boot" in p for p in paths)
    assert any(".ivy2/cache" in p for p in paths)
    # Cache miss/corruption must fall back safely, not fail the job.
    for t in tasks:
        assert t.get("continueOnError") is True


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
    jobs = {j.get("job") for j in data["jobs"] if isinstance(j, dict)}
    assert "BuildAndCacheSbt" in jobs
    assert "BuildAndCacheCondaEnv" in jobs  # existing prewarm preserved


def test_every_sbt_running_job_uses_the_cache_template():
    """Each job that invokes sbt must include the sbt_cache template."""
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
    for job in data["jobs"]:
        if not isinstance(job, dict) or "job" not in job:
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
        if runs_sbt and not uses_cache:
            offenders.append(job.get("job"))
    assert not offenders, f"sbt jobs missing sbt_cache template: {offenders}"
