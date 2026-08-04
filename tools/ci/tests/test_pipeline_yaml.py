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
DATABRICKS_IMPACT = REPO_ROOT / "tools" / "ci" / "databricks_impact.py"
DATABRICKS_STEPS_TPL = REPO_ROOT / "templates" / "databricks_e2e_steps.yml"
CLEAN_ACR_PIPELINE = REPO_ROOT / ".pipelines" / "clean-acr.yml"


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
    assert yaml.safe_load(CLEAN_ACR_PIPELINE.read_text()) is not None
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
    assert "BuildAndCacheCondaEnv" not in jobs
    prewarm = jobs["BuildAndCacheSbt"]
    assert "condition" not in prewarm, "prewarm must run whenever sbt jobs can run"
    warm_steps = [
        step
        for step in prewarm["steps"]
        if isinstance(step, dict) and "sbt_retry.sh update" in step.get("bash", "")
    ]
    assert len(warm_steps) == 1
    assert warm_steps[0].get("continueOnError") is not True


def test_databricks_e2e_uses_fail_open_pr_impact_detection():
    assert DATABRICKS_IMPACT.exists()
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    prewarm = jobs["BuildAndCacheSbt"]
    databricks_cpu = jobs["DatabricksCPUE2E"]
    databricks_gpu = jobs["DatabricksGPUE2E"]

    detection_steps = [
        step
        for step in prewarm["steps"]
        if isinstance(step, dict) and step.get("name") == "detectDatabricksImpact"
    ]
    assert len(detection_steps) == 1
    detection_script = detection_steps[0]["bash"]
    assert "databricks_impact.py --null --suite cpu" in detection_script
    assert "databricks_impact.py --null --suite gpu" in detection_script
    assert "Build.Reason" in detection_script
    assert "SYSTEM_PULLREQUEST_TARGETBRANCH" in detection_script
    assert "isOutput=true" in detection_script
    assert "run_databricks_cpu=true" in detection_script
    assert "run_databricks_gpu=true" in detection_script
    assert "runDatabricksCpuE2E;isOutput=true" in detection_script
    assert "runDatabricksGpuE2E;isOutput=true" in detection_script

    for job, suite in ((databricks_cpu, "Cpu"), (databricks_gpu, "Gpu")):
        condition = job["condition"]
        assert "succeeded()" in condition
        assert "variables.runTests" in condition
        assert "parameters.testDatabricksE2E" in condition
        assert (
            "dependencies.BuildAndCacheSbt.outputs"
            f"['detectDatabricksImpact.runDatabricks{suite}E2E']"
        ) in condition
        assert "DATABRICKS_SUITE" not in condition
        assert job["steps"] == [{"template": "templates/databricks_e2e_steps.yml"}]

    assert len(databricks_cpu["strategy"]["matrix"]) == 5
    assert "strategy" not in databricks_gpu
    assert (
        databricks_gpu["variables"]["TEST-CLASS"]
        == "com.microsoft.azure.synapse.ml.nbtest.DatabricksGPUTests"
    )

    steps = yaml.safe_load(DATABRICKS_STEPS_TPL.read_text())["steps"]
    assert any(step.get("displayName") == "E2E" for step in steps)
    assert any(step.get("displayName") == "Publish Test Results" for step in steps)


def test_release_compat_accepts_github_target_and_uses_one_sbt_process():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    release_compat = jobs["ReleaseBranchCompat"]

    condition = release_compat["condition"]
    assert "System.PullRequest.TargetBranch" in condition
    assert "'master'" in condition
    assert "'refs/heads/master'" in condition

    steps = release_compat["steps"]
    assert not any(step.get("task") == "AzureCLI@2" for step in steps)
    assert not any(step.get("template") == "templates/kv.yml" for step in steps)

    rebase_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Apply PR changes onto $(RELEASE_BRANCH)"
    ]
    assert len(rebase_steps) == 1
    rebase_script = rebase_steps[0]["bash"]
    assert "TARGET_HEAD=$(git rev-parse HEAD^1)" in rebase_script
    assert "SOURCE_HEAD=$(git rev-parse HEAD^2)" in rebase_script
    assert "git rebase --onto $RELEASE_TIP $TARGET_HEAD $SOURCE_HEAD" in rebase_script
    assert "git rebase --onto $PR_HEAD $MASTER_BASE" not in rebase_script
    assert 'git diff --name-only -z "$TARGET_HEAD" HEAD' in rebase_script
    assert "pipeline.yaml|CODEOWNERS" in rebase_script
    assert "templates/*|tools/acr/*|tools/ci/*" in rebase_script
    assert "variable=releaseCompatRequired]false" in rebase_script
    assert "variable=releaseCompatRequired]true" in rebase_script

    validation_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Validate $(RELEASE_BRANCH) after rebase"
    ]
    assert len(validation_steps) == 1
    script = validation_steps[0]["bash"]
    assert script.count("sbt $(SBT_JAVA_OPTS)") == 1
    assert "test:compile" in script
    assert "getDatasets" in script
    for project in ("core", "vw", "opencv"):
        assert f'"project {project}"' in script
    assert "sbt_retry.sh" not in script
    assert "for pkg in" not in script
    assert (
        validation_steps[0]["condition"]
        == "and(succeeded(), eq(variables.releaseCompatRequired, 'true'))"
    )

    result_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Publish $(RELEASE_BRANCH) Test Results"
    ]
    assert len(result_steps) == 1
    assert "releaseCompatRequired" in result_steps[0]["condition"]


def test_acr_cleanup_is_schedule_only_and_uses_dedicated_identity():
    data = yaml.safe_load(CLEAN_ACR_PIPELINE.read_text())
    assert data["trigger"] == "none"
    assert data["pr"] == "none"
    assert data["variables"]["azureServiceConnection"] == "synapseml-clean-acr"

    cleanup = next(
        step for step in data["steps"] if step.get("displayName") == "Clean ACR"
    )
    assert cleanup["inputs"]["azureSubscription"] == "$(azureServiceConnection)"
    script = cleanup["inputs"]["inlineScript"]
    assert "pip install" not in script
    assert "clean-acr-connection-string" not in script
    assert "python tools/acr/clean_acr.py" in script


def test_non_azure_setup_steps_do_not_authenticate_with_azure_cli():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    expected_bash_steps = {
        "Style": {"Scala Style Check"},
        "BuildDocker": {"Get Docker Tag + Version"},
        "PythonTests": {"Install and package deps", "Generate Codecov report"},
        "RTests": {"Prepare for tests", "Generate Codecov report"},
        "WebsiteSamplesTests": {"Generate Codecov report"},
        "UnitTests": {"Setup repo", "Generate Codecov report"},
    }

    for job_name, display_names in expected_bash_steps.items():
        steps = jobs[job_name]["steps"]
        for display_name in display_names:
            step = next(
                step for step in steps if step.get("displayName") == display_name
            )
            assert "bash" in step, f"{job_name}/{display_name} should be a Bash step"
            assert step.get("task") != "AzureCLI@2"


def test_style_does_not_restore_the_full_conda_environment():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    style = jobs["Style"]
    templates = [step.get("template") for step in style["steps"] if "template" in step]
    assert "templates/conda.yml" not in templates
    python_style = next(
        step
        for step in style["steps"]
        if step.get("displayName") == "Python Style Check"
    )
    assert "black[jupyter]==22.3.0" in python_style["bash"]


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
