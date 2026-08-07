"""Validation for the sbt-bootstrap CI hardening wiring in pipeline.yaml.

Ensures the durable fix is actually wired in: every sbt-running job restores the
shared bootstrap cache, the prewarm job exists, the cache keys invalidate on the
bootstrap inputs, and the duplicated inline retry blocks were replaced by the
shared helper. Run with: ``python -m pytest tools/ci/tests/test_pipeline_yaml.py``.
"""

import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
PIPELINE = REPO_ROOT / "pipeline.yaml"
SBT_CACHE_TPL = REPO_ROOT / "templates" / "sbt_cache.yml"
SBT_RETRY = REPO_ROOT / "tools" / "ci" / "sbt_retry.sh"
SBT_VERSION = REPO_ROOT / "tools" / "ci" / "get_sbt_version.sh"
DATABRICKS_IMPACT = REPO_ROOT / "tools" / "ci" / "databricks_impact.py"
DATABRICKS_STEPS_TPL = REPO_ROOT / "templates" / "databricks_e2e_steps.yml"
CLEAN_ACR_PIPELINE = REPO_ROOT / ".pipelines" / "clean-acr.yml"
RELEASE_COMPAT_PREREQUISITES = (
    REPO_ROOT / ".pipelines" / "release-compat-prerequisites.txt"
)


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


def _release_compat_script():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    steps = jobs["ReleaseBranchCompat"]["steps"]
    return next(
        step["bash"]
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Apply PR changes onto $(RELEASE_BRANCH)"
    )


def _git(repo, *args):
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"git {' '.join(args)} failed\nstdout:\n{result.stdout}\nstderr:\n"
        f"{result.stderr}"
    )
    return result


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
    # Dependency caches invalidate on every root/project sbt definition.
    dependency_keys = [k for k in keys if "sbtboot" not in k]
    assert all("project/*.sbt" in k for k in dependency_keys)
    assert all("**/build.sbt" in k for k in dependency_keys)
    assert all("project/**/*.scala" in k for k in dependency_keys)
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
        if isinstance(s, dict) and s.get("displayName") == "Ensure sbt cache is usable"
    ]
    assert len(fallback_scripts) == 1
    fallback_script = fallback_scripts[0]
    assert "SBT_SETUP_MAX_STAGGER_SECONDS" in fallback_script
    assert 'bash "$SBT_RETRY_SCRIPT_PATH" update' in fallback_script
    assert 'if [ "$exact_hit" != "true" ]' in fallback_script
    parameters = {parameter["name"]: parameter for parameter in data["parameters"]}
    assert parameters["retryScriptPath"]["default"] == "tools/ci/sbt_retry.sh"

    fallback_step = next(
        s
        for s in steps
        if isinstance(s, dict) and s.get("displayName") == "Ensure sbt cache is usable"
    )
    for cache_hit_var in (
        "SBT_BOOT_CACHE_RESTORED",
        "SBT_IVY_CACHE_RESTORED",
        "SBT_COURSIER_CACHE_RESTORED",
    ):
        assert f"$({cache_hit_var})" in fallback_step["env"].values()
    assert (
        fallback_step["env"]["SBT_RETRY_SCRIPT_PATH"]
        == "${{ parameters.retryScriptPath }}"
    )


def test_sbt_retry_script_referenced_and_exists():
    assert SBT_RETRY.exists()
    assert SBT_VERSION.exists()
    txt = _pipeline_text()
    assert "tools/ci/sbt_retry.sh" in txt
    assert "tools/ci/get_sbt_version.sh" in txt


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
    cache_steps = [
        step
        for step in prewarm["steps"]
        if isinstance(step, dict) and step.get("template") == "templates/sbt_cache.yml"
    ]
    assert len(cache_steps) == 1
    assert cache_steps[0]["parameters"] == {
        "prewarm": True,
        "maxAttempts": 7,
        "maxBackoffSeconds": 180,
    }


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


def test_fabric_e2e_cleans_stale_artifacts_before_running_tests():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    fabric_e2e = jobs["FabricE2E"]
    e2e_steps = [
        step
        for step in fabric_e2e["steps"]
        if isinstance(step, dict) and step.get("displayName") == "E2E"
    ]
    assert len(e2e_steps) == 1

    script = e2e_steps[0]["inputs"]["inlineScript"]
    cleanup_command = (
        '"testOnly com.microsoft.azure.synapse.ml.nbtest.FabricTestCleanup"'
    )
    test_command = (
        '"testOnly com.microsoft.azure.synapse.ml.nbtest.FabricSmokeTests '
        'com.microsoft.azure.synapse.ml.nbtest.FabricNotebookTests"'
    )
    assert script.count("sbt ") == 1
    assert cleanup_command in script
    assert test_command in script
    assert script.index(cleanup_command) < script.index(test_command)


def test_release_compat_accepts_github_target_and_uses_one_sbt_process():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    release_compat = jobs["ReleaseBranchCompat"]
    matrix = release_compat["strategy"]["matrix"]
    assert set(matrix) == {"spark4.1"}
    assert matrix["spark4.1"]["RELEASE_BRANCH"] == "spark4.1"

    condition = release_compat["condition"]
    assert "System.PullRequest.TargetBranch" in condition
    assert "'master'" in condition
    assert "'refs/heads/master'" in condition

    steps = release_compat["steps"]
    assert not any(step.get("task") == "AzureCLI@2" for step in steps)
    assert not any(step.get("template") == "templates/kv.yml" for step in steps)

    helper_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Stage sbt retry helper for release checkout"
    ]
    assert len(helper_steps) == 1
    helper_script = helper_steps[0]["bash"]
    assert (
        'install -m 755 tools/ci/sbt_retry.sh "$(Agent.TempDirectory)/sbt_retry.sh"'
        in helper_script
    )
    assert 'test -x "$(Agent.TempDirectory)/sbt_retry.sh"' in helper_script

    rebase_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName") == "Apply PR changes onto $(RELEASE_BRANCH)"
    ]
    assert len(rebase_steps) == 1
    assert steps.index(helper_steps[0]) < steps.index(rebase_steps[0])
    rebase_script = rebase_steps[0]["bash"]
    assert "PR_MERGE_HEAD=$(git rev-parse HEAD)" in rebase_script
    assert "TARGET_HEAD=$(git rev-parse HEAD^1)" in rebase_script
    assert "SOURCE_HEAD=$(git rev-parse HEAD^2)" in rebase_script
    assert 'git diff --name-only -z "$TARGET_HEAD" HEAD' in rebase_script
    assert "pipeline.yaml|CODEOWNERS" in rebase_script
    assert "templates/*|tools/acr/*|tools/ci/*" in rebase_script
    assert "variable=releaseCompatRequired]false" in rebase_script
    assert "variable=releaseCompatRequired]true" in rebase_script
    assert (
        'git diff --binary --full-index "$TARGET_HEAD" "$PR_MERGE_HEAD"'
        in rebase_script
    )
    assert "REPLAY_PATHS=()" in rebase_script
    assert 'git cat-file -e "$PR_MERGE_HEAD:$path"' in rebase_script
    assert 'git cat-file -e "$RELEASE_TIP:$path"' in rebase_script
    assert "Skipping deletion already absent on $(RELEASE_BRANCH)" in rebase_script
    assert "[ ${#REPLAY_PATHS[@]} -eq 0 ]" in rebase_script
    assert '"${REPLAY_PATHS[@]}" > "$PATCH_PATH"' in rebase_script
    assert 'PREREQUISITES_CONFIG=".pipelines/release-compat-prerequisites.txt"' in (
        rebase_script
    )
    assert 'git show "$PR_MERGE_HEAD:$PREREQUISITES_CONFIG"' in rebase_script
    assert '[[ ! "$PREREQUISITE" =~ ^[0-9a-fA-F]{40}$ ]]' in rebase_script
    assert (
        'git merge-base --is-ancestor "$PREREQUISITE" "$TARGET_HEAD"' in rebase_script
    )
    assert 'git rev-parse "$PREREQUISITE^1"' in rebase_script
    assert (
        'git diff --name-only -z "$PREREQUISITE_PARENT" "$PREREQUISITE"'
        in rebase_script
    )
    release_exclusions = (
        ".github/*|.pipelines/*|docs/*|templates/*|tools/acr/*|tools/ci/*|"
        "tools/docker/*|tools/helm/*|website/*"
    )
    assert rebase_script.count(release_exclusions) == 2
    assert (
        'git diff --binary --full-index "$PREREQUISITE_PARENT" "$PREREQUISITE"'
        in rebase_script
    )
    assert "git checkout --detach $RELEASE_TIP" in rebase_script
    assert 'git apply --reverse --check --index "$PREREQUISITE_PATCH"' in rebase_script
    assert 'git apply --3way --index "$PREREQUISITE_PATCH"' in rebase_script
    assert 'git apply --3way --index "$PATCH_PATH"' in rebase_script
    assert rebase_script.index(
        'git show "$PR_MERGE_HEAD:$PREREQUISITES_CONFIG"'
    ) < rebase_script.index("git checkout --detach $RELEASE_TIP")
    assert rebase_script.index(
        'git apply --3way --index "$PREREQUISITE_PATCH"'
    ) < rebase_script.index('git apply --3way --index "$PATCH_PATH"')
    assert "git rebase" not in rebase_script
    assert "CONFLICTING_FILES=$(git diff --name-only --diff-filter=U" in rebase_script
    assert "before conflict detection" in rebase_script
    assert rebase_script.count("printf '%s\\n' \"$APPLY_OUTPUT\"") == 4

    cache_step = next(
        step
        for step in steps
        if isinstance(step, dict) and step.get("template") == "templates/sbt_cache.yml"
    )
    assert steps.index(rebase_steps[0]) < steps.index(cache_step)
    assert cache_step["parameters"]["retryScriptPath"] == (
        "$(Agent.TempDirectory)/sbt_retry.sh"
    )

    validation_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("displayName")
        == "Validate $(RELEASE_BRANCH) after applying PR changes"
    ]
    assert len(validation_steps) == 1
    script = validation_steps[0]["bash"]
    assert script.count("sbt $(SBT_JAVA_OPTS)") == 1
    assert "test:compile" in script
    assert "getDatasets" not in script
    assert "testOnly" not in script
    assert '"project ' not in script
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


def test_release_compat_prerequisites_are_full_shas():
    lines = [
        line.strip()
        for line in RELEASE_COMPAT_PREREQUISITES.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert lines == ["04897bae9baa08f0d67855566f7bad235791d508"]
    assert all(re.fullmatch(r"[0-9a-fA-F]{40}", line) for line in lines)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_replays_prerequisite_before_pr_patch():
    scratch_root = REPO_ROOT / "target" / f"release-compat-replay-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        subprocess.run(
            ["git", "init", "--initial-branch=master", str(repo)],
            check=True,
            capture_output=True,
            text=True,
        )
        _git(repo, "config", "user.name", "Release Compat Test")
        _git(repo, "config", "user.email", "release-compat@example.test")

        source_file = repo / "src" / "value.txt"
        source_file.parent.mkdir()
        source_file.write_text("base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        source_file.write_text("aad\n")
        _git(repo, "commit", "-am", "prerequisite")
        prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_text(f"# prerequisite\n{prerequisite}\n")
        source_file.write_text("aad\nsearch\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature")

        _git(repo, "checkout", "master")
        _git(repo, "merge", "--no-ff", "source", "-m", "merge feature")

        subprocess.run(
            ["git", "init", "--bare", str(origin)],
            check=True,
            capture_output=True,
            text=True,
        )
        _git(repo, "remote", "add", "origin", str(origin))
        _git(repo, "push", "origin", "master", "source", "release")

        script = _release_compat_script()
        script = script.replace("$(Agent.TempDirectory)", str(agent_temp))
        script = script.replace("$(RELEASE_BRANCH)", "release")
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )

        assert (
            result.returncode == 0
        ), f"release replay failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        assert source_file.read_text() == "aad\nsearch\n"
        assert _git(repo, "diff", "--cached", "--name-only").stdout.splitlines() == [
            "src/value.txt"
        ]
        assert f"Prerequisite {prerequisite} applies cleanly" in result.stdout
        assert "PR changes apply cleanly onto release" in result.stdout
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


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


def test_build_docker_allows_time_for_both_image_builds():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    assert jobs["BuildDocker"]["timeoutInMinutes"] >= 120


def test_publish_jobs_resolve_and_preserve_package_versions():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}

    publish = jobs["Publish"]
    publish_steps = publish["steps"]
    assert any(step.get("task") == "MavenAuthenticate@0" for step in publish_steps)
    assert any(step.get("template") == "templates/conda.yml" for step in publish_steps)
    assert any(step.get("template") == "templates/kv.yml" for step in publish_steps)
    version_step = next(
        step
        for step in publish_steps
        if step.get("displayName") == "Resolve package version"
    )
    assert "get_sbt_version.sh" in version_step["bash"]
    artifact_step = next(
        step for step in publish_steps if step.get("displayName") == "Publish Artifacts"
    )
    artifact_script = artifact_step["inputs"]["inlineScript"]
    for task in (
        "packagePython uploadNotebooks",
        "publishBlob publishDocs publishR publishPython",
        "publishLocalSigned",
    ):
        assert task in artifact_script
    assert artifact_step["env"]["SYNAPSEML_ENABLE_PUBLISH"] is True
    assert "$(packageVersion)" in artifact_script

    release = jobs["Release"]
    release_steps = release["steps"]
    release_version = next(
        step
        for step in release_steps
        if step.get("displayName") == "Validate release package version"
    )
    assert "get_sbt_version.sh" in release_version["bash"]
    assert 'EXPECTED_VERSION="${RELEASE_TAG#v}"' in release_version["bash"]
    assert "PACKAGE_VERSION" in release_version["bash"]
    release_guard_index = release_steps.index(release_version)
    side_effect_steps = [
        next(step for step in release_steps if "git-chglog" in step.get("bash", "")),
        next(step for step in release_steps if step.get("task") == "GitHubRelease@1"),
        next(step for step in release_steps if "publishPypi" in step.get("bash", "")),
        next(
            step
            for step in release_steps
            if "publishLocalSigned" in step.get("bash", "")
        ),
        next(
            step
            for step in release_steps
            if step.get("displayName") == "ESRP Publish Package"
        ),
    ]
    assert all(
        release_guard_index < release_steps.index(step) for step in side_effect_steps
    )


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
