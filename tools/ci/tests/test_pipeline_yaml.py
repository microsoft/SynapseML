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
BUILD_SBT = REPO_ROOT / "build.sbt"
PIPELINE = REPO_ROOT / "pipeline.yaml"
SBT_CACHE_TPL = REPO_ROOT / "templates" / "sbt_cache.yml"
SBT_RETRY = REPO_ROOT / "tools" / "ci" / "sbt_retry.sh"
SBT_VERSION = REPO_ROOT / "tools" / "ci" / "get_sbt_version.sh"
DATABRICKS_IMPACT = REPO_ROOT / "tools" / "ci" / "databricks_impact.py"
DATABRICKS_STEPS_TPL = REPO_ROOT / "templates" / "databricks_e2e_steps.yml"
CLEAN_ACR_PIPELINE = REPO_ROOT / ".pipelines" / "clean-acr.yml"
DEMO_DOCKERFILE = REPO_ROOT / "tools" / "docker" / "demo" / "Dockerfile"
MINIMAL_DOCKERFILE = REPO_ROOT / "tools" / "docker" / "minimal" / "Dockerfile"
RELEASE_COMPAT_PREREQUISITES = (
    REPO_ROOT / ".pipelines" / "release-compat-prerequisites.txt"
)
ASCII_WHITESPACE = " \t\r\n\v\f"


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


def _init_release_compat_scratch_repo(repo):
    subprocess.run(
        ["git", "init", "--initial-branch=master", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    for key, value in (
        ("user.name", "Release Compat Test"),
        ("user.email", "release-compat@example.test"),
        ("core.autocrlf", "false"),
        ("core.hooksPath", ".git/hooks-disabled"),
        ("commit.gpgsign", "false"),
        ("merge.autostash", "false"),
        ("rebase.autostash", "false"),
    ):
        _git(repo, "config", key, value)


def _assert_git_clean(repo, context):
    status = _git(repo, "status", "--short").stdout.strip()
    assert not status, f"{context} left scratch repo dirty:\n{status}"


def _is_normalized_prerequisite_path(path):
    return (
        bool(path)
        and path == path.strip(ASCII_WHITESPACE)
        and not any(character in path for character in "\t\r\n")
        and not path.startswith("/")
        and not path.endswith("/")
        and all(part not in {"", ".", ".."} for part in path.split("/"))
    )


def test_pipeline_and_templates_parse():
    assert yaml.safe_load(PIPELINE.read_text()) is not None
    assert yaml.safe_load(CLEAN_ACR_PIPELINE.read_text()) is not None
    for tpl in (REPO_ROOT / "templates").glob("*.yml"):
        assert yaml.safe_load(tpl.read_text()) is not None, f"{tpl} failed to parse"


def test_build_has_canonical_maven_central_fallback():
    build = "\n".join(
        line
        for line in BUILD_SBT.read_text().splitlines()
        if not line.lstrip().startswith("//")
    )
    resolver = (
        r'"Maven Central fallback"\s+at\s+' r'"https://repo\.maven\.apache\.org/maven2"'
    )
    active_setting = (
        rf"ThisBuild\s*/\s*resolvers\s*"
        rf"(?:\+=\s*{resolver}|\+\+=\s*Seq\s*\([^)]*{resolver}[^)]*\))"
    )
    assert len(re.findall(active_setting, build, flags=re.DOTALL)) == 1


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
    stagger_export = "export SBT_SETUP_MAX_STAGGER_SECONDS=0"
    assert stagger_export in fallback_script
    assert 'bash "$SBT_RETRY_SCRIPT_PATH" update' in fallback_script
    assert fallback_script.index(stagger_export) < fallback_script.index(
        'bash "$SBT_RETRY_SCRIPT_PATH" update'
    )
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


def test_fabric_e2e_runs_openai_prompt_with_exact_artifacts():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    fabric_e2e = jobs["FabricE2E"]

    assert "System.PullRequest.IsFork" in fabric_e2e["condition"]
    assert fabric_e2e["timeoutInMinutes"] >= 180
    assert fabric_e2e["cancelTimeoutInMinutes"] >= 5

    feed_auth_steps = [
        step
        for step in fabric_e2e["steps"]
        if isinstance(step, dict)
        and step.get("displayName") == "Authenticate fabric-spark-cli feed"
    ]
    assert len(feed_auth_steps) == 1
    feed_auth_step = feed_auth_steps[0]
    assert feed_auth_step["task"] == "PipAuthenticate@1"
    assert feed_auth_step["inputs"]["artifactFeeds"] == "A365/SynapseMaven"
    assert "succeededOrFailed()" in feed_auth_step["condition"]

    openai_steps = [
        step
        for step in fabric_e2e["steps"]
        if isinstance(step, dict)
        and step.get("displayName") == "Run OpenAIPrompt on Fabric"
    ]
    assert len(openai_steps) == 1
    openai_step = openai_steps[0]
    assert openai_step["task"] == "AzureCLI@2"
    assert openai_step["inputs"]["azureSubscription"] == "SynapseML Build"
    assert fabric_e2e["steps"].index(feed_auth_step) < fabric_e2e["steps"].index(
        openai_step
    )
    assert "runFabricOpenAIPrompt" in openai_step["condition"]
    assert "succeededOrFailed()" in openai_step["condition"]

    script = openai_step["inputs"]["inlineScript"]
    assert "sbt core/packageBin cognitive/packageBin" in script
    assert "fabric-spark-cli==0.1.20260807.5" in script
    assert "az account get-access-token" not in script
    assert "feed_token" not in script
    assert data["variables"]["FABRIC_OPENAI_WORKSPACE"] == (
        "DONT_DELETE_SynapseML_Build"
    )
    assert 'workspace="$FABRIC_OPENAI_WORKSPACE"' in script
    assert openai_step["env"]["FABRIC_OPENAI_WORKSPACE"] == (
        "$(FABRIC_OPENAI_WORKSPACE)"
    )
    assert "INTEGRATION_ACCOUNT" not in openai_step["env"]
    assert "INTEGRATION_WORKSPACE_PREFIX" not in openai_step["env"]
    assert "--scenario openai-prompt-ai-functions" in script
    assert '--extra-jar "$core_jar"' in script
    assert '--extra-jar "$cognitive_jar"' in script
    assert "fabricOpenAIPromptAttempted]true" in script
    assert "OPENAI_API_KEY" not in script
    assert "AZURE_OPENAI_API_KEY" not in script

    result_steps = [
        step
        for step in fabric_e2e["steps"]
        if isinstance(step, dict)
        and step.get("displayName") == "Publish Fabric OpenAIPrompt Results"
    ]
    assert len(result_steps) == 1
    assert result_steps[0]["task"] == "PublishTestResults@2"
    assert result_steps[0]["inputs"]["testResultsFormat"] == "JUnit"
    assert "fabricOpenAIPromptAttempted" in result_steps[0]["condition"]

    artifact_steps = [
        step
        for step in fabric_e2e["steps"]
        if isinstance(step, dict)
        and step.get("displayName") == "Publish Fabric OpenAIPrompt Evidence"
    ]
    assert len(artifact_steps) == 1
    assert artifact_steps[0]["task"] == "PublishPipelineArtifact@1"
    assert artifact_steps[0]["inputs"]["artifact"] == "fabric-openai-e2e"
    assert "fabricOpenAIPromptAttempted" in artifact_steps[0]["condition"]


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
    assert "LINE_WITHOUT_CR=\"${RAW_LINE%$'\\r'}\"" in rebase_script
    assert r"""[[ "$SCOPED_LINE" == *$'\t'* ]]""" in rebase_script
    assert r"""[[ "$RAW_LINE" == *$'\t'* ]]""" not in rebase_script
    assert "PREREQUISITE=\"${SCOPED_LINE%%$'\\t'*}\"" in rebase_script
    assert "PREREQUISITE_SCOPE=\"${SCOPED_LINE#*$'\\t'}\"" in rebase_script
    assert "CONFIGURED_PREREQUISITE_SCOPES=()" in rebase_script
    assert (
        'git merge-base --is-ancestor "$PREREQUISITE" "$TARGET_HEAD"' in rebase_script
    )
    assert 'case "/$path/" in' in rebase_script
    assert (
        "Scoped prerequisite path must not have leading or trailing whitespace: $path"
        in rebase_script
    )
    assert "Scoped prerequisite path must be normalized: $path" in rebase_script
    assert 'for REPLAY_PATH in "${REPLAY_PATHS[@]}"' in rebase_script
    assert '[ "$path" = "$REPLAY_PATH" ]' in rebase_script
    assert 'PREREQUISITE_PATHS+=("$path")' in rebase_script
    assert "has no paths in this PR replay; skipping" in rebase_script
    assert rebase_script.index('case "/$path/" in') < rebase_script.index(
        'for REPLAY_PATH in "${REPLAY_PATHS[@]}"'
    )
    assert 'git diff --quiet "$PREREQUISITE_PARENT" "$PREREQUISITE" --' in rebase_script
    assert 'git cat-file -e "$PREREQUISITE_PARENT:$path"' in rebase_script
    assert 'git rev-parse "$PREREQUISITE:$path"' in rebase_script
    assert 'BASELINE_COMMIT="$TARGET_HEAD"' in rebase_script
    assert 'git rev-parse "$BASELINE_COMMIT:$path"' in rebase_script
    assert '[ "$PREREQUISITE_BLOB" != "$BASELINE_BLOB" ]' in rebase_script
    assert '":(literal)$path"' in rebase_script
    assert '"${PREREQUISITE_PATHSPECS[@]}"' in rebase_script
    assert "eval " not in rebase_script
    assert 'git rev-parse "$PREREQUISITE^1"' in rebase_script
    guarded_dependent_parent = (
        'if ! LATER_PARENT=$(git rev-parse "$LATER_PREREQUISITE^1" '
        "2>/dev/null); then"
    )
    assert rebase_script.count(guarded_dependent_parent) == 2
    assert (
        rebase_script.count(
            "Dependent release compatibility prerequisite "
            "$LATER_PREREQUISITE has no first parent"
        )
        == 2
    )
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
    assert "git reset --hard $RELEASE_TIP" in rebase_script
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


def test_release_compat_prerequisites_have_valid_format():
    lines = [
        line
        for line in RELEASE_COMPAT_PREREQUISITES.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    # The list is meant to drain to empty once every validated release branch carries the
    # backports, and the replay script already treats an absent or empty list as a no-op.
    entries = [line.split("\t") for line in lines]
    assert all(re.fullmatch(r"[0-9a-fA-F]{40}", fields[0]) for fields in entries)
    assert all(all(fields[1:]) for fields in entries)
    for _, *paths in entries:
        assert all(_is_normalized_prerequisite_path(path) for path in paths)
    shas = [fields[0] for fields in entries]
    assert len(shas) == len(set(shas)), "prerequisite commits must be unique"


@pytest.mark.parametrize(
    "path",
    [
        "src/file.txt",
        "src/file with spaces.txt",
        "src/with  repeated internal spaces.txt",
        ".pipelines/release-compat-prerequisites.txt",
        "src/.hidden/file",
        "src/.../file",
    ],
)
def test_release_compat_prerequisite_path_normalization_accepts_valid_paths(path):
    assert _is_normalized_prerequisite_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".",
        "./src/file",
        "/src/file",
        "src/../file",
        "src/..",
        "src/./file",
        "src/.",
        "src//file",
        "src/file/",
        " src/file",
        "src/file ",
        "\tsrc/file",
        "src/file\t",
        "\rsrc/file",
        "src/file\r",
        "\nsrc/file",
        "src/file\n",
    ],
)
def test_release_compat_prerequisite_path_normalization_rejects_invalid_paths(path):
    assert not _is_normalized_prerequisite_path(path)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_replays_prerequisite_before_pr_patch():
    scratch_root = REPO_ROOT / "target" / f"release-compat-replay-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

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
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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


def _run_release_compat_with_scoped_config(scope):
    scratch_root = (
        REPO_ROOT / "target" / f"release-compat-invalid-path-{uuid.uuid4().hex}"
    )
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        pr_file = repo / "src" / "pr.txt"
        pr_file.parent.mkdir()
        pr_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        _git(repo, "commit", "--allow-empty", "-m", "prerequisite marker")
        prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_bytes(f"{prerequisite}\t{scope}".encode())
        pr_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature with invalid prerequisite path")

        target = _git(repo, "rev-parse", "master").stdout.strip()
        source = _git(repo, "rev-parse", "source").stdout.strip()
        source_tree = _git(repo, "rev-parse", f"{source}^{{tree}}").stdout.strip()
        merge_commit = _git(
            repo,
            "commit-tree",
            source_tree,
            "-p",
            target,
            "-p",
            source,
            "-m",
            "merge feature",
        ).stdout.strip()
        _git(repo, "checkout", "--detach", merge_commit)
        _assert_git_clean(repo, "synthetic PR merge checkout")

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

        return result
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
@pytest.mark.parametrize(
    "invalid_path",
    [
        "src/./scoped.txt",
        "src//scoped.txt",
        "src/scoped.txt/",
        "src/scoped.txt/.",
    ],
    ids=["dot-segment", "repeated-slash", "trailing-slash", "terminal-dot"],
)
def test_release_compat_rejects_non_normalized_scoped_paths(invalid_path):
    result = _run_release_compat_with_scoped_config(f"{invalid_path}\n")

    assert result.returncode != 0
    assert (
        f"Scoped prerequisite path must be normalized: {invalid_path}" in result.stdout
    )


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
@pytest.mark.parametrize(
    ("scope", "expected_error"),
    [
        (
            " src/scoped.txt\n",
            "Scoped prerequisite path must not have leading or trailing whitespace",
        ),
        (
            "src/scoped.txt \n",
            "Scoped prerequisite path must not have leading or trailing whitespace",
        ),
        (
            "src/scoped.txt \r\n",
            "Scoped prerequisite path must not have leading or trailing whitespace",
        ),
        (
            "src/scoped.txt \tsrc/other.txt\r\n",
            "Scoped prerequisite path must not have leading or trailing whitespace",
        ),
        (
            "src/scoped.txt\t src/other.txt\r\n",
            "Scoped prerequisite path must not have leading or trailing whitespace",
        ),
        ("\tsrc/scoped.txt\r\n", "contains an empty path"),
        ("src/scoped.txt\t\r\n", "contains an empty path"),
    ],
    ids=[
        "leading-space",
        "trailing-space",
        "trailing-space-crlf",
        "first-path-trailing-space",
        "second-path-leading-space",
        "leading-tab",
        "trailing-tab-crlf",
    ],
)
def test_release_compat_rejects_scoped_path_field_whitespace(scope, expected_error):
    result = _run_release_compat_with_scoped_config(scope)

    assert result.returncode != 0
    assert expected_error in result.stdout


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
@pytest.mark.parametrize(
    ("config_prefix", "scoped_path", "config_suffix"),
    [
        ("", "src/scoped.txt", "\n"),
        ("", "src/scoped.txt", "\r\n"),
        ("  ", "src/scoped.txt", "\r\n"),
        ("", "src/scoped file.txt", "\n"),
    ],
    ids=["normalized", "crlf", "leading-sha-whitespace", "internal-space"],
)
def test_release_compat_replays_scoped_missing_file_prerequisite_full_overlap(
    config_prefix, scoped_path, config_suffix
):
    scratch_root = REPO_ROOT / "target" / f"release-compat-scoped-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        base_file = repo / "base.txt"
        base_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        scoped_file = repo / scoped_path
        unrelated_file = repo / "src" / "unrelated.txt"
        scoped_file.parent.mkdir()
        scoped_file.write_text("target baseline\n")
        unrelated_file.write_text("must not be replayed\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "prerequisite adds target files")
        prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_bytes(
            (f"{config_prefix}{prerequisite}\t{scoped_path}{config_suffix}").encode()
        )
        scoped_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature modifies scoped file")

        _git(repo, "checkout", "master")
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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

        assert result.returncode == 0, (
            f"scoped release replay failed\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        assert scoped_file.read_text() == "pull request change\n"
        assert not unrelated_file.exists()
        assert _git(repo, "diff", "--cached", "--name-only").stdout.splitlines() == [
            scoped_path
        ]
        assert f"Prerequisite {prerequisite} applies cleanly" in result.stdout
        assert "PR changes apply cleanly onto release" in result.stdout
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_skips_scoped_prerequisite_for_unrelated_pr_path():
    scratch_root = REPO_ROOT / "target" / f"release-compat-unrelated-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        pr_file = repo / "src" / "pr.txt"
        pr_file.parent.mkdir()
        pr_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        scoped_file = repo / "src" / "scoped.txt"
        _git(repo, "commit", "--allow-empty", "-m", "prerequisite marker")
        prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_text(f"{prerequisite}\tsrc/scoped.txt\n")
        pr_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature modifies unrelated file")

        _git(repo, "checkout", "master")
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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

        assert result.returncode == 0, (
            f"unrelated-path release replay failed\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        assert pr_file.read_text() == "pull request change\n"
        assert not scoped_file.exists()
        assert _git(repo, "diff", "--cached", "--name-only").stdout.splitlines() == [
            "src/pr.txt"
        ]
        assert (
            f"Scoped prerequisite {prerequisite} has no paths in this PR replay; skipping"
            in result.stdout
        )
        assert f"Prerequisite {prerequisite} applies cleanly" not in result.stdout
        assert "PR changes apply cleanly onto release" in result.stdout
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_replays_scoped_baseline_for_later_prerequisite():
    scratch_root = REPO_ROOT / "target" / f"release-compat-dependent-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        pr_file = repo / "src" / "pr.txt"
        pr_file.parent.mkdir()
        pr_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        scoped_file = repo / "src" / "scoped.txt"
        scoped_file.write_text("target baseline\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "add scoped baseline")
        scoped_prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        scoped_file.write_text("dependent prerequisite\n")
        _git(repo, "commit", "-am", "modify scoped baseline")
        dependent_prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_text(
            f"{scoped_prerequisite}\tsrc/scoped.txt\n{dependent_prerequisite}\n"
        )
        pr_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature modifies unrelated file")

        _git(repo, "checkout", "master")
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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

        assert result.returncode == 0, (
            f"dependent release replay failed\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        assert pr_file.read_text() == "pull request change\n"
        assert scoped_file.read_text() == "dependent prerequisite\n"
        assert _git(repo, "diff", "--cached", "--name-only").stdout.splitlines() == [
            "src/pr.txt",
            "src/scoped.txt",
        ]
        assert (
            f"Scoped prerequisite {scoped_prerequisite} path src/scoped.txt "
            f"is required by later prerequisite {dependent_prerequisite}"
            in result.stdout
        )
        assert "PR changes apply cleanly onto release" in result.stdout
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.parametrize("modify_scoped_path", [False, True])
@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_reports_dependent_prerequisite_without_parent(
    modify_scoped_path,
):
    scratch_root = (
        REPO_ROOT
        / "target"
        / (f"release-compat-parent-{modify_scoped_path}-{uuid.uuid4().hex}")
    )
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        base_file = repo / "src" / "base.txt"
        base_file.parent.mkdir()
        base_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "root base")
        root_prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", root_prerequisite)

        scoped_file = repo / "src" / "scoped.txt"
        scoped_file.write_text("scoped baseline\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "add scoped baseline")
        scoped_prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_text(
            f"{scoped_prerequisite}\tsrc/scoped.txt\n{root_prerequisite}\n"
        )
        if modify_scoped_path:
            scoped_file.write_text("pull request change\n")
        else:
            pr_file = repo / "src" / "pr.txt"
            pr_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature change")

        _git(repo, "checkout", "master")
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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

        assert result.returncode == 1
        assert (
            "##vso[task.logissue type=error]Dependent release compatibility "
            f"prerequisite {root_prerequisite} has no first parent" in result.stdout
        )
        assert "fatal:" not in result.stderr
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.skipif(os.name != "posix", reason="release replay script requires Bash")
def test_release_compat_replays_only_partial_scoped_path_intersection():
    scratch_root = REPO_ROOT / "target" / f"release-compat-partial-{uuid.uuid4().hex}"
    repo = scratch_root / "repo"
    origin = scratch_root / "origin.git"
    agent_temp = scratch_root / "agent"

    try:
        repo.mkdir(parents=True)
        agent_temp.mkdir()
        _init_release_compat_scratch_repo(repo)

        base_file = repo / "base.txt"
        base_file.write_text("release base\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        base = _git(repo, "rev-parse", "HEAD").stdout.strip()
        _git(repo, "branch", "release", base)

        intersecting_file = repo / "src" / "intersecting.txt"
        nonintersecting_file = repo / "src" / "nonintersecting.txt"
        intersecting_file.parent.mkdir()
        intersecting_file.write_text("target baseline\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "prerequisite adds intersecting file")
        prerequisite = _git(repo, "rev-parse", "HEAD").stdout.strip()

        _git(repo, "checkout", "-b", "source")
        prerequisite_config = repo / ".pipelines" / "release-compat-prerequisites.txt"
        prerequisite_config.parent.mkdir()
        prerequisite_config.write_text(
            f"{prerequisite}\tsrc/intersecting.txt\tsrc/nonintersecting.txt\n"
        )
        intersecting_file.write_text("pull request change\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "feature modifies one scoped file")

        _git(repo, "checkout", "master")
        _assert_git_clean(repo, "checkout before synthetic PR merge")
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

        assert result.returncode == 0, (
            f"partial scoped release replay failed\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        assert intersecting_file.read_text() == "pull request change\n"
        assert not nonintersecting_file.exists()
        assert _git(repo, "diff", "--cached", "--name-only").stdout.splitlines() == [
            "src/intersecting.txt"
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


def test_build_docker_reuses_bootstrap_layers_and_emits_progress():
    data = yaml.safe_load(_pipeline_text())
    jobs = {j.get("job"): j for j in _jobs(data["jobs"])}
    image_builds = [
        step
        for step in jobs["BuildDocker"]["steps"]
        if step.get("displayName") in {"Demo Image Build", "Minimal Image Build"}
    ]

    assert len(image_builds) == 2
    for step in image_builds:
        assert "--quiet" not in step["inputs"]["arguments"]
        assert (
            "--build-arg PYTHON_VERSION=$(pythonVersion)" in step["inputs"]["arguments"]
        )
        assert step["env"]["DOCKER_BUILDKIT"] == "1"
        assert step["env"]["BUILDKIT_PROGRESS"] == "plain"

    version_step = next(
        step
        for step in jobs["BuildDocker"]["steps"]
        if step.get("displayName") == "Get Docker Tag + Version"
    )
    assert "tools/ci/get_python_version.sh" in version_step["bash"]
    assert "variable=pythonVersion" in version_step["bash"]

    dependency_marker = "# SYNAPSEML_BOOTSTRAP_END"
    demo_bootstrap, separator, _ = DEMO_DOCKERFILE.read_text().partition(
        dependency_marker
    )
    assert separator
    minimal_bootstrap, separator, _ = MINIMAL_DOCKERFILE.read_text().partition(
        dependency_marker
    )
    assert separator
    assert demo_bootstrap == minimal_bootstrap
    assert "pip install --no-cache-dir" in DEMO_DOCKERFILE.read_text()
    assert "pip install --no-cache-dir" in MINIMAL_DOCKERFILE.read_text()
    assert 'conda install -y "python=${PYTHON_VERSION}"' in DEMO_DOCKERFILE.read_text()
    assert (
        'conda install -y "python=${PYTHON_VERSION}"' in MINIMAL_DOCKERFILE.read_text()
    )
    assert "PYTHON_VERSION build argument is required" in DEMO_DOCKERFILE.read_text()
    assert "PYTHON_VERSION build argument is required" in MINIMAL_DOCKERFILE.read_text()


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
