# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from tools.ci.e2e_impact import (
    ALL_SUITES,
    GOVERNANCE_FILES,
    MODULES,
    OUTPUTS,
    ALL_JOBS,
    UNIT_PACKAGES,
    changed_paths,
    jobs_for_path,
    required_jobs,
    required_suites,
    selection_outputs,
    select_suites,
    suites_for_path,
)


ROOT = Path(__file__).resolve().parents[3]
RUNTIME_PATH = "core/src/main/scala/example/Runtime.scala"
PYTHON_TEST = "core/src/test/python/synapsemltest/test_example.py"


@pytest.mark.parametrize("path", sorted(GOVERNANCE_FILES))
def test_governance_files_do_not_change_runtime_inputs(path):
    assert suites_for_path(path) == frozenset()


@pytest.mark.parametrize("module", MODULES)
@pytest.mark.parametrize(
    "suffix", ["test_example.py", "helpers/data.json", "conftest.py"]
)
def test_python_test_inputs_do_not_change_notebook_inputs(module, suffix):
    assert suites_for_path(f"{module}/src/test/python/{suffix}") == frozenset()


@pytest.mark.parametrize("module", MODULES)
def test_r_test_inputs_do_not_change_notebook_inputs(module):
    assert (
        suites_for_path(f"{module}/src/test/R/testthat/test-example.R") == frozenset()
    )


@pytest.mark.parametrize(
    "path",
    [
        "website/doctest.py",
        "website/src/pages/index.js",
        "website/package-lock.json",
        "docs/Quick Examples/transformers/cognitive/_Translator.md",
    ],
)
def test_website_inputs_do_not_change_notebook_inputs(path):
    assert suites_for_path(path) == frozenset()


@pytest.mark.parametrize(
    "path",
    [
        ".github/skills/example/SKILL.md",
        ".agents/skills/README.md",
        "reviews/round-1.md",
    ],
)
def test_agent_guidance_and_review_records_are_not_runtime_inputs(path):
    assert suites_for_path(path) == frozenset()


def test_r_runner_is_only_consumed_by_r_tests():
    assert suites_for_path("tools/tests/run_r_tests.R") == frozenset()


@pytest.mark.parametrize(
    "path",
    [
        RUNTIME_PATH,
        "opencv/src/main/scala/example/Image.scala",
        "lightgbm/src/main/python/synapse/ml/lightgbm/LightGBMClassifier.py",
        "cognitive/src/test/scala/example/ServiceSuite.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/TestGen.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/NewHelper.scala",
        "core/src/test/resources/README.md",
        "docs/Explore Algorithms/Deep Learning/ONNX.md",
        "docs/Explore Algorithms/Deep Learning/Fine-tune.ipynb",
        "docs/Quick Examples/example.ipynb",
        "docs/Quick Examples/data.json",
        "docs/Reference/Developer Setup.md",
        "docs/data/model.json",
        "environment.yml",
        "environment.dev.yml",
        "build.sbt",
        "project/CodegenPlugin.scala",
        "templates/sbt_cache.yml",
        "pipeline.yaml",
        ".github/workflows/pr-validation.yml",
        ".github/skills/example/scripts/run.py",
        ".pipelines/clean-acr.yml",
        "tools/ci/e2e_impact.py",
        "tools/ci/tests/test_e2e_impact.py",
        "tools/docker/demo/Dockerfile",
        "tools/pytest/run_all_tests.py",
        "new-module/src/test/python/test_example.py",
        "core/src/test/r/test-example.R",
        "README.md.scala",
        "reviews/not-a-review.py",
        "unknown.md",
    ],
)
def test_unknown_shared_and_runtime_inputs_keep_every_family(path):
    assert suites_for_path(path) == ALL_SUITES


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/README.md",
        "./README.md",
        "../README.md",
        "website/../build.sbt",
        "website//README.md",
        "website\\README.md",
        "C:/README.md",
        "website/",
        "website/\n##vso[task.setvariable variable=x]false.md",
        "website/\N{SNOWMAN}.md",
    ],
)
def test_ambiguous_paths_keep_every_family(path):
    assert suites_for_path(path) == ALL_SUITES


def test_empty_and_mixed_changes_cannot_hide_impact():
    assert required_suites([]) == ALL_SUITES
    assert required_suites(["README.md", PYTHON_TEST]) == frozenset()
    assert (
        required_suites(
            [PYTHON_TEST, "tools/tests/run_r_tests.R", "website/doctest.py"]
        )
        == frozenset()
    )
    assert required_suites(["README.md", RUNTIME_PATH]) == ALL_SUITES
    for path in ("README.md", PYTHON_TEST, "website/doctest.py"):
        assert required_suites([path, "unknown"]) == ALL_SUITES


def git(repo, *args, input=None):
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        input=input,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def make_pr(tmp_path):
    def create(changes):
        repo = tmp_path / "repo"
        repo.mkdir()
        git(repo, "init", "--initial-branch=master")
        for key, value in (
            ("user.name", "CI selection test"),
            ("user.email", "ci@example.test"),
            ("core.autocrlf", "false"),
            ("core.hooksPath", str(tmp_path / "disabled-hooks")),
            ("commit.gpgsign", "false"),
        ):
            git(repo, "config", key, value)
        for path in ("README.md", RUNTIME_PATH, PYTHON_TEST):
            file = repo / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("baseline\n")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "baseline")
        git(repo, "checkout", "-b", "source")
        for path, text in changes.items():
            file = repo / path
            if text is None:
                file.unlink()
            else:
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(text)
        git(repo, "add", ".")
        git(repo, "commit", "--allow-empty", "-m", "source")
        source = git(repo, "rev-parse", "HEAD")
        git(repo, "checkout", "-b", "queued-merge", "master")
        git(repo, "merge", "--no-ff", "source", "-m", "queued PR merge")
        env = {
            "BUILD_REASON": "PullRequest",
            "BUILD_SOURCEBRANCH": "refs/pull/123/merge",
            "BUILD_SOURCEVERSION": git(repo, "rev-parse", "HEAD"),
            "SYSTEM_PULLREQUEST_SOURCECOMMITID": source,
            "SYNAPSEML_FULL_TESTS": "false",
        }
        return repo, env

    return create


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"README.md": "changed\n"}, frozenset()),
        ({PYTHON_TEST: "changed\n"}, frozenset()),
        ({PYTHON_TEST: None}, frozenset()),
        ({"website/space in name.md": "changed\n"}, frozenset()),
        ({RUNTIME_PATH: "changed\n"}, ALL_SUITES),
        ({RUNTIME_PATH: None, "reviews/moved.md": "baseline\n"}, ALL_SUITES),
        ({}, ALL_SUITES),
    ],
)
def test_real_merge_add_modify_delete_rename_and_empty_diff(make_pr, changes, expected):
    repo, env = make_pr(changes)
    assert select_suites(repo, env) == expected


def test_target_advancement_cannot_erase_the_queued_runtime_change(make_pr):
    repo, env = make_pr({RUNTIME_PATH: "changed\n"})
    git(repo, "checkout", "master")
    git(repo, "merge", "--ff-only", "source")
    git(repo, "checkout", "queued-merge")
    assert git(repo, "diff", "--name-only", "master", "HEAD") == ""
    assert select_suites(repo, env) == ALL_SUITES
    assert changed_paths(repo, env) == [RUNTIME_PATH]


@pytest.mark.parametrize("depth,expected", [(1, ALL_SUITES), (2, frozenset())])
def test_shallow_history_fails_open_unless_both_parents_are_available(
    make_pr, tmp_path, depth, expected
):
    repo, env = make_pr({PYTHON_TEST: "changed\n"})
    clone = tmp_path / "clone"
    git(tmp_path, "clone", "--depth", str(depth), repo.as_uri(), str(clone))
    assert select_suites(clone, env) == expected


@pytest.mark.parametrize("mode", ["120000", "160000"])
def test_symlinks_and_gitlinks_under_safe_paths_cannot_skip_tests(make_pr, mode):
    repo, env = make_pr({"README.md": "changed\n"})
    git(repo, "checkout", "source")
    oid = (
        git(repo, "hash-object", "-w", "--stdin", input="outside")
        if mode == "120000"
        else git(repo, "rev-parse", "HEAD")
    )
    git(repo, "update-index", "--add", "--cacheinfo", f"{mode},{oid},README.md")
    git(repo, "commit", "-m", "change type")
    env["SYSTEM_PULLREQUEST_SOURCECOMMITID"] = git(repo, "rev-parse", "HEAD")
    merge = git(
        repo,
        "commit-tree",
        "HEAD^{tree}",
        "-p",
        "master",
        "-p",
        "HEAD",
        "-m",
        "type merge",
    )
    git(repo, "update-ref", "HEAD", merge)
    env["BUILD_SOURCEVERSION"] = merge
    assert select_suites(repo, env) == ALL_SUITES


@pytest.mark.parametrize(
    "reason", ["Schedule", "Manual", "IndividualCI", "BatchedCI", ""]
)
def test_non_pr_runs_do_not_even_consult_git(tmp_path, reason):
    assert (
        select_suites(
            tmp_path, {"BUILD_REASON": reason, "SYNAPSEML_FULL_TESTS": "false"}
        )
        == ALL_SUITES
    )


@pytest.mark.parametrize("override", ["true", "True", "", "yes", "$(fullTests)"])
def test_forced_or_unknown_full_test_option_runs_everything(tmp_path, override):
    assert (
        select_suites(
            tmp_path,
            {"BUILD_REASON": "PullRequest", "SYNAPSEML_FULL_TESTS": override},
        )
        == ALL_SUITES
    )


@pytest.mark.parametrize(
    "key,value",
    [
        ("BUILD_SOURCEBRANCH", "refs/heads/source"),
        ("BUILD_SOURCEBRANCH", "refs/pull/123/head"),
        ("BUILD_SOURCEVERSION", "0" * 40),
        ("SYSTEM_PULLREQUEST_SOURCECOMMITID", ""),
        ("SYSTEM_PULLREQUEST_SOURCECOMMITID", "0" * 40),
    ],
)
def test_incomplete_or_mismatched_pr_metadata_runs_everything(make_pr, key, value):
    repo, env = make_pr({"README.md": "changed\n"})
    env[key] = value
    assert select_suites(repo, env) == ALL_SUITES


def test_source_checkout_cannot_impersonate_a_merge(make_pr):
    repo, env = make_pr({"README.md": "changed\n"})
    git(repo, "checkout", "source")
    env["BUILD_SOURCEVERSION"] = git(repo, "rev-parse", "HEAD")
    assert select_suites(repo, env) == ALL_SUITES


def test_git_failure_is_visible_and_runs_everything(make_pr, tmp_path, capsys):
    _, env = make_pr({"README.md": "changed\n"})
    assert select_suites(tmp_path, env) == ALL_SUITES
    diagnostics = capsys.readouterr().err
    assert "task.logissue type=warning" in diagnostics
    assert "not a git repository" in diagnostics


@pytest.mark.parametrize(
    "error",
    [
        OSError("git unavailable"),
        subprocess.TimeoutExpired("git", 60),
        ValueError("malformed diff"),
        UnicodeDecodeError("utf8", b"\xff", 0, 1, "invalid path"),
    ],
)
def test_detection_errors_cannot_emit_skip_decisions(monkeypatch, tmp_path, error):
    def fail(*args):
        raise error

    monkeypatch.setattr("tools.ci.e2e_impact.changed_paths", fail)
    assert (
        select_suites(
            tmp_path, {"BUILD_REASON": "PullRequest", "SYNAPSEML_FULL_TESTS": "false"}
        )
        == ALL_SUITES
    )


@pytest.mark.parametrize("reason", ["PullRequest", "Schedule", "Manual"])
def test_cli_emits_exact_complete_output_contract(make_pr, reason):
    repo, env = make_pr({PYTHON_TEST: "changed\n"})
    env["BUILD_REASON"] = reason
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "ci" / "e2e_impact.py")],
        cwd=repo,
        env={**os.environ, **env},
        check=True,
        capture_output=True,
        text=True,
    )
    expected = selection_outputs([PYTHON_TEST] if reason == "PullRequest" else [])
    assert result.stdout.splitlines() == [
        line
        for variable, selected in expected.items()
        for line in (
            f"{variable}={'true' if selected else 'false'}",
            f"##vso[task.setvariable variable={variable};isOutput=true]"
            f"{'true' if selected else 'false'}",
        )
    ]


def test_pipeline_gates_only_audited_families_and_keeps_full_schedule():
    pipeline = yaml.safe_load((ROOT / "pipeline.yaml").read_text())
    jobs = {job["job"]: job for job in pipeline["jobs"] if "job" in job}
    job_suites = {
        "DatabricksCPUE2E": "databricks_cpu",
        "DatabricksGPUE2E": "databricks_gpu",
        "FabricE2E": "fabric",
    }
    assert set(job_suites.values()) == ALL_SUITES
    for job, suite in job_suites.items():
        condition = jobs[job]["condition"]
        assert (
            "ne(dependencies.BuildAndCacheSbt.outputs"
            f"['detectTestImpact.{OUTPUTS[suite]}'], 'false')"
        ) in condition
        assert "succeeded()" in condition
        assert jobs[job]["dependsOn"] == "BuildAndCacheSbt"
    for job in ("CIHelpers", "Style"):
        assert "detectTestImpact" not in jobs[job].get("condition", "")
    prewarm = jobs["BuildAndCacheSbt"]["steps"]
    assert prewarm[0]["fetchDepth"] >= 2
    assert prewarm[1]["name"] == "detectTestImpact"
    assert prewarm[1]["env"] == {"SYNAPSEML_FULL_TESTS": "${{ parameters.fullTests }}"}
    helpers = jobs["CIHelpers"]
    assert "dependsOn" not in helpers
    assert "condition" not in helpers
    assert helpers["steps"][0]["retryCountOnTaskFailure"] == 2
    assert "python3 -m pytest tools/ci/tests/ -q" in helpers["steps"][1]["bash"]
    assert pipeline["schedules"] == [
        {
            "cron": "0 0 * * *",
            "displayName": "Daily midnight build",
            "always": True,
            "branches": {"include": ["master"]},
        }
    ]
    parameters = {parameter["name"]: parameter for parameter in pipeline["parameters"]}
    assert parameters["fullTests"]["default"] is False
    for name in (
        "testUnit",
        "testPython",
        "testR",
        "testDatabricksE2E",
        "testFabricE2E",
        "testWebsiteSamples",
    ):
        assert parameters[name]["default"] is True


def test_selected_coverage_finishes_after_all_uploaders_and_carries_unaffected_flags():
    pipeline = yaml.safe_load((ROOT / "pipeline.yaml").read_text())
    codecov = yaml.safe_load((ROOT / "codecov.yaml").read_text())
    coverage_jobs = []
    for job in pipeline["jobs"]:
        if job.get("job") != "CoverageComplete" and any(
            step.get("template") == "templates/codecov.yml"
            or any(
                isinstance(value, list)
                and any(
                    item.get("template") == "templates/codecov.yml" for item in value
                )
                for value in step.values()
            )
            for step in job.get("steps", [])
        ):
            coverage_jobs.append(job)
    assert {job["job"] for job in coverage_jobs} == {
        "UnitTests",
        "PythonTests",
        "RTests",
        "WebsiteSamplesTests",
    }
    uploads = sum(
        len(job.get("strategy", {}).get("matrix", {"single": {}}))
        for job in coverage_jobs
    )
    assert uploads == 55
    assert "after_n_builds" not in codecov["codecov"]["notify"]
    assert "after_n_builds" not in codecov["comment"]
    assert codecov["codecov"]["notify"]["manual_trigger"] is True
    assert codecov["flag_management"]["default_rules"]["carryforward"] is True
    finalize = next(
        job for job in pipeline["jobs"] if job.get("job") == "CoverageComplete"
    )
    assert {job["job"] for job in coverage_jobs} <= set(finalize["dependsOn"])
    assert finalize["steps"][-1]["parameters"]["notifyOnly"] is True
    assert "not(canceled())" in finalize["condition"]
    template = yaml.safe_load((ROOT / "templates" / "codecov.yml").read_text())
    script = template["steps"][0]["bash"]
    assert '--flag "$flag"' in script
    for job in coverage_jobs:
        for step in job["steps"]:
            for value in step.values():
                if isinstance(value, list):
                    for template in value:
                        if template.get("template") == "templates/codecov.yml":
                            assert template["parameters"]["coverageFlag"]
    assert "send-notifications" in script


@pytest.mark.parametrize(
    "path,expected",
    [
        ("README.md", set()),
        ("website/doctest.py", {"website"}),
        ("lightgbm/src/test/python/test_model.py", {"python_lightgbm"}),
        ("vw/src/test/R/test_model.R", {"r_vw"}),
        (
            "cognitive/src/main/scala/Service.scala",
            {
                "unit_cognitive",
                "python_cognitive",
                "r_cognitive",
                "website",
                "docker",
                "internal",
            },
        ),
        (
            "opencv/src/test/scala/ImageSuite.scala",
            {
                f"{language}_{module}"
                for language in ("unit", "python", "r")
                for module in ("opencv", "deep-learning")
            },
        ),
        ("core/src/test/python/shared.py", {f"python_{module}" for module in MODULES}),
        ("core/src/test/scala/Shared.scala", ALL_JOBS),
        ("core/src/main/scala/Shared.scala", ALL_JOBS),
        ("cognitive/src/test/resources/shared.json", ALL_JOBS),
        ("lightgbm/src/main/resources/model.json", ALL_JOBS),
        ("pipeline.yaml", ALL_JOBS),
        ("project/CodegenPlugin.scala", ALL_JOBS),
        ("environment.yml", ALL_JOBS),
        ("codecov.yaml", ALL_JOBS),
        ("tools/ci/e2e_impact.py", ALL_JOBS),
        ("new-module/src/main/scala/Stage.scala", ALL_JOBS),
        ("website/../build.sbt", ALL_JOBS),
    ],
)
def test_job_selection_tracks_dependencies_and_shared_inputs(path, expected):
    assert jobs_for_path(path) == expected


def test_mixed_and_empty_job_selection_cannot_hide_changes():
    assert required_jobs([]) == ALL_JOBS
    assert required_jobs(["README.md", "unknown"]) == ALL_JOBS
    assert required_jobs(
        ["vw/src/test/R/Test.R", "lightgbm/src/test/python/test.py"]
    ) == {"r_vw", "python_lightgbm"}


def test_matrix_keys_all_resolve_and_unknown_keys_cannot_skip():
    pipeline = yaml.safe_load((ROOT / "pipeline.yaml").read_text())
    jobs = {job["job"]: job for job in pipeline["jobs"] if "job" in job}
    outputs = selection_outputs(["lightgbm/src/main/scala/Model.scala"])
    for job, language in (
        ("UnitTests", "unit"),
        ("PythonTests", "python"),
        ("RTests", "r"),
    ):
        assert (
            f"format('detectTestImpact.{language}_{{0}}', variables['PACKAGE'])"
            in jobs[job]["condition"]
        )
        assert "], 'false')" in jobs[job]["condition"]
        for lane in jobs[job]["strategy"]["matrix"].values():
            assert f"{language}_{lane['PACKAGE']}" in outputs
    declared = {
        package for packages in UNIT_PACKAGES.values() for package in packages
    } | {"misc"}
    assert declared == {
        lane["PACKAGE"] for lane in jobs["UnitTests"]["strategy"]["matrix"].values()
    }
    assert outputs["unit_misc"] is True
    assert outputs["unit_core"] is True
    assert outputs["unit_lightgbm.split1"] is True
    assert outputs["unit_services.openai"] is False
    assert outputs["python_deepLearning"] is False
    assert selection_outputs([PYTHON_TEST])["unit_misc"] is False


def test_publish_is_unfiltered_and_extensions_summary_follows_publication():
    pipeline = yaml.safe_load((ROOT / "pipeline.yaml").read_text())
    publish = next(
        job
        for block in pipeline["jobs"]
        for jobs in block.values()
        if isinstance(jobs, list)
        for job in jobs
        if job.get("job") == "Publish"
    )
    assert "detectTestImpact" not in publish["condition"]
    script = next(
        step["inputs"]["inlineScript"]
        for step in publish["steps"]
        if step.get("displayName") == "Publish Artifacts"
    )
    assert script.index("publishPython") < script.index("write_artifact_summary.py")
    assert script.index("write_artifact_summary.py") < script.index(
        "task.uploadsummary"
    )


def test_live_site_scan_is_scheduled_and_python_style_stays_in_azure():
    links = yaml.load(
        (ROOT / ".github/workflows/check-dead-links.yml").read_text(),
        Loader=yaml.BaseLoader,
    )
    assert "pull_request" not in links["on"]
    assert "schedule" in links["on"]
    actions = yaml.safe_load((ROOT / ".github/workflows/pr-validation.yml").read_text())
    assert "python-style" not in actions["jobs"]
    pipeline = yaml.safe_load((ROOT / "pipeline.yaml").read_text())
    style = next(job for job in pipeline["jobs"] if job.get("job") == "Style")
    assert any("black --check" in step.get("bash", "") for step in style["steps"])
