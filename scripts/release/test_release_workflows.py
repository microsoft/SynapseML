# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"


def read_workflow(name):
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_release_notes_is_manual_and_artifact_gated():
    workflow = read_workflow("release-notes.yml")
    trigger = workflow.split("permissions:", 1)[0]
    assert "\n  workflow_dispatch:" in trigger
    assert "\n  push:" not in trigger
    assert "--skip" not in workflow
    assert "--plan" in workflow
    assert "release_guard.py notes" in workflow
    assert "APPROVE_PLAN" in workflow
    assert "RELEASE_EVIDENCE_BASE64" in workflow
    assert "--evidence-base64-env" in workflow
    assert "--inventory-only" in workflow
    assert "--targets master" not in workflow
    assert "python3 scripts/release/verify_release.py" in workflow
    assert 'target_commitish="$TAG"' in workflow
    assert "release_guard.py verify-primary-integration" in workflow
    assert workflow.index("verify-primary-integration") < workflow.index(
        "release_guard.py notes"
    )
    assert "pull-requests: read" in workflow
    assert "--installation-output" in workflow
    assert 'cat "$RUNNER_TEMP/release-installation.md"' in workflow
    assert 'echo "| 4.0' not in workflow


def test_optional_target_requires_per_dispatch_opt_in_not_repository_variables():
    workflow = yaml.safe_load(read_workflow("release-tag.yml"))
    inputs = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]
    assert inputs["include_spark40"]["default"] is False
    assert workflow["env"]["INCLUDE_SPARK40"] == "${{ inputs.include_spark40 == true }}"
    assert "vars.INCLUDE_SPARK40" not in read_workflow("release-tag.yml")
    guard = next(
        step
        for step in workflow["jobs"]["release-tags"]["steps"]
        if step.get("name") == "Validate the full release policy before creating tags"
    )
    assert '--include-spark40 "$INCLUDE_SPARK40"' in guard["run"]


def test_unpublished_docs_preview_cannot_deploy_pages():
    workflow = yaml.safe_load(read_workflow("website-deploy.yml"))
    build = next(
        step
        for step in workflow["jobs"]["build"]["steps"]
        if step.get("name") == "Install and build website"
    )
    assert build["env"]["SYNAPSEML_DOCS_PREVIEW"] == (
        "${{ github.event_name == 'pull_request' || github.ref != 'refs/heads/master' }}"
    )
    assert "npm test" in build["run"]
    deploy = workflow["jobs"]["deploy"]
    assert deploy["if"] == (
        "github.ref == 'refs/heads/master' && github.event_name != 'pull_request'"
    )
    assert deploy["needs"] == "build"


def test_release_prepare_tags_merged_commit_and_dispatches_orchestrator():
    workflow = read_workflow("release-prepare.yml")
    assert "\n  pull_request:" in workflow
    assert "github.event.pull_request.merge_commit_sha" in workflow
    assert "website/versioned_docs/version-${VERSION}" in workflow
    assert 'git tag "$TAG" "$MERGED_SHA"' in workflow
    assert 'gh workflow run release-tag.yml --ref "v${VERSION}"' in workflow
    assert "gh workflow run release-notes.yml" not in workflow
    assert (
        "curl --fail --show-error --location --retry 3 --retry-all-errors" in workflow
    )


def test_generated_release_pr_receives_dispatched_validation():
    prepare = read_workflow("release-prepare.yml")
    validation = read_workflow("pr-validation.yml")
    assert "gh workflow run pr-validation.yml" in prepare
    assert "gh workflow run website-deploy.yml" in prepare
    assert "\n  workflow_dispatch:" in validation.split("jobs:", 1)[0]


def test_full_release_policy_precedes_primary_and_derivative_tags():
    prepare = read_workflow("release-prepare.yml")
    finalize = prepare.split("  finalize:", 1)[1]
    assert finalize.index("release_guard.py full-release") < finalize.index(
        'git tag "$TAG"'
    )
    tags = read_workflow("release-tag.yml")
    assert tags.index("release_guard.py full-release") < tags.index('git tag "$TAG"')
    assert 'release_guard.py push-tags --repo . "${TO_PUSH[@]}"' in tags
    assert 'release_guard.py push-tags --repo . "${TO_PUSH[@]}"' in read_workflow(
        "release-tag-spark.yml"
    )
    assert "outside the branch filters" not in tags


def test_port_tag_authorization_uses_a_same_repository_merge_not_a_fork_or_tag_alone():
    workflow = read_workflow("release-tag-spark.yml")
    job = yaml.safe_load(workflow)["jobs"]["create-spark-tags"]
    assert "github.event.pull_request.merged == true" in job["if"]
    assert (
        "github.event.pull_request.head.repo.full_name == github.repository"
        in job["if"]
    )
    checkout = next(step for step in job["steps"] if step.get("name") == "Checkout")
    assert (
        checkout["with"]["ref"] == "${{ github.event.pull_request.merge_commit_sha }}"
    )
    tag_step = next(
        step for step in job["steps"] if step.get("name") == "Create and push tags"
    )
    assert (
        tag_step["env"]["MERGED_SHA"]
        == "${{ github.event.pull_request.merge_commit_sha }}"
    )
    assert '--commit "$MERGED_SHA"' in tag_step["run"]
    assert "publishRelease" not in workflow
    assert "resume --apply" not in workflow


def test_prepared_release_pr_does_not_make_internal_a_public_release_prerequisite():
    workflow = yaml.safe_load(read_workflow("release-prepare.yml"))
    step = next(
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("name") == "Open release PR"
    )
    public, optional = step["run"].split("### Optional later tracks", maxsplit=1)
    assert "Publish notes" in public
    assert "Internal release PRs" not in public
    assert "Downstream private integrations" in optional
    assert "not prerequisites for OSS publication or release notes" in optional
