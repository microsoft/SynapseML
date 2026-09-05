# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from pathlib import Path

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
    assert 'git push --atomic origin "${TO_PUSH[@]}"' in tags
    assert 'git push --atomic origin "${TO_PUSH[@]}"' in read_workflow(
        "release-tag-spark.yml"
    )
    assert "outside the branch filters" not in tags
