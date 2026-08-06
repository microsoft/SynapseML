# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import re
import subprocess
from unittest.mock import call, patch

import pytest

from tools.acr import clean_acr

DIGEST_A = "sha256:" + ("a" * 64)
DIGEST_B = "sha256:" + ("b" * 64)


def completed(stdout="", stderr="", returncode=0):
    return subprocess.CompletedProcess(
        args=["az"], returncode=returncode, stdout=stdout, stderr=stderr
    )


@patch("tools.acr.clean_acr.subprocess.run")
def test_blob_lookup_uses_entra_authentication(run):
    run.return_value = completed(stdout="true\n")

    assert clean_acr.backup_exists("repo/tag.tar")

    command = run.call_args.args[0]
    assert "--auth-mode" in command
    assert command[command.index("--auth-mode") + 1] == "login"
    assert "account-key" not in " ".join(command)
    assert "connection-string" not in " ".join(command)


@patch("tools.acr.clean_acr.time.sleep")
@patch("tools.acr.clean_acr.subprocess.run")
def test_azure_cli_retries_and_surfaces_the_final_error(run, sleep):
    run.side_effect = [
        completed(stderr="temporary", returncode=1),
        completed(stderr="permanent", returncode=1),
    ]

    with pytest.raises(RuntimeError, match="permanent"):
        clean_acr.run_az(["account", "show"], attempts=2)

    assert sleep.call_args_list == [call(5)]


def test_pipeline_run_name_meets_acr_resource_constraints():
    name = clean_acr.pipeline_run_name(f"repo/manifests/sha256-{'a' * 64}.tar")

    assert 5 <= len(name) <= 50
    assert re.fullmatch(r"[a-zA-Z0-9]+", name)


def test_manifest_backup_is_keyed_by_immutable_digest():
    blob = clean_acr.manifest_blob_name("repo/path", DIGEST_A)

    assert blob == f"repo/path/manifests/sha256-{'a' * 64}.tar"


def test_manifest_backup_rejects_invalid_digest():
    with pytest.raises(ValueError, match="Unsupported ACR manifest digest"):
        clean_acr.manifest_blob_name("repo", "latest")


@patch(
    "tools.acr.clean_acr.pipeline_run_name",
    return_value="synapseml794a1ed53092878b267120260801030000",
)
@patch("tools.acr.clean_acr.run_az")
def test_export_uses_digest_and_valid_pipeline_run_name(run_az, pipeline_run_name):
    target_blob = clean_acr.manifest_blob_name("repo", DIGEST_A)

    clean_acr.export_manifest("repo", DIGEST_A, target_blob)

    command = run_az.call_args.args[0]
    assert command[command.index("--artifacts") + 1] == f"repo@{DIGEST_A}"
    assert command[command.index("--storage-blob") + 1] == target_blob
    assert (
        command[command.index("--name") + 1]
        == "synapseml794a1ed53092878b267120260801030000"
    )
    pipeline_run_name.assert_called_once_with(target_blob)


@patch("tools.acr.clean_acr.delete_manifest")
@patch("tools.acr.clean_acr.export_manifest")
@patch("tools.acr.clean_acr.get_manifest_metadata")
@patch("tools.acr.clean_acr.backup_exists")
@patch("tools.acr.clean_acr.list_manifests")
@patch("tools.acr.clean_acr.run_az_json")
@patch("tools.acr.clean_acr.run_az")
def test_cleanup_defers_deletion_after_queuing_export(
    run_az,
    run_az_json,
    list_manifests,
    backup_exists,
    get_manifest_metadata,
    export_manifest,
    delete_manifest,
):
    run_az_json.return_value = ["repo"]
    list_manifests.return_value = [{"digest": DIGEST_A, "tags": ["1.0.0", "latest"]}]
    backup_exists.return_value = False

    clean_acr.clean_acr()

    export_manifest.assert_called_once_with(
        "repo",
        DIGEST_A,
        clean_acr.manifest_blob_name("repo", DIGEST_A),
    )
    get_manifest_metadata.assert_not_called()
    delete_manifest.assert_not_called()


@patch("tools.acr.clean_acr.delete_manifest")
@patch("tools.acr.clean_acr.export_manifest")
@patch("tools.acr.clean_acr.get_manifest_metadata")
@patch("tools.acr.clean_acr.backup_exists")
@patch("tools.acr.clean_acr.list_manifests")
@patch("tools.acr.clean_acr.run_az_json")
@patch("tools.acr.clean_acr.run_az")
def test_cleanup_deletes_digest_once_after_backup_and_revalidation(
    run_az,
    run_az_json,
    list_manifests,
    backup_exists,
    get_manifest_metadata,
    export_manifest,
    delete_manifest,
):
    run_az_json.return_value = ["repo"]
    list_manifests.return_value = [
        {"digest": DIGEST_A, "tags": ["1.0.0", "latest"]},
        {"digest": DIGEST_A, "tags": ["duplicate-entry"]},
    ]
    backup_exists.return_value = True
    get_manifest_metadata.return_value = {
        "digest": DIGEST_A,
        "tags": ["1.0.0", "latest"],
    }

    clean_acr.clean_acr()

    export_manifest.assert_not_called()
    get_manifest_metadata.assert_called_once_with("repo", DIGEST_A)
    delete_manifest.assert_called_once_with("repo", DIGEST_A)


@patch("tools.acr.clean_acr.delete_manifest")
@patch("tools.acr.clean_acr.export_manifest")
@patch("tools.acr.clean_acr.get_manifest_metadata")
@patch("tools.acr.clean_acr.backup_exists")
@patch("tools.acr.clean_acr.list_manifests")
@patch("tools.acr.clean_acr.run_az_json")
@patch("tools.acr.clean_acr.run_az")
def test_cleanup_handles_moved_tag_without_reusing_old_backup(
    run_az,
    run_az_json,
    list_manifests,
    backup_exists,
    get_manifest_metadata,
    export_manifest,
    delete_manifest,
):
    run_az_json.return_value = ["release"]
    list_manifests.return_value = [
        {"digest": DIGEST_A, "tags": ["1.0.0"]},
        {"digest": DIGEST_B, "tags": ["latest"]},
    ]
    backup_exists.side_effect = [True, False]
    get_manifest_metadata.return_value = {
        "digest": DIGEST_A,
        "tags": ["1.0.0"],
    }

    clean_acr.clean_acr()

    delete_manifest.assert_called_once_with("release", DIGEST_A)
    export_manifest.assert_called_once_with(
        "release",
        DIGEST_B,
        clean_acr.manifest_blob_name("release", DIGEST_B),
    )
