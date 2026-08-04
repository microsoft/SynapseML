# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import subprocess
from unittest.mock import call, patch

import pytest

from tools.acr import clean_acr


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


@patch(
    "tools.acr.clean_acr.pipeline_run_name",
    return_value="synapseml-794a1ed53092878b2671-20260801030000",
)
@patch("tools.acr.clean_acr.run_az")
def test_export_uses_unique_pipeline_run_name(run_az, pipeline_run_name):
    clean_acr.export_image("repo:tag", "repo/tag.tar")

    command = run_az.call_args.args[0]
    assert command[command.index("--artifacts") + 1] == "repo:tag"
    assert command[command.index("--storage-blob") + 1] == "repo/tag.tar"
    assert (
        command[command.index("--name") + 1]
        == "synapseml-794a1ed53092878b2671-20260801030000"
    )
    pipeline_run_name.assert_called_once_with("repo/tag.tar")


@patch("tools.acr.clean_acr.delete_image")
@patch("tools.acr.clean_acr.export_image")
@patch("tools.acr.clean_acr.backup_exists")
@patch("tools.acr.clean_acr.run_az_json")
@patch("tools.acr.clean_acr.run_az")
def test_cleanup_defers_deletion_after_queuing_export(
    run_az, run_az_json, backup_exists, export_image, delete_image
):
    run_az_json.side_effect = [["repo"], ["tag"]]
    backup_exists.return_value = False

    clean_acr.clean_acr()

    export_image.assert_called_once_with("repo:tag", "repo/tag.tar")
    delete_image.assert_not_called()


@patch("tools.acr.clean_acr.delete_image")
@patch("tools.acr.clean_acr.export_image")
@patch("tools.acr.clean_acr.backup_exists")
@patch("tools.acr.clean_acr.run_az_json")
@patch("tools.acr.clean_acr.run_az")
def test_cleanup_deletes_only_when_backup_is_confirmed(
    run_az, run_az_json, backup_exists, export_image, delete_image
):
    run_az_json.side_effect = [["repo"], ["tag"]]
    backup_exists.return_value = True

    clean_acr.clean_acr()

    export_image.assert_not_called()
    delete_image.assert_called_once_with("repo:tag")
