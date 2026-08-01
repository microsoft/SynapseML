# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import hashlib
import json
import subprocess
import time
from datetime import datetime, timezone
from typing import List


ACR_NAME = "mmlsparkmcr"
ACR_RESOURCE_GROUP = "marhamil-mmlspark"
EXPORT_PIPELINE = "mmlsparkacrexport3"
STORAGE_ACCOUNT = "mmlspark"
STORAGE_CONTAINER = "acrbackup"


def run_az(arguments: List[str], attempts: int = 3) -> str:
    delay_seconds = 5
    command = ["az", *arguments, "--only-show-errors"]

    for attempt in range(1, attempts + 1):
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result.stdout
        if attempt == attempts:
            raise RuntimeError(
                f"Azure CLI command failed after {attempts} attempts: "
                f"{' '.join(command)}\n{result.stderr}"
            )
        print(
            f"Azure CLI command attempt {attempt} failed; "
            f"retrying after {delay_seconds} seconds"
        )
        time.sleep(delay_seconds)
        delay_seconds *= 3

    raise AssertionError("unreachable")


def run_az_json(arguments: List[str]) -> List[str]:
    return json.loads(run_az([*arguments, "--output", "json"]))


def backup_exists(blob_name: str) -> bool:
    output = run_az(
        [
            "storage",
            "blob",
            "exists",
            "--account-name",
            STORAGE_ACCOUNT,
            "--container-name",
            STORAGE_CONTAINER,
            "--name",
            blob_name,
            "--auth-mode",
            "login",
            "--query",
            "exists",
            "--output",
            "tsv",
        ]
    )
    return output.strip().lower() == "true"


def pipeline_run_name(target_blob: str) -> str:
    digest = hashlib.sha256(target_blob.encode("utf-8")).hexdigest()[:20]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"synapseml-{digest}-{timestamp}"


def export_image(image: str, target_blob: str) -> None:
    run_az(
        [
            "acr",
            "pipeline-run",
            "create",
            "--resource-group",
            ACR_RESOURCE_GROUP,
            "--registry",
            ACR_NAME,
            "--pipeline",
            EXPORT_PIPELINE,
            "--name",
            pipeline_run_name(target_blob),
            "--pipeline-type",
            "export",
            "--storage-blob",
            target_blob,
            "--artifacts",
            image,
        ],
        attempts=5,
    )


def delete_image(image: str) -> None:
    run_az(
        [
            "acr",
            "repository",
            "delete",
            "--name",
            ACR_NAME,
            "--image",
            image,
            "--yes",
        ],
        attempts=5,
    )


def clean_acr() -> None:
    run_az(["extension", "add", "--name", "acrtransfer", "--upgrade"])
    repositories = run_az_json(["acr", "repository", "list", "--name", ACR_NAME])

    for repository in repositories:
        tags = run_az_json(
            [
                "acr",
                "repository",
                "show-tags",
                "--name",
                ACR_NAME,
                "--repository",
                repository,
                "--orderby",
                "time_desc",
            ]
        )
        for tag in tags:
            target_blob = f"{repository}/{tag}.tar"
            image = f"{repository}:{tag}"

            if backup_exists(target_blob):
                print(f"Skipped existing backup for {image}")
                print(f"Deleting {image}")
                delete_image(image)
            else:
                export_image(image, target_blob)
                print(
                    f"Queued export for {image}; deletion is deferred until "
                    f"{target_blob} is confirmed by a later cleanup run"
                )


if __name__ == "__main__":
    clean_acr()
