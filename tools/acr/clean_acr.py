# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import hashlib
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict, List


ACR_NAME = "mmlsparkmcr"
ACR_RESOURCE_GROUP = "marhamil-mmlspark"
EXPORT_PIPELINE = "mmlsparkacrexport3"
STORAGE_ACCOUNT = "mmlspark"
STORAGE_CONTAINER = "acrbackup"
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


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


def run_az_json(arguments: List[str]) -> Any:
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
    return f"synapseml{digest}{timestamp}"


def normalize_digest(digest: str) -> str:
    normalized = digest.lower()
    if not DIGEST_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported ACR manifest digest: {digest}")
    return normalized


def manifest_blob_name(repository: str, digest: str) -> str:
    algorithm, value = normalize_digest(digest).split(":", 1)
    return f"{repository}/manifests/{algorithm}-{value}.tar"


def list_manifests(repository: str) -> List[Dict[str, Any]]:
    return run_az_json(
        [
            "acr",
            "manifest",
            "list-metadata",
            "--registry",
            ACR_NAME,
            "--name",
            repository,
            "--orderby",
            "time_desc",
        ]
    )


def get_manifest_metadata(repository: str, digest: str) -> Dict[str, Any]:
    return run_az_json(
        [
            "acr",
            "manifest",
            "show-metadata",
            "--registry",
            ACR_NAME,
            "--name",
            f"{repository}@{normalize_digest(digest)}",
        ]
    )


def export_manifest(repository: str, digest: str, target_blob: str) -> None:
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
            f"{repository}@{normalize_digest(digest)}",
        ],
        attempts=5,
    )


def delete_manifest(repository: str, digest: str) -> None:
    run_az(
        [
            "acr",
            "repository",
            "delete",
            "--name",
            ACR_NAME,
            "--image",
            f"{repository}@{normalize_digest(digest)}",
            "--yes",
        ],
        attempts=5,
    )


def clean_acr() -> None:
    run_az(["extension", "add", "--name", "acrtransfer", "--upgrade"])
    repositories = run_az_json(["acr", "repository", "list", "--name", ACR_NAME])

    for repository in repositories:
        processed_digests = set()
        for manifest in list_manifests(repository):
            tags = manifest.get("tags") or []
            if not tags:
                continue

            digest = normalize_digest(manifest["digest"])
            if digest in processed_digests:
                continue
            processed_digests.add(digest)

            target_blob = manifest_blob_name(repository, digest)
            aliases = ", ".join(sorted(tags))

            if backup_exists(target_blob):
                current = get_manifest_metadata(repository, digest)
                current_digest = normalize_digest(current["digest"])
                if current_digest != digest:
                    raise RuntimeError(
                        f"Manifest digest changed before deletion: "
                        f"{repository}@{digest} resolved to {current_digest}"
                    )
                current_aliases = ", ".join(sorted(current.get("tags") or []))
                print(
                    f"Confirmed backup for {repository}@{digest}; "
                    f"deleting manifest aliases: {current_aliases or '<none>'}"
                )
                delete_manifest(repository, digest)
            else:
                export_manifest(repository, digest, target_blob)
                print(
                    f"Queued digest export for {repository}@{digest} "
                    f"(aliases: {aliases}); deletion is deferred until "
                    f"{target_blob} is confirmed by a later cleanup run"
                )


if __name__ == "__main__":
    clean_acr()
