# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Optional private release configuration. Never include this profile in public inputs."""

import json
import os
import re
from pathlib import Path

PROFILE_ENV = "SYNAPSEML_RELEASE_PROFILE"
GUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
FEED_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def invalid_constant(_value):
    raise ValueError("non-finite JSON number")


def strict_json(value):
    try:
        return json.loads(
            value, object_pairs_hook=unique_object, parse_constant=invalid_constant
        )
    except RecursionError:
        raise ValueError("JSON nesting exceeds the supported depth") from None


def validate_profile(data):
    if (
        not isinstance(data, dict)
        or set(data)
        != {
            "schema_version",
            "project_id",
            "pip_feed",
            "upack_feed",
            "internal_maven_pipeline_id",
            "publish_pipeline_id",
            "internal_repository",
            "internal_packages",
        }
        or type(data["schema_version"]) is not int
        or data["schema_version"] != 1
        or not isinstance(data["project_id"], str)
        or not GUID.fullmatch(data["project_id"])
    ):
        raise ValueError("invalid local release profile")
    for key in ("internal_maven_pipeline_id", "publish_pipeline_id"):
        if type(data[key]) is not int or not 0 < data[key] < 2**31:
            raise ValueError("invalid local release profile pipeline identity")
    if data["internal_maven_pipeline_id"] == data["publish_pipeline_id"]:
        raise ValueError("local release profile requires distinct pipelines")
    repository = data["internal_repository"]
    if (
        not isinstance(repository, dict)
        or set(repository) != {"name", "id"}
        or not isinstance(repository["name"], str)
        or not FEED_NAME.fullmatch(repository["name"])
        or not isinstance(repository["id"], str)
        or not GUID.fullmatch(repository["id"])
    ):
        raise ValueError("invalid local release profile source identity")
    for key in ("pip_feed", "upack_feed"):
        feed = data[key]
        if (
            not isinstance(feed, dict)
            or set(feed) != {"name", "id"}
            or not isinstance(feed["name"], str)
            or not FEED_NAME.fullmatch(feed["name"])
            or GUID.fullmatch(feed["name"].lower())
            or not isinstance(feed["id"], str)
            or not GUID.fullmatch(feed["id"])
        ):
            raise ValueError("invalid local release profile feed identity")
    if (
        data["pip_feed"]["id"] == data["upack_feed"]["id"]
        or data["pip_feed"]["name"].casefold() == data["upack_feed"]["name"].casefold()
    ):
        raise ValueError("local release profile requires distinct feeds")
    packages = data["internal_packages"]
    if not isinstance(packages, dict) or set(packages) != {"maven", "pip", "upack"}:
        raise ValueError(
            "local release profile requires explicit private package identities"
        )
    for family, name in packages.items():
        if (
            not isinstance(name, str)
            or not 1 <= len(name) <= 128
            or not re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", name)
            or (family == "pip" and re.sub(r"[-_.]+", "-", name) != name)
        ):
            raise ValueError("invalid local release profile package identity")
    return data


def package_name(profile, repository, family):
    if repository not in {"oss", "internal"} or family not in {"maven", "pip", "upack"}:
        raise ValueError("unknown release package selection")
    if repository == "oss":
        return "synapseml"
    if profile is None:
        raise ValueError(
            "legacy private package identities are not bound to a profile; regenerate and reapprove"
        )
    return validate_profile(profile)["internal_packages"][family]


def load_profile():
    raw = os.environ.get(PROFILE_ENV)
    if not raw:
        raise ValueError(
            f"private release operations require an explicit local profile via {PROFILE_ENV}"
        )
    path = Path(raw)
    if not path.is_absolute():
        raise ValueError(
            "local release profile must use an absolute path outside checkout"
        )
    try:
        path = path.resolve(strict=True)
        checkout = Path(__file__).resolve().parents[2]
        if (
            checkout == path
            or checkout in path.parents
            or any((parent / ".git").exists() for parent in path.parents)
        ):
            raise ValueError("local release profile must remain outside checkout")
        with path.open("rb") as stream:
            contents = stream.read(8193)
        if len(contents) > 8192:
            raise ValueError("local release profile exceeds the supported size")
        return validate_profile(strict_json(contents))
    except (OSError, UnicodeError) as error:
        raise ValueError("cannot read the explicit local release profile") from error
