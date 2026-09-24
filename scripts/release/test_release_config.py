# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import json

import pytest

import release_config as config

PIP_FEED = "synthetic-private-pip"
UPACK_FEED = "synthetic-private-upack"
INTERNAL_MAVEN_PIPELINE_ID = 900001
PUBLISH_PIPELINE_ID = 900002
PROFILE = {
    "schema_version": 1,
    "project_id": "55555555-5555-5555-5555-555555555555",
    "pip_feed": {"name": PIP_FEED, "id": "11111111-1111-1111-1111-111111111111"},
    "upack_feed": {"name": UPACK_FEED, "id": "22222222-2222-2222-2222-222222222222"},
    "internal_maven_pipeline_id": INTERNAL_MAVEN_PIPELINE_ID,
    "publish_pipeline_id": PUBLISH_PIPELINE_ID,
    "internal_repository": {
        "name": "synthetic-private-source",
        "id": "77777777-7777-7777-7777-777777777777",
    },
    "internal_packages": {
        "maven": "synthetic-private-package",
        "pip": "synthetic-private-package",
        "upack": "synthetic_private_package",
    },
}


@pytest.fixture(autouse=True)
def private_profile(tmp_path, monkeypatch):
    path = tmp_path / "local-release-profile.json"
    path.write_text(json.dumps(PROFILE), encoding="utf-8")
    monkeypatch.setenv(config.PROFILE_ENV, str(path))
    return path


def test_private_configuration_requires_explicit_local_input(monkeypatch):
    monkeypatch.delenv(config.PROFILE_ENV)
    with pytest.raises(ValueError, match="explicit local profile"):
        config.load_profile()
    monkeypatch.setenv(config.PROFILE_ENV, "relative.json")
    with pytest.raises(ValueError, match="absolute"):
        config.load_profile()


def test_private_configuration_is_exact_and_local(private_profile, monkeypatch):
    assert config.load_profile() == PROFILE
    monkeypatch.setenv(config.PROFILE_ENV, config.__file__)
    with pytest.raises(ValueError, match="outside checkout"):
        config.load_profile()


def test_profile_cannot_be_read_from_another_checkout(tmp_path, monkeypatch):
    checkout = tmp_path / "other-checkout"
    checkout.mkdir()
    (checkout / ".git").write_text("gitdir: external")
    path = checkout / "profile.json"
    path.write_text(json.dumps(PROFILE))
    monkeypatch.setenv(config.PROFILE_ENV, str(path))
    with pytest.raises(ValueError, match="outside checkout"):
        config.load_profile()


def test_private_configuration_is_bounded(private_profile):
    private_profile.write_text(" " * 8193)
    with pytest.raises(ValueError, match="size"):
        config.load_profile()


@pytest.mark.parametrize(
    "change",
    [
        lambda data: data.update(extra="not-approved"),
        lambda data: data.update(project_id="not-a-guid"),
        lambda data: data.update(publish_pipeline_id=True),
        lambda data: data.update(publish_pipeline_id=0),
        lambda data: data["pip_feed"].update(name="https://example.invalid/feed"),
        lambda data: data["pip_feed"].update(extra="not-approved"),
        lambda data: data.update(upack_feed=data["pip_feed"]),
        lambda data: data["internal_repository"].update(name="../source"),
    ],
)
def test_private_configuration_rejects_invalid_fields(private_profile, change):
    data = copy.deepcopy(PROFILE)
    change(data)
    private_profile.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError):
        config.load_profile()


def test_private_configuration_rejects_duplicates_and_nonfinite_json(private_profile):
    for raw in ('{"schema_version":1,"schema_version":1}', '{"value":NaN}'):
        private_profile.write_text(raw, encoding="utf-8")
        with pytest.raises(ValueError):
            config.load_profile()


@pytest.mark.parametrize("binary", [False, True], ids=["text", "bytes"])
def test_strict_json_controls_excessive_nesting(binary):
    marker = "synthetic-nested-json-value"
    raw = '{"' + marker + '":' + "[" * 20000 + "0" + "]" * 20000 + "}"
    with pytest.raises(ValueError, match="JSON nesting") as error:
        config.strict_json(raw.encode("utf-8") if binary else raw)
    assert marker not in str(error.value)
    assert raw not in str(error.value)


@pytest.mark.parametrize("python_scanner", [False, True], ids=["default", "python"])
def test_external_profile_nested_json_is_a_controlled_refusal(
    private_profile, monkeypatch, python_scanner
):
    if python_scanner:
        monkeypatch.setattr(json.scanner, "make_scanner", json.scanner.py_make_scanner)
    marker = "synthetic-nested-profile-value"
    raw = '{"' + marker + '":' + "[" * 3000 + "0" + "]" * 3000 + "}"
    assert len(raw.encode("utf-8")) < 8192
    private_profile.write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError) as error:
        config.load_profile()
    assert marker not in str(error.value)
    assert raw not in str(error.value)


def test_private_configuration_requires_explicit_package_identities(private_profile):
    data = json.loads(private_profile.read_text())
    data.pop("internal_packages", None)
    private_profile.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="profile|package"):
        config.load_profile()


@pytest.mark.parametrize(
    "packages",
    [
        None,
        {},
        {"maven": "synthetic-library", "pip": "synthetic-wheel"},
        {"maven": "../private", "pip": "synthetic-wheel", "upack": "synthetic_bundle"},
        {
            "maven": "synthetic-library",
            "pip": "NonCanonical_Name",
            "upack": "synthetic_bundle",
        },
        {"maven": "synthetic-library", "pip": "synthetic-wheel", "upack": ""},
        {"maven": "synthetic-library", "pip": "synthetic-wheel", "upack": "a" * 129},
    ],
)
def test_private_configuration_rejects_invalid_package_identities(
    private_profile, packages
):
    data = json.loads(private_profile.read_text())
    data["internal_packages"] = packages
    private_profile.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="profile|package"):
        config.load_profile()
