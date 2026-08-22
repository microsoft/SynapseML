# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import urllib.error
import urllib.parse

import pytest

import verify_release as verify


class FakeResponse:
    def __init__(self, body, headers=None):
        self._body = body
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self._body).encode("utf-8")


class AlwaysPresentChecker:
    def __init__(self, *_args, **_kwargs):
        pass

    def github_tag(self, _tag):
        return verify.OK

    def public_maven(self, _scala, _version):
        return verify.OK

    def public_pypi(self, _version):
        return verify.OK

    def ado_tag(self, _tag):
        return verify.OK

    def upack(self, _package, _version):
        return verify.OK

    def pip(self, _package, _version):
        return verify.OK


def test_json_get_parses_successful_response(monkeypatch):
    monkeypatch.setattr(
        verify.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse({"value": ["ok"]}),
    )
    assert verify._json_get("https://example", {}) == {"value": ["ok"]}


def test_json_get_returns_none_only_for_not_found(monkeypatch):
    def not_found(*_args, **_kwargs):
        raise urllib.error.HTTPError("https://example", 404, "missing", {}, None)

    monkeypatch.setattr(verify.urllib.request, "urlopen", not_found)
    assert verify._json_get("https://example", {}) is None


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.HTTPError("https://example", 500, "failed", {}, None),
        urllib.error.URLError("network unavailable"),
    ],
)
def test_json_get_surfaces_service_and_network_failures(monkeypatch, error):
    def fail(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(verify.urllib.request, "urlopen", fail)
    with pytest.raises(RuntimeError):
        verify._json_get("https://example", {})


def test_checker_skips_ado_login_when_all_ado_checks_are_skipped(monkeypatch):
    def fail_if_called(_token):
        raise AssertionError("ADO login should not be requested")

    monkeypatch.setattr(verify, "_get_ado_token", fail_if_called)
    checker = verify.Checker(None, None, ["ado"])
    assert checker._ado_headers is None


def test_feed_lookup_filters_exact_package_and_follows_continuation(monkeypatch):
    calls = []

    def fake_page(url, _headers):
        calls.append(url)
        if len(calls) == 1:
            return (
                {
                    "value": [
                        {
                            "name": "unrelated",
                            "versions": [{"version": "9.9.9"}],
                        }
                    ]
                },
                {"x-ms-continuationtoken": "next page"},
            )
        return (
            {
                "value": [
                    {
                        "name": "synapseml",
                        "versions": [
                            {"version": "1.1.3+python3.11"},
                            {"version": "1.1.3+python3.12"},
                        ],
                    }
                ]
            },
            {},
        )

    monkeypatch.setattr(verify, "_get_ado_token", lambda _token: "token")
    monkeypatch.setattr(verify, "_json_get_page", fake_page)
    checker = verify.Checker("token", None, [])

    assert checker._feed_versions("Synapse-Conda", "pypi", "synapseml") == [
        "1.1.3+python3.11",
        "1.1.3+python3.12",
    ]
    first_query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0]).query)
    second_query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[1]).query)
    assert first_query["packageNameQuery"] == ["synapseml"]
    assert first_query["includeAllVersions"] == ["true"]
    assert first_query["api-version"] == ["7.1-preview.1"]
    assert second_query["continuationToken"] == ["next page"]

    checker._feed_versions("Synapse-Conda", "pypi", "synapseml")
    assert len(calls) == 2


def test_run_checks_public_maven_and_pypi(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)
    rows, complete = verify.run("1.1.3", "0", ["master"], None, None, [])

    assert complete
    assert any(row["kind"] == "maven" for row in rows)
    assert any(row["kind"] == "pypi" for row in rows)


def test_run_applies_upack_rebuild_counters(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)
    rows, complete = verify.run(
        "1.1.1",
        "0",
        ["spark4.0"],
        None,
        None,
        [],
        {"spark4.0": 1},
    )

    assert complete
    assert any(
        row["kind"] == "upack"
        and row["name"] == "synapseml"
        and row["identifier"] == "1.1.1-spark4-0-1"
        for row in rows
    )


def test_main_rejects_unknown_skip_without_network(capsys):
    assert verify.main(["--version", "1.1.3", "--skip", "typo"]) == 2
    assert "unknown --skip" in capsys.readouterr().err


def test_public_pypi_requires_the_requested_version(monkeypatch):
    monkeypatch.setattr(
        verify,
        "_json_get",
        lambda _url, _headers: {"info": {"version": "1.1.2"}},
    )
    checker = verify.Checker(None, None, ["ado"])
    assert checker.public_pypi("1.1.3") == verify.MISSING


def test_public_maven_uses_release_specific_coordinate(monkeypatch):
    requested = []

    def exists(url, headers):
        requested.append((url, headers))
        return True

    monkeypatch.setattr(verify, "_url_exists", exists)
    checker = verify.Checker(None, "github-token", ["ado"])
    assert checker.public_maven("2.13", "1.1.3-spark4.0") == verify.OK
    assert requested == [
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-core_2.13/1.1.3-spark4.0/"
            "synapseml-core_2.13-1.1.3-spark4.0.pom",
            {"User-Agent": "synapseml-release-verify"},
        )
    ]
