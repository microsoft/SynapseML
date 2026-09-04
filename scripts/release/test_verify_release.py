# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import os
import sys
import urllib.error
import urllib.parse

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_release as verify  # noqa: E402


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
        return verify.OK, "github-commit"

    def public_maven(self, _module, _scala, _version):
        return verify.OK

    def internal_maven(self, _scala, _version):
        return verify.OK

    def public_pypi(self, _version):
        return verify.OK

    def ado_tag(self, _tag):
        return verify.OK, "ado-commit"

    def upack(self, _package, _version, internal=False):
        return verify.OK

    def pip(self, _package, _version, internal=False):
        return verify.OK


@pytest.mark.parametrize(
    "platform,command_type,use_shell",
    [
        ("win32", str, True),
        ("linux", list, False),
    ],
)
def test_ado_token_uses_platform_appropriate_command(
    monkeypatch, platform, command_type, use_shell
):
    captured = {}

    def run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return verify.subprocess.CompletedProcess(command, 0, "token\n", "")

    monkeypatch.setattr(verify.sys, "platform", platform)
    monkeypatch.setattr(verify.subprocess, "run", run)

    assert verify._get_ado_token(None) == "token"
    assert isinstance(captured["command"], command_type)
    assert captured["kwargs"]["shell"] is use_shell
    if use_shell:
        assert "az account get-access-token" in captured["command"]
    else:
        assert captured["command"][:3] == ["az", "account", "get-access-token"]


def test_ado_token_reports_missing_azure_cli(monkeypatch):
    def missing_cli(*_args, **_kwargs):
        raise FileNotFoundError("az not found")

    monkeypatch.setattr(verify.subprocess, "run", missing_cli)

    with pytest.raises(RuntimeError, match="set ADO_TOKEN"):
        verify._get_ado_token(None)


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


def test_url_exists_uses_head_without_downloading_body(monkeypatch):
    methods = []

    def open_url(request, **_kwargs):
        methods.append(request.get_method())
        return FakeResponse({})

    monkeypatch.setattr(verify.urllib.request, "urlopen", open_url)

    assert verify._url_exists("https://example/artifact.jar", {})
    assert methods == ["HEAD"]


@pytest.mark.parametrize("head_status", [405, 501])
def test_url_exists_falls_back_to_get_when_head_is_unsupported(
    monkeypatch, head_status
):
    methods = []

    def open_url(request, **_kwargs):
        method = request.get_method()
        methods.append(method)
        if method == "HEAD":
            raise urllib.error.HTTPError(
                request.full_url, head_status, "unsupported", {}, None
            )
        return FakeResponse({})

    monkeypatch.setattr(verify.urllib.request, "urlopen", open_url)

    assert verify._url_exists("https://example/artifact.jar", {})
    assert methods == ["HEAD", "GET"]


def test_url_exists_returns_false_for_missing_head(monkeypatch):
    methods = []

    def not_found(request, **_kwargs):
        methods.append(request.get_method())
        raise urllib.error.HTTPError(request.full_url, 404, "missing", {}, None)

    monkeypatch.setattr(verify.urllib.request, "urlopen", not_found)

    assert not verify._url_exists("https://example/missing.jar", {})
    assert methods == ["HEAD"]


def test_url_exists_returns_false_when_fallback_get_is_missing(monkeypatch):
    methods = []

    def open_url(request, **_kwargs):
        method = request.get_method()
        methods.append(method)
        status = 405 if method == "HEAD" else 404
        raise urllib.error.HTTPError(request.full_url, status, "unavailable", {}, None)

    monkeypatch.setattr(verify.urllib.request, "urlopen", open_url)

    assert not verify._url_exists("https://example/missing.jar", {})
    assert methods == ["HEAD", "GET"]


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


def test_run_checks_public_and_internal_maven_and_pypi(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)
    rows, complete = verify.run("1.1.3", "0", ["master"], None, None, [])

    assert complete
    assert [row["name"] for row in rows if row["kind"] == "maven"] == [
        "synapseml_2.12",
        "synapseml-core_2.12",
        "synapseml-cognitive_2.12",
        "synapseml-deep-learning_2.12",
        "synapseml-lightgbm_2.12",
        "synapseml-opencv_2.12",
        "synapseml-vw_2.12",
        "synapseml-internal_2.12",
    ]
    assert any(row["kind"] == "pypi" for row in rows)


def test_missing_public_install_coordinate_fails_release(monkeypatch):
    class MissingInstallCoordinateChecker(AlwaysPresentChecker):
        def public_maven(self, module, _scala, _version):
            return verify.MISSING if module == "synapseml" else verify.OK

    monkeypatch.setattr(verify, "Checker", MissingInstallCoordinateChecker)

    rows, complete = verify.run("1.1.3", "0", ["master"], None, None, [])

    assert not complete
    assert [row["name"] for row in rows if row["status"] == verify.MISSING] == [
        "synapseml_2.12"
    ]


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


def test_run_internal_only_scope_omits_all_oss_rows(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)

    rows, complete = verify.run(
        "1.1.3",
        "1",
        ["master"],
        None,
        None,
        [],
        scope="internal-only",
    )

    assert complete
    assert len(rows) == 7
    assert all(
        row["name"].startswith("ado/")
        or row["name"].startswith("synapseml-internal_")
        or row["name"] in {"synapseml_internal", "synapseml-internal"}
        for row in rows
    )


def test_run_infers_internal_only_scope_from_nonzero_patch(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)

    rows, complete = verify.run(
        "1.1.3",
        "1",
        ["master"],
        None,
        None,
        [],
    )

    assert complete
    assert len(rows) == 7
    assert all(not row["name"].startswith("github/") for row in rows)


def test_internal_skip_omits_only_internal_ado_artifacts(monkeypatch):
    feed_calls = []

    monkeypatch.setattr(verify, "_get_ado_token", lambda _token: "token")
    monkeypatch.setattr(
        verify,
        "_json_get",
        lambda url, _headers: (
            {"info": {"version": "1.1.3"}}
            if url.startswith(verify.PYPI_BASE)
            else {"object": {"type": "commit", "sha": "github-commit"}}
        ),
    )
    monkeypatch.setattr(verify, "_url_exists", lambda _url, _headers: True)

    def no_versions(_checker, feed, protocol, package):
        feed_calls.append((feed, protocol, package))
        return []

    monkeypatch.setattr(verify.Checker, "_feed_versions", no_versions)

    rows, complete = verify.run(
        "1.1.3",
        "0",
        ["master"],
        None,
        None,
        ["internal"],
    )

    assert not complete
    assert feed_calls == [
        ("BBC-VHD_PublicPackages", "upack", "synapseml"),
        ("Synapse-Conda", "pypi", "synapseml"),
    ]
    internal_rows = [
        row
        for row in rows
        if row["name"].startswith("ado/")
        or row["name"].startswith("synapseml-internal_")
        or row["name"] in {"synapseml_internal", "synapseml-internal"}
    ]
    assert internal_rows
    assert all(row["status"] == verify.SKIPPED for row in internal_rows)
    oss_feed_rows = [
        row
        for row in rows
        if row["kind"] in {"upack", "pip"} and row["name"] == "synapseml"
    ]
    assert all(row["status"] == verify.MISSING for row in oss_feed_rows)


def test_main_rejects_unknown_skip_without_network(capsys):
    assert verify.main(["--version", "1.1.3", "--skip", "typo"]) == 2
    assert "unknown --skip" in capsys.readouterr().err


def test_main_passes_internal_only_scope_to_run(monkeypatch):
    captured = {}

    def fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return [], True

    monkeypatch.setattr(verify, "run", fake_run)

    assert (
        verify.main(
            [
                "--version",
                "1.1.3",
                "--internal-patch",
                "1",
                "--scope",
                "internal-only",
            ]
        )
        == 0
    )
    assert captured["kwargs"]["scope"] == "internal-only"


def test_main_infers_internal_only_scope_from_nonzero_patch(monkeypatch):
    captured = {}

    def fake_run(*args, **kwargs):
        captured["scope"] = kwargs["scope"]
        return [], True

    monkeypatch.setattr(verify, "run", fake_run)

    assert verify.main(["--version", "1.1.3", "--internal-patch", "1"]) == 0
    assert captured["scope"] == "internal-only"


def test_main_json_reports_resolved_scope(monkeypatch, capsys):
    monkeypatch.setattr(verify, "run", lambda *_args, **_kwargs: ([], True))

    assert (
        verify.main(
            [
                "--version",
                "1.1.3",
                "--internal-patch",
                "1",
                "--json",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["version"] == "1.1.3"
    assert output["internal_patch"] == "1"
    assert output["scope"] == "internal-only"


def test_main_text_reports_resolved_scope(monkeypatch, capsys):
    monkeypatch.setattr(verify, "run", lambda *_args, **_kwargs: ([], True))

    assert verify.main(["--version", "1.1.3", "--internal-patch", "1"]) == 0
    assert "scope=internal-only" in capsys.readouterr().out


def test_internal_only_scope_requires_nonzero_patch(capsys):
    assert (
        verify.main(
            [
                "--version",
                "1.1.3",
                "--internal-patch",
                "0",
                "--scope",
                "internal-only",
            ]
        )
        == 2
    )
    assert "requires a nonzero --internal-patch" in capsys.readouterr().err


def test_full_scope_rejects_nonzero_patch(capsys):
    assert (
        verify.main(
            [
                "--version",
                "1.1.3",
                "--internal-patch",
                "1",
                "--scope",
                "full",
            ]
        )
        == 2
    )
    assert "use --scope internal-only" in capsys.readouterr().err


def test_skip_help_defines_internal_and_public_scopes(capsys):
    with pytest.raises(SystemExit) as exc:
        verify.main(["--help"])

    assert exc.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "internal (Internal tags, Maven, UPacks, and wheels)" in help_text
    assert "public (OSS Maven CDN and PyPI)" in help_text


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
    assert checker.public_maven("synapseml", "2.13", "1.1.3-spark4.0") == verify.OK
    assert checker.public_maven("synapseml-core", "2.13", "1.1.3-spark4.0") == verify.OK
    assert requested == [
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml_2.13/1.1.3-spark4.0/"
            "synapseml_2.13-1.1.3-spark4.0.pom",
            {"User-Agent": "synapseml-release-verify"},
        ),
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml_2.13/1.1.3-spark4.0/"
            "synapseml_2.13-1.1.3-spark4.0.jar",
            {"User-Agent": "synapseml-release-verify"},
        ),
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-core_2.13/1.1.3-spark4.0/"
            "synapseml-core_2.13-1.1.3-spark4.0.pom",
            {"User-Agent": "synapseml-release-verify"},
        ),
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-core_2.13/1.1.3-spark4.0/"
            "synapseml-core_2.13-1.1.3-spark4.0.jar",
            {"User-Agent": "synapseml-release-verify"},
        ),
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-core_2.13/1.1.3-spark4.0/"
            "synapseml-core_2.13-1.1.3-spark4.0-tests.jar",
            {"User-Agent": "synapseml-release-verify"},
        ),
    ]


def test_internal_maven_uses_release_specific_coordinate(monkeypatch):
    requested = []

    def exists(url, headers):
        requested.append((url, headers))
        return True

    monkeypatch.setattr(verify, "_url_exists", exists)
    checker = verify.Checker(None, None, ["ado"])

    assert checker.internal_maven("2.13", "1.1.3.0-spark4.1") == verify.OK
    assert requested == [
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-internal_2.13/1.1.3.0-spark4.1/"
            "synapseml-internal_2.13-1.1.3.0-spark4.1.pom",
            {"User-Agent": "synapseml-release-verify"},
        ),
        (
            "https://mmlspark.azureedge.net/maven/com/microsoft/azure/"
            "synapseml-internal_2.13/1.1.3.0-spark4.1/"
            "synapseml-internal_2.13-1.1.3.0-spark4.1.jar",
            {"User-Agent": "synapseml-release-verify"},
        ),
    ]


def test_tag_family_must_share_one_commit(monkeypatch):
    class MismatchedTagChecker(AlwaysPresentChecker):
        def github_tag(self, tag):
            return verify.OK, tag

    monkeypatch.setattr(verify, "Checker", MismatchedTagChecker)

    rows, complete = verify.run("1.1.3", "0", ["master"], None, None, [])

    assert not complete
    assert [
        row
        for row in rows
        if row["kind"] == "tag-set" and row["status"] == verify.MISSING
    ] == [
        {
            "kind": "tag-set",
            "target": "master",
            "name": "github/microsoft/SynapseML/same-commit",
            "identifier": ("v1.1.3, v1.1.3-spark3.5, v1.1.3-python3.11"),
            "status": verify.MISSING,
        }
    ]


def test_github_tag_peels_annotated_tag(monkeypatch):
    responses = {
        "https://api.github.com/repos/microsoft/SynapseML/git/ref/tags/v1.1.3": {
            "object": {
                "type": "tag",
                "sha": "tag-object",
                "url": "https://api.github.com/tag-object",
            }
        },
        "https://api.github.com/tag-object": {
            "object": {"type": "commit", "sha": "release-commit"}
        },
    }
    monkeypatch.setattr(
        verify,
        "_json_get",
        lambda url, _headers: responses[url],
    )

    checker = verify.Checker(None, None, ["ado"])
    assert checker.github_tag("v1.1.3") == (verify.OK, "release-commit")


def test_ado_tag_requests_and_uses_peeled_commit(monkeypatch):
    requested = []

    def get(url, _headers):
        requested.append(url)
        return {
            "value": [
                {
                    "name": "refs/tags/v1.1.3.0",
                    "objectId": "annotated-tag-object",
                    "peeledObjectId": "release-commit",
                }
            ]
        }

    monkeypatch.setattr(verify, "_get_ado_token", lambda _token: "token")
    monkeypatch.setattr(verify, "_json_get", get)

    checker = verify.Checker("token", None, [])
    assert checker.ado_tag("v1.1.3.0") == (verify.OK, "release-commit")
    assert "peelTags=true" in requested[0]
