# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import io
import json
import os
import sys
import urllib.error
import urllib.parse

import pytest

from test_release_config import private_profile  # noqa: F401

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_release as verify  # noqa: E402


class FakeResponse(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(body if isinstance(body, bytes) else json.dumps(body).encode())
        self.headers = headers or {}


class AlwaysPresentChecker:
    def __init__(self, *_args, **_kwargs):
        pass

    def github_tag(self, _tag):
        return verify.OK, "github-commit"

    def _artifact(self, *_args, **_kwargs):
        return verify.OK

    public_maven = (
        public_central_maven
    ) = internal_maven = public_pypi = upack = pip = _artifact

    def public_dbc(self, _version, _commit):
        return verify.OK, "f" * 64, 321

    def ado_tag(self, _tag):
        return verify.OK, "ado-commit"


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
    assert captured["kwargs"]["timeout"] == 60
    if use_shell:
        assert "az account get-access-token" in captured["command"]
    else:
        assert captured["command"][:3] == ["az", "account", "get-access-token"]


@pytest.mark.parametrize("failure", ["missing", "timeout", "failed", "empty"])
def test_ado_token_errors_are_controlled_and_sanitized(monkeypatch, failure):
    def run(command, **kwargs):
        if failure == "missing":
            raise FileNotFoundError("synthetic-private-detail")
        if failure == "timeout":
            raise verify.subprocess.TimeoutExpired(
                command, 60, output="synthetic-private-detail"
            )
        return verify.subprocess.CompletedProcess(
            command, int(failure == "failed"), "", "synthetic-private-detail"
        )

    monkeypatch.setattr(verify.subprocess, "run", run)
    assert verify._get_ado_token("provided-token") == "provided-token"
    with pytest.raises(RuntimeError, match="Azure CLI|ADO token") as error:
        verify._get_ado_token(None)
    assert "synthetic-private-detail" not in str(error.value)


@pytest.mark.parametrize(
    "body", [b'{"value":["ok"]}', b"<html>failure</html>", b'{"truncated":']
)
def test_json_get_handles_valid_and_malformed_responses(monkeypatch, body):
    url = "https://example.invalid/feed"
    monkeypatch.setattr(
        verify.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(body),
    )
    if body.startswith(b'{"value"'):
        assert verify._json_get(url, {}) == {"value": ["ok"]}
    else:
        with pytest.raises(RuntimeError, match="invalid JSON response") as error:
            verify._json_get(url, {})
        assert url in str(error.value)


@pytest.mark.parametrize("extra", [0, 1])
def test_json_get_enforces_the_exact_response_byte_limit(monkeypatch, extra):
    limit = 16 * 1024 * 1024
    reads = []

    class Response(FakeResponse):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    response = Response(b"{}" + b" " * (limit - 2 + extra))
    monkeypatch.setattr(verify.urllib.request, "urlopen", lambda *_a, **_k: response)
    if extra:
        with pytest.raises(RuntimeError, match="size limit"):
            verify._json_get("https://example.invalid", {})
    else:
        assert verify._json_get("https://example.invalid", {}) == {}
    assert reads == [limit + 1]
    assert response.closed


@pytest.mark.parametrize("status", [404, 401, 403, 429, 500, None])
def test_json_get_returns_absence_only_for_404(monkeypatch, status):
    def fail(*_args, **_kwargs):
        if status is None:
            raise urllib.error.URLError("network unavailable")
        raise urllib.error.HTTPError("https://example", status, "failed", {}, None)

    monkeypatch.setattr(verify.urllib.request, "urlopen", fail)
    if status == 404:
        assert verify._json_get("https://example", {}) is None
    else:
        with pytest.raises(RuntimeError):
            verify._json_get("https://example", {})


@pytest.mark.parametrize(
    "statuses,present",
    [
        ([200], True),
        ([405, 200], True),
        ([501, 200], True),
        ([404], False),
        ([405, 404], False),
    ],
)
def test_url_exists_uses_head_and_only_falls_back_when_unsupported(
    monkeypatch, statuses, present
):
    methods = []

    def open_url(request, **_kwargs):
        methods.append(request.get_method())
        status = statuses[len(methods) - 1]
        if status != 200:
            raise urllib.error.HTTPError(request.full_url, status, "failed", {}, None)
        return FakeResponse({})

    monkeypatch.setattr(verify.urllib.request, "urlopen", open_url)
    assert verify._url_exists("https://example/artifact.jar", {}) is present
    assert methods == ["HEAD", "GET"][: len(statuses)]


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

    assert checker._feed_versions("synthetic-private-pip", "pypi", "synapseml") == [
        "1.1.3+python3.11",
        "1.1.3+python3.12",
    ]
    first_query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0]).query)
    second_query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[1]).query)
    assert first_query["packageNameQuery"] == ["synapseml"]
    assert first_query["includeAllVersions"] == ["true"]
    assert first_query["api-version"] == ["7.1-preview.1"]
    assert second_query["continuationToken"] == ["next page"]

    checker._feed_versions("synthetic-private-pip", "pypi", "synapseml")
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
        "synthetic-private-package_2.12",
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


@pytest.mark.parametrize("scope", [None, "internal-only"])
def test_run_internal_only_scope_omits_all_oss_rows(monkeypatch, scope):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)

    rows, complete = verify.run(
        "1.1.3",
        "1",
        ["master"],
        None,
        None,
        [],
        scope=scope,
    )

    assert complete
    assert len(rows) == 7
    assert all(
        row["name"].startswith("ado/")
        or row["name"].startswith("synthetic-private-package_")
        or row["name"] in {"synthetic_private_package", "synthetic-private-package"}
        for row in rows
    )


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
        ("synthetic-private-upack", "upack", "synapseml"),
        ("synthetic-private-pip", "pypi", "synapseml"),
    ]
    internal_rows = [
        row
        for row in rows
        if row["name"].startswith("ado/")
        or row["name"].startswith("synthetic-private-package_")
        or row["name"] in {"synthetic_private_package", "synthetic-private-package"}
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


@pytest.mark.parametrize(
    "skip", ["ado,internal", "internal,pip,upack", "ado,internal,pip,upack"]
)
@pytest.mark.parametrize("available", [True, False])
def test_public_only_historical_check_needs_no_private_profile(
    monkeypatch, capsys, skip, available
):
    import release_config as config
    import release_dbc

    monkeypatch.delenv(config.PROFILE_ENV)

    def forbidden(*_args, **_kwargs):
        pytest.fail("Historical public checks must not use private services or DBCs")

    def public_json(url, _headers):
        if url.startswith("https://api.github.com/repos/microsoft/SynapseML/"):
            return {"object": {"type": "commit", "sha": "a" * 40}}
        if url == "https://pypi.org/pypi/synapseml/1.1.4/json":
            return {"info": {"version": "1.1.4"}}
        pytest.fail(f"Unexpected public lookup: {url}")

    requested = []

    def exists(url, _headers):
        requested.append(url)
        return available

    monkeypatch.setattr(verify, "_get_ado_token", forbidden)
    monkeypatch.setattr(verify.urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(release_dbc, "fetch_public_archive", forbidden)
    monkeypatch.setattr(verify, "_json_get", public_json)
    monkeypatch.setattr(verify, "_url_exists", exists)

    assert verify.main(["--version", "1.1.4", "--skip", skip, "--json"]) == (
        0 if available else 1
    )
    report = json.loads(capsys.readouterr().out)
    assert report["complete"] is available
    assert report["provenance"] == "unbound historical check; not approval evidence"
    assert {row["target"] for row in report["rows"]} == {"master", "spark4.1"}
    assert {row["kind"] for row in report["rows"]} == {
        "git-tag",
        "tag-set",
        "maven",
        "pypi",
    }
    assert requested
    assert all(
        url.startswith("https://mmlspark.blob.core.windows.net/maven/")
        for url in requested
    )


@pytest.mark.parametrize("skip", ["internal", "ado", "internal,pip", "internal,upack"])
def test_historical_check_still_requires_profile_for_enabled_private_checks(
    monkeypatch, capsys, skip
):
    import release_config as config

    monkeypatch.delenv(config.PROFILE_ENV)

    def forbidden(*_args, **_kwargs):
        pytest.fail("Missing private configuration must fail before network access")

    monkeypatch.setattr(verify.urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(verify, "_get_ado_token", forbidden)

    assert verify.main(["--version", "1.1.4", "--skip", skip]) == 2
    assert "explicit local profile" in capsys.readouterr().err


@pytest.mark.parametrize("explicit_scope", [False, True])
@pytest.mark.parametrize("json_output", [False, True])
def test_main_reports_and_passes_resolved_scope(
    monkeypatch, capsys, explicit_scope, json_output
):
    captured = {}

    def fake_run(*args, **kwargs):
        captured["scope"] = kwargs["scope"]
        return [], True

    monkeypatch.setattr(verify, "run", fake_run)
    arguments = ["--version", "1.1.3", "--internal-patch", "1"]
    if explicit_scope:
        arguments += ["--scope", "internal-only"]
    if json_output:
        arguments += ["--json"]
    assert verify.main(arguments) == 0
    assert captured["scope"] == "internal-only"
    output = capsys.readouterr().out
    if json_output:
        report = json.loads(output)
        assert (report["version"], report["internal_patch"], report["scope"]) == (
            "1.1.3",
            "1",
            "internal-only",
        )
    else:
        assert "scope=internal-only" in output


@pytest.mark.parametrize(
    "patch,scope,message",
    [
        ("0", "internal-only", "requires a nonzero --internal-patch"),
        ("1", "full", "use --scope internal-only"),
    ],
)
def test_main_rejects_inconsistent_patch_scope(capsys, patch, scope, message):
    assert (
        verify.main(["--version", "1.1.3", "--internal-patch", patch, "--scope", scope])
        == 2
    )
    assert message in capsys.readouterr().err


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


@pytest.mark.parametrize("data", [{"info": None}, {"info": []}, {}, ["invalid"]])
@pytest.mark.parametrize("strict", [False, True])
def test_public_pypi_malformed_metadata_fails_with_controlled_error(
    monkeypatch, data, strict
):
    monkeypatch.setattr(verify, "_json_get", lambda *_: data)

    def forbidden(*_):
        pytest.fail("Malformed metadata must not trigger a wheel lookup")

    monkeypatch.setattr(verify, "_url_exists", forbidden)
    checker = verify.Checker(None, None, ["ado", "internal"])
    with pytest.raises(RuntimeError, match="invalid release metadata"):
        checker.public_pypi("1.2.0", strict)


@pytest.mark.parametrize("source", ["version", "plan"])
def test_malformed_pypi_metadata_returns_cli_error(monkeypatch, capsys, source):
    plan = verify.build_plan(
        "1.2.0", target_keys=["master"], oss_commits={"master": "a" * 40}
    )
    monkeypatch.setattr(verify, "read_plan", lambda *_a, **_k: plan)
    monkeypatch.setattr(verify.Checker, "github_tag", lambda *_: (verify.OK, "a" * 40))
    monkeypatch.setattr(verify.Checker, "_maven", lambda *_a, **_k: verify.OK)
    monkeypatch.setattr(
        verify.Checker, "public_dbc", lambda *_: (verify.OK, "f" * 64, 321)
    )
    monkeypatch.setattr(verify, "_json_get", lambda *_: {"info": None})
    args = (
        ["--plan", "unused.json"]
        if source == "plan"
        else ["--version", "1.2.0", "--skip", "ado,internal"]
    )
    assert verify.main(args) == 2
    assert "invalid release metadata" in capsys.readouterr().err


@pytest.mark.parametrize(
    "state",
    ["available", "empty", "wrong-name", "source-only", "yanked", "unavailable"],
)
def test_bound_inventory_requires_the_available_public_wheel(monkeypatch, state):
    version = "1.2.0"
    wheel_url = (
        "https://files.pythonhosted.org/packages/example/"
        + verify.public_pypi_wheel_name(version)
    )
    wheel = {
        "filename": verify.public_pypi_wheel_name(version),
        "packagetype": "bdist_wheel",
        "yanked": False,
        "url": wheel_url,
    }
    if state == "wrong-name":
        wheel["filename"] = "another.whl"
    elif state == "source-only":
        wheel["packagetype"] = "sdist"
    elif state == "yanked":
        wheel["yanked"] = True
    files = [] if state == "empty" else [wheel]
    monkeypatch.setattr(
        verify, "_json_get", lambda *_: {"info": {"version": version}, "urls": files}
    )
    monkeypatch.setattr(verify.Checker, "github_tag", lambda *_: (verify.OK, "a" * 40))
    monkeypatch.setattr(verify.Checker, "public_maven", lambda *_: verify.OK)
    monkeypatch.setattr(verify.Checker, "public_central_maven", lambda *_: verify.OK)
    monkeypatch.setattr(
        verify.Checker, "public_dbc", lambda *_: (verify.OK, "f" * 64, 321)
    )
    requested = []

    def exists(url, headers):
        requested.append(url)
        assert headers == {"User-Agent": "synapseml-release-verify"}
        return state != "unavailable"

    monkeypatch.setattr(verify, "_url_exists", exists)
    plan = verify.build_plan(
        version, target_keys=["master"], oss_commits={"master": "a" * 40}
    )
    rows, complete = verify.run_plan(plan)
    assert complete is (state == "available")
    pypi = next(row for row in rows if row["kind"] == "pypi")
    assert pypi["status"] == (verify.OK if state == "available" else verify.MISSING)
    assert requested == ([wheel_url] if state in ("available", "unavailable") else [])


def test_historical_pypi_check_keeps_its_metadata_only_scope(monkeypatch):
    monkeypatch.setattr(
        verify, "_json_get", lambda *_: {"info": {"version": "1.2.0"}, "urls": []}
    )

    def forbidden(*_):
        pytest.fail("Historical inventory must not start strict wheel downloads")

    monkeypatch.setattr(verify, "_url_exists", forbidden)
    checker = verify.Checker(None, None, ["ado", "internal"])
    assert checker.public_pypi("1.2.0") == verify.OK


@pytest.mark.parametrize(
    "download",
    [
        None,
        "http://files.pythonhosted.org/packages/synapseml-1.2.0-py2.py3-none-any.whl",
        "https://example.invalid/packages/synapseml-1.2.0-py2.py3-none-any.whl",
        "https://user@files.pythonhosted.org/packages/synapseml-1.2.0-py2.py3-none-any.whl",
        "https://files.pythonhosted.org:invalid/packages/synapseml-1.2.0-py2.py3-none-any.whl",
        "https://files.pythonhosted.org/packages/another.whl",
        "https://files.pythonhosted.org/other/synapseml-1.2.0-py2.py3-none-any.whl",
        "https://files.pythonhosted.org/packages/synapseml-1.2.0-py2.py3-none-any.whl?other=1",
        "https://files.pythonhosted.org/packages/synapseml-1.2.0-py2.py3-none-any.whl#other",
    ],
)
def test_strict_pypi_rejects_untrusted_download_urls(monkeypatch, download):
    monkeypatch.setattr(
        verify,
        "_json_get",
        lambda *_: {
            "info": {"version": "1.2.0"},
            "urls": [
                {
                    "filename": verify.public_pypi_wheel_name("1.2.0"),
                    "packagetype": "bdist_wheel",
                    "yanked": False,
                    "url": download,
                }
            ],
        },
    )

    def forbidden(*_):
        pytest.fail("An untrusted download URL must not be requested")

    monkeypatch.setattr(verify, "_url_exists", forbidden)
    checker = verify.Checker(None, None, ["ado", "internal"])
    with pytest.raises(RuntimeError, match="wheel download URL"):
        checker.public_pypi("1.2.0", True)


@pytest.mark.parametrize("files", [None, {}, [None], ["invalid"]])
def test_strict_pypi_rejects_malformed_file_lists(monkeypatch, files):
    monkeypatch.setattr(
        verify, "_json_get", lambda *_: {"info": {"version": "1.2.0"}, "urls": files}
    )
    checker = verify.Checker(None, None, ["ado", "internal"])
    with pytest.raises(RuntimeError, match="invalid release file list"):
        checker.public_pypi("1.2.0", True)


def test_strict_pypi_respects_public_skip_before_lookup(monkeypatch):
    def forbidden(*_):
        pytest.fail("Skipped public verification must not query PyPI")

    monkeypatch.setattr(verify, "_json_get", forbidden)
    checker = verify.Checker(None, None, ["ado", "internal", "public"])
    assert checker.public_pypi("1.2.0", True) == verify.SKIPPED


@pytest.mark.parametrize(
    "repository,module,version,suffixes",
    [
        ("public", "synapseml", "1.1.3-spark4.0", [".pom", ".jar"]),
        ("public", "synapseml-core", "1.1.3-spark4.0", [".pom", ".jar", "-tests.jar"]),
        ("internal", "synthetic-private-package", "1.1.3.0-spark4.1", [".pom", ".jar"]),
    ],
)
def test_maven_uses_release_specific_coordinate(
    monkeypatch, repository, module, version, suffixes
):
    requested = []

    def exists(url, headers):
        requested.append((url, headers))
        return True

    monkeypatch.setattr(verify, "_url_exists", exists)
    checker = verify.Checker(None, "github-token", ["ado"])
    status = (
        checker.public_maven(module, "2.13", version)
        if repository == "public"
        else checker.internal_maven("2.13", version)
    )
    assert status == verify.OK
    assert requested == [
        (
            "https://mmlspark.blob.core.windows.net/maven/com/microsoft/azure/"
            f"{module}_2.13/{version}/{module}_2.13-{version}{suffix}",
            {"User-Agent": "synapseml-release-verify"},
        )
        for suffix in suffixes
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


@pytest.mark.parametrize("inventory_only", [False, True])
def test_schema3_inventory_uses_approved_repository_after_profile_change(
    private_profile, tmp_path, monkeypatch, capsys, inventory_only
):
    import release_config as config

    plan = verify.build_plan(
        "1.2.0",
        target_keys=["master"],
        repositories=["internal"],
        families=["maven"],
        oss_commits={"master": "a" * 40},
        internal_commits={"master": "b" * 40},
    )
    assert plan.schema_version == 3
    approved_profile = json.loads(private_profile.read_text(encoding="utf-8"))
    changed_profile = json.loads(private_profile.read_text(encoding="utf-8"))
    changed_profile["internal_repository"][
        "id"
    ] = "88888888-8888-8888-8888-888888888888"
    other_profile = tmp_path / "other-synthetic-profile.json"
    other_profile.write_text(json.dumps(changed_profile), encoding="utf-8")
    monkeypatch.setenv(config.PROFILE_ENV, str(other_profile))
    plan_file = tmp_path / "approved-plan.json"
    plan_file.write_text(json.dumps(verify.plan_to_dict(plan)), encoding="utf-8")
    requested = []

    def get(url, _headers):
        requested.append(url)
        assert (
            "/repositories/" + approved_profile["internal_repository"]["id"] + "/"
        ) in url
        tag = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["filter"][0]
        return {"value": [{"name": "refs/" + tag, "objectId": "b" * 40}]}

    def unexpected(*_args, **_kwargs):
        pytest.fail("Synthetic inventory must not make a real network request")

    monkeypatch.setattr(verify, "_get_ado_token", lambda _token: "synthetic-token")
    monkeypatch.setattr(verify, "_json_get", get)
    monkeypatch.setattr(verify, "_url_exists", lambda *_args: True)
    monkeypatch.setattr(verify.urllib.request, "urlopen", unexpected)
    args = ["--plan", str(plan_file), "--json"]
    if inventory_only:
        args.append("--inventory-only")
    assert verify.main(args) == (0 if inventory_only else 1)
    output = capsys.readouterr()
    assert not output.err
    report = json.loads(output.out)
    assert report["inventory_complete"]
    assert len(requested) == len(plan.targets[0].internal_tags)
    assert all(
        changed_profile["internal_repository"]["id"] not in url for url in requested
    )
    assert plan.private_profile == approved_profile
