# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import hashlib
import io
import urllib.error
import urllib.request

import pytest

import release_matrix as matrix
import release_guard as guard
import verify_release as verify


@pytest.fixture
def content_case():
    plan = matrix.build_plan(
        "1.2.0", target_keys=["master"], oss_commits={"master": "a" * 40}
    )
    target = plan.targets[0]
    artifacts, blob_artifacts, bodies = [], [], {}
    for module in verify.PUBLIC_MAVEN_MODULES:
        module = f"{module}_{target.scala}"
        suffixes = [".pom", ".jar"]
        if module == f"synapseml-core_{target.scala}":
            suffixes.append("-tests.jar")
        for suffix in suffixes:
            path = f"{module}/{module}-{target.oss_maven_version}{suffix}"
            for destination, base, output in (
                ("maven", verify.MAVEN_BASE, blob_artifacts),
                ("maven-central", verify.MAVEN_CENTRAL_BASE, artifacts),
            ):
                body = (destination + "/" + path).encode()
                output.append(
                    {
                        "path": path,
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "size": len(body),
                    }
                )
                filename = path.split("/")[1]
                url = (
                    f"{base}/com/microsoft/azure/{module}/"
                    f"{target.oss_maven_version}/{filename}"
                )
                bodies[url] = body
    wheel = verify.public_pypi_wheel_name(plan.oss_version)
    wheel_url = f"https://files.pythonhosted.org/packages/example/{wheel}"
    body = b"synthetic-published-wheel"
    bodies[wheel_url] = body
    wheel_record = {
        "path": f"pypi/{wheel}",
        "sha256": hashlib.sha256(body).hexdigest(),
        "size": len(body),
    }
    artifacts.append(wheel_record)
    document = {
        "schema_version": 2,
        "artifacts": artifacts,
        "blob_artifacts": blob_artifacts,
    }
    metadata = {
        "info": {"version": plan.oss_version},
        "urls": [
            {
                "filename": wheel,
                "packagetype": "bdist_wheel",
                "yanked": False,
                "url": wheel_url,
                "size": len(body),
                "digests": {"sha256": wheel_record["sha256"]},
            }
        ],
    }
    return plan, target, document, bodies, metadata


@pytest.fixture
def content_http(monkeypatch, content_case):
    _, _, _, bodies, metadata = content_case
    requested = []

    class Response(io.BytesIO):
        headers = {}

    def open_public(_self, request, **_kwargs):
        assert request.get_method() == "GET"
        assert "Authorization" not in request.headers
        requested.append(request.full_url)
        return Response(bodies[request.full_url])

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", open_public)
    monkeypatch.setattr(verify, "_json_get", lambda *_: copy.deepcopy(metadata))
    return requested


def test_independent_cdn_and_central_bytes_match_their_own_receipts(
    content_case, content_http
):
    plan, target, document, _, _ = content_case
    assert document["artifacts"][0]["sha256"] != document["blob_artifacts"][0]["sha256"]
    cache = {}
    rows = verify.collect_public_artifact_content(plan, target, document, cache)
    verify.validate_public_artifact_content(plan, target, document, rows)
    assert verify.collect_public_artifact_content(plan, target, document, cache) == rows
    assert len(content_http) == len(set(content_http)) == 31
    assert {row["destination"] for row in rows} == {"maven", "maven-central", "pypi"}


@pytest.mark.parametrize("fault", ["empty", "same-size", "truncated", "oversized"])
@pytest.mark.parametrize("destination", ["maven", "maven-central", "pypi"])
def test_public_bytes_must_match_producer_receipts(
    content_case, content_http, fault, destination
):
    plan, target, document, bodies, _ = content_case
    prefix = {
        "maven": verify.MAVEN_BASE,
        "maven-central": verify.MAVEN_CENTRAL_BASE,
        "pypi": "https://files.pythonhosted.org",
    }[destination]
    url = next(url for url in bodies if url.startswith(prefix))
    body = bodies[url]
    bodies[url] = {
        "empty": b"",
        "same-size": b"x" * len(body),
        "truncated": body[:-1],
        "oversized": body + b"x",
    }[fault]
    with pytest.raises(ValueError, match="content|size|hash"):
        verify.collect_public_artifact_content(plan, target, document)


@pytest.mark.parametrize("fault", ["legacy", "missing-cdn", "swapped", "duplicate"])
def test_destination_specific_receipts_are_required(content_case, content_http, fault):
    plan, target, document, _, _ = content_case
    if fault == "legacy":
        document["schema_version"] = 1
    elif fault == "missing-cdn":
        document["blob_artifacts"].pop()
    elif fault == "swapped":
        document["blob_artifacts"][0]["sha256"] = document["artifacts"][0]["sha256"]
    else:
        document["blob_artifacts"].append(copy.deepcopy(document["blob_artifacts"][0]))
    with pytest.raises(ValueError):
        verify.collect_public_artifact_content(plan, target, document)


@pytest.mark.parametrize("fault", ["missing", "yanked", "hash", "size"])
def test_pypi_metadata_cannot_contradict_download_receipt(
    content_case, content_http, fault
):
    plan, target, document, _, metadata = content_case
    wheel = metadata["urls"][0]
    if fault == "missing":
        metadata["urls"] = []
    elif fault == "yanked":
        wheel["yanked"] = True
    elif fault == "hash":
        wheel["digests"]["sha256"] = "0" * 64
    else:
        wheel["size"] += 1
    with pytest.raises(ValueError, match="PyPI"):
        verify.collect_public_artifact_content(plan, target, document)


@pytest.mark.parametrize("fault", ["omit", "duplicate", "destination", "hash", "size"])
def test_observation_validation_is_exact(content_case, content_http, fault):
    plan, target, document, _, _ = content_case
    rows = verify.collect_public_artifact_content(plan, target, document)
    if fault == "omit":
        rows.pop()
    elif fault == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    elif fault == "destination":
        rows[0]["destination"] = "other"
    elif fault == "hash":
        rows[0]["sha256"] = "0" * 64
    else:
        rows[0]["size"] += 1
    with pytest.raises(ValueError):
        verify.validate_public_artifact_content(plan, target, document, rows)


def test_download_cache_does_not_hide_conflicting_expected_hash(
    content_case, content_http
):
    plan, target, document, _, _ = content_case
    cache = {}
    verify.collect_public_artifact_content(plan, target, document, cache)
    document["blob_artifacts"][0]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="content|hash"):
        verify.collect_public_artifact_content(plan, target, document, cache)
    assert len(content_http) == 31


def test_artifact_redirects_are_rejected():
    with pytest.raises(ValueError, match="redirect"):
        verify.PublicArtifactRedirects().redirect_request(
            None, None, 302, "", {}, "https://example.invalid"
        )


def test_download_error_does_not_echo_response_details(content_case, monkeypatch):
    plan, target, document, _, _ = content_case

    def fail(*_args, **_kwargs):
        raise urllib.error.HTTPError(
            "https://example.invalid", 500, "untrusted-response", {}, None
        )

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fail)
    with pytest.raises(ValueError, match="HTTP 500") as error:
        verify.collect_public_artifact_content(plan, target, document)
    assert "untrusted-response" not in str(error.value)


@pytest.mark.parametrize("schema", [2, 4])
def test_content_proof_does_not_change_plan_identity(
    content_case, content_http, schema
):
    plan, target, document, _, _ = content_case
    serialized = matrix.plan_to_dict(plan)
    serialized["schema_version"] = schema
    serialized["plan_id"] = matrix.plan_digest(serialized)
    plan = matrix.load_plan(serialized, require_bound=True)
    verify.collect_public_artifact_content(plan, target, document)
    assert matrix.plan_to_dict(plan) == serialized


@pytest.mark.parametrize("changed", [False, True])
def test_notes_recheck_downloads_against_each_destination(
    content_case, content_http, monkeypatch, changed
):
    plan, target, document, bodies, _ = content_case
    document["target"] = target.key
    report = {"producer_evidence": {"runs": [{"provenance": [document]}]}}
    notebook_checks = []
    monkeypatch.setattr(
        guard, "verify_notebook_downloads", lambda *_: notebook_checks.append(True)
    )
    if changed:
        url = next(url for url in bodies if url.startswith(verify.MAVEN_BASE))
        bodies[url] = b"changed"
        with pytest.raises(ValueError, match="content"):
            guard.verify_public_downloads(plan, report)
        assert not notebook_checks
    else:
        guard.verify_public_downloads(plan, report)
        assert notebook_checks == [True]
        assert len(content_http) == 31


@pytest.mark.parametrize("fault", [None, "empty", "oversized", "missing", "service"])
def test_api_documentation_pages_are_bounded_anonymous_gets(monkeypatch, fault):
    requested = []

    def open_page(_self, request, **_kwargs):
        assert request.get_method() == "GET"
        assert "Authorization" not in request.headers
        requested.append(request.full_url)
        if fault in {"missing", "service"}:
            raise urllib.error.HTTPError(
                request.full_url, 404 if fault == "missing" else 500, "", {}, None
            )
        body = b"<html>API reference</html>"
        if fault == "empty":
            body = b""
        elif fault == "oversized":
            body = b"x" * (5 * 1024 * 1024 + 1)
        return io.BytesIO(body)

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", open_page)
    if fault:
        with pytest.raises(ValueError, match="API documentation"):
            verify.verify_public_api_docs("1.2.0")
    else:
        verify.verify_public_api_docs("1.2.0")
        assert requested == [
            verify.DOCS_BASE + "/1.2.0/" + page
            for page in (
                "pyspark/index.html",
                "scala/index.html",
                "scala/com/microsoft/azure/synapse/ml/index.html",
            )
        ]


@pytest.mark.parametrize("target_key", ["master", "spark4.1"])
def test_api_docs_guard_uses_only_the_approved_primary_source(monkeypatch, target_key):
    plan = matrix.build_plan(
        "1.2.0", target_keys=[target_key], oss_commits={target_key: "a" * 40}
    )
    target = plan.targets[0]
    calls = []
    monkeypatch.setattr(guard, "maven_plan", lambda *_: (plan, target))
    monkeypatch.setattr(guard, "validate_checkout", lambda *_: calls.append("checkout"))
    monkeypatch.setattr(
        guard, "verify_public_api_docs", lambda version: calls.append(version)
    )
    assert guard.main(["api-docs"]) == (0 if target_key == "master" else 2)
    assert calls == (["checkout", "1.2.0"] if target_key == "master" else ["checkout"])
