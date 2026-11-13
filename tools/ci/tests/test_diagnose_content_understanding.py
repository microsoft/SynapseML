import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from tools.ci import diagnose_content_understanding as diagnostic


@pytest.mark.parametrize("status", [400, 202, 302, 409, 500])
def test_notebook_request_and_redacted_response(tmp_path, capsys, status):
    key = 'synthetic-"key'
    requests = []
    deletions = []
    service_error = {
        "code": "InvalidRequest",
        "message": f"Rejected {key}",
        "innererror": {"code": "InvalidParameter", "message": "Service explanation"},
    }

    class Handler(BaseHTTPRequestHandler):
        def do_DELETE(self):
            deletions.append(self.path)
            self.send_response(204)
            self.end_headers()

        def do_PUT(self):
            requests.append(
                (
                    self.path,
                    self.headers["Ocp-Apim-Subscription-Key"],
                    json.loads(self.rfile.read(int(self.headers["Content-Length"]))),
                )
            )
            self.send_response(status)
            self.send_header("apim-request-id", "synthetic-request-id")
            self.send_header(
                "Location", f"http://127.0.0.1:{self.server.server_port}/redirected"
            )
            self.send_header(
                "operation-location",
                f"http://127.0.0.1:{self.server.server_port}/not-for-diagnostic-evidence",
            )
            self.send_header("Retry-After", "0")
            self.end_headers()
            body = json.dumps(
                {"error": service_error} if status == 400 else {"status": "creating"}
            ).encode()
            self.wfile.write(body if status != 500 else b"not-json")

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"Succeeded"}')

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            result = diagnostic.diagnose(
                diagnostic.ROOT,
                tmp_path,
                f"http://127.0.0.1:{server.server_port}",
                key,
            )
        finally:
            server.shutdown()
            worker.join()
    text = (tmp_path / "evidence.json").read_text()
    report = json.loads(text)
    assert result == (0 if status == 202 else 1)
    assert report["httpStatus"] == status
    assert report["requestId"] == "synthetic-request-id"
    assert len(requests) == 1
    path, actual_key, definition = requests[0]
    assert path.endswith("?api-version=2025-11-01&allowReplace=false")
    assert path.startswith(
        "/contentunderstanding/analyzers/synapseml_purchase_order_ci_"
    )
    assert "-" not in path.split("?")[0].rsplit("/", 1)[1]
    assert actual_key == key
    if status in (202, 500):
        assert deletions == [path.replace("&allowReplace=false", "")]
        assert report["cleanup"]["status"] == "deleted"
    else:
        assert deletions == []
    assert definition == report["request"]["definition"]
    assert definition["baseAnalyzerId"] == "prebuilt-document"
    assert definition["models"] == {
        "completion": "gpt-4.1",
        "embedding": "text-embedding-3-large",
    }
    assert set(definition["fieldSchema"]["fields"]) == {
        "Supplier",
        "TotalAmount",
        "RequestedDeliveryDate",
        "OrderSummary",
    }
    if status == 400:
        assert report["serviceError"] == {
            **service_error,
            "message": "Rejected [REDACTED]",
        }
    elif status == 500:
        assert report["responseWasJson"] is False
    for evidence in (text, capsys.readouterr().out):
        assert key not in evidence
        assert json.dumps(key)[1:-1] not in evidence
        assert "not-for-diagnostic-evidence" not in evidence


def test_model_inspection_only_reads_selected_resource_metadata(tmp_path, monkeypatch):
    requests = []
    models = {
        "completion": ["existing-completion"],
        "embedding": ["existing-embedding"],
    }

    def open_request(request, timeout):
        assert timeout == 60
        requests.append(request)
        body = (
            {"error": {"code": "DefaultsNotSet"}}
            if "/defaults?" in request.full_url
            else {"supportedModels": models, "otherData": "not-retained"}
        )
        response = io.BytesIO(json.dumps(body).encode())
        if "error" in body:
            raise HTTPError(request.full_url, 400, "Rejected", {}, response)
        response.code = 200
        return response

    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    assert (
        diagnostic.inspect_models(
            diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
        )
        == 0
    )
    report = json.loads((tmp_path / "models.json").read_text())
    assert [request.get_method() for request in requests] == ["GET", "GET"]
    assert report["defaults"]["error"]["code"] == "DefaultsNotSet"
    assert report["baseAnalyzer"] == {"httpStatus": 200, "supportedModels": models}


@pytest.mark.parametrize("failure", ["connect", "read"])
def test_timed_out_create_is_not_retried_and_preserves_cleanup_evidence(
    tmp_path, monkeypatch, failure
):
    response = Mock()
    response.code = 202
    response.headers = {}
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.side_effect = TimeoutError("synthetic-key")
    open_request = Mock(
        side_effect=TimeoutError("synthetic-key") if failure == "connect" else None,
        return_value=response,
    )
    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    cleanup = Mock(return_value={"status": "deleted"})
    monkeypatch.setattr(diagnostic, "cleanup_analyzer", cleanup)
    assert (
        diagnostic.diagnose(
            diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
        )
        == 1
    )
    open_request.assert_called_once()
    assert open_request.call_args.kwargs == {"timeout": 60}
    cleanup.assert_called_once()
    report = json.loads((tmp_path / "evidence.json").read_text())
    assert report["diagnosticError"] == "TimeoutError"
    assert report["requestAccepted"] is False
    assert report["cleanup"] == {"status": "deleted"}
    assert "synthetic-key" not in (tmp_path / "evidence.json").read_text()


def test_accepted_request_with_failed_cleanup_is_not_success(tmp_path, monkeypatch):
    response = io.BytesIO(b'{"status":"ready"}')
    response.code = 202
    response.headers = {}
    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=Mock(return_value=response))
    )
    monkeypatch.setattr(
        diagnostic,
        "cleanup_analyzer",
        Mock(return_value={"status": "failed", "httpStatus": 503}),
    )
    assert (
        diagnostic.diagnose(
            diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
        )
        == 1
    )
    report = json.loads((tmp_path / "evidence.json").read_text())
    assert report["requestAccepted"] is True
    assert report["provisioningSucceeded"] is True
    assert report["cleanup"]["httpStatus"] == 503


@pytest.mark.parametrize(
    "terminal,http_status",
    [
        ("Succeeded", 200),
        ("Failed", 200),
        ("Canceled", 200),
        ("unknown", 200),
        ("", 503),
    ],
)
def test_accepted_creation_waits_for_terminal_result_before_cleanup(
    tmp_path, monkeypatch, capsys, terminal, http_status
):
    requests = []
    responses = [
        (201, {"status": "creating"}),
        (200, {"status": "Running"}),
        (
            http_status,
            {
                "status": terminal,
                "error": None
                if terminal == "Succeeded"
                else {"code": "ResourceError", "message": "Missing synthetic-key"},
            },
        ),
    ]

    def open_request(request, timeout):
        assert timeout == 60
        requests.append(request)
        status, body = responses.pop(0)
        response = io.BytesIO(json.dumps(body).encode())
        response.code = status
        response.headers = {
            "operation-location": "https://example.invalid/operations/creation",
            "Retry-After": "2",
            "apim-request-id": "poll-request",
        }
        if status != 200 and request.get_method() == "GET":
            raise HTTPError(
                request.full_url, status, "Rejected", response.headers, response
            )
        return response

    def cleanup(*args):
        assert responses == []
        requests.append("cleanup")
        return {"status": "deleted"}

    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    monkeypatch.setattr(diagnostic, "cleanup_analyzer", cleanup)
    sleep = Mock()
    monkeypatch.setattr(diagnostic, "sleep", sleep, raising=False)
    result = diagnostic.diagnose(
        diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
    )
    assert result == (0 if terminal == "Succeeded" else 1)
    assert [request.get_method() for request in requests[:-1]] == ["PUT", "GET", "GET"]
    assert requests[-1] == "cleanup"
    assert [call.args for call in sleep.call_args_list] == [(2,), (2,)]
    report = json.loads((tmp_path / "evidence.json").read_text())
    assert report["requestAccepted"] is True
    assert report["provisioningSucceeded"] == (terminal == "Succeeded")
    assert report["provisioning"]["status"] == terminal
    assert report["provisioning"]["httpStatus"] == http_status
    assert report["pollCount"] == 2
    assert report["httpStatus"] == 201
    if terminal != "Succeeded":
        assert report["serviceError"]["message"] == "Missing [REDACTED]"
    assert "synthetic-key" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "location",
    [
        None,
        "https://other.invalid/operation",
        "http://example.invalid/operation",
        "https://user:password@example.invalid/operation",
    ],
)
def test_unsafe_or_missing_operation_location_is_not_polled(
    tmp_path, monkeypatch, location
):
    response = io.BytesIO(b'{"status":"creating"}')
    response.code = 201
    response.headers = {"operation-location": location} if location else {}
    open_request = Mock(return_value=response)
    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    cleanup = Mock(return_value={"status": "deleted"})
    monkeypatch.setattr(diagnostic, "cleanup_analyzer", cleanup)
    assert (
        diagnostic.diagnose(
            diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
        )
        == 1
    )
    open_request.assert_called_once()
    cleanup.assert_called_once()
    report = json.loads((tmp_path / "evidence.json").read_text())
    assert report["requestAccepted"] is True
    assert report["provisioningSucceeded"] is False
    assert report["diagnosticError"] == "ValueError"


def test_poll_timeout_preserves_acceptance_and_cleans_up_without_resubmission(
    tmp_path, monkeypatch
):
    response = io.BytesIO(b'{"status":"creating"}')
    response.code = 201
    response.headers = {
        "operation-location": "https://example.invalid/operations/creation",
        "Retry-After": "0",
    }
    open_request = Mock(side_effect=[response, TimeoutError("synthetic-key")])
    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    cleanup = Mock(return_value={"status": "deleted"})
    monkeypatch.setattr(diagnostic, "cleanup_analyzer", cleanup)
    assert (
        diagnostic.diagnose(
            diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
        )
        == 1
    )
    assert [call.args[0].get_method() for call in open_request.call_args_list] == [
        "PUT",
        "GET",
    ]
    cleanup.assert_called_once()
    report = json.loads((tmp_path / "evidence.json").read_text())
    assert report["requestAccepted"] is True
    assert report["provisioningSucceeded"] is False
    assert report["diagnosticError"] == "TimeoutError"
    assert "synthetic-key" not in (tmp_path / "evidence.json").read_text()


@pytest.mark.parametrize(
    "scenario",
    [
        "unset",
        "merge",
        "unchanged",
        "conflict",
        "disabled",
        "unsupported",
        "defaults-forbidden",
        "patch-rejected",
        "readback-mismatch",
    ],
)
def test_defaults_setup_preserves_existing_mappings_and_fails_closed(
    tmp_path, monkeypatch, scenario
):
    models = {"completion": ["gpt-4.1"], "embedding": ["text-embedding-3-large"]}
    expected = {
        "gpt-4.1": "gpt-4.1",
        "prebuilt-analyzer-completion": "gpt-4.1",
        "text-embedding-3-large": "text-embedding-3-large",
        "prebuilt-analyzer-embedding": "text-embedding-3-large",
    }
    deployments = [
        {"deployment": model, "model": {"name": model}, "state": "Succeeded"}
        for model in ("gpt-4.1", "text-embedding-3-large")
    ]
    before = (
        {}
        if scenario == "unset"
        else {"unrelated-model": "keep-this-deployment", "gpt-4.1": "gpt-4.1"}
    )
    if scenario == "unchanged":
        before.update(expected)
    if scenario == "conflict":
        before["prebuilt-analyzer-completion"] = "someone-elses-deployment"
    if scenario == "disabled":
        deployments[0]["state"] = "Disabled"
    if scenario == "unsupported":
        models["completion"] = []
    (tmp_path / "deployments.json").write_text(json.dumps(deployments))
    state = before.copy()
    requests = []
    patched = False

    def open_request(request, timeout):
        nonlocal patched
        assert timeout == 60
        requests.append(request)
        status = 200
        if request.get_method() == "PATCH":
            assert request.headers["Content-type"] == "application/merge-patch+json"
            changes = json.loads(request.data)["modelDeployments"]
            assert changes == {
                name: value for name, value in expected.items() if name not in before
            }
            if scenario == "patch-rejected":
                status, body = 400, {"error": {"code": "InvalidRequest"}}
            else:
                state.update(changes)
                patched = True
                body = {"modelDeployments": state}
        elif "/defaults?" in request.full_url:
            if scenario == "defaults-forbidden":
                status, body = 403, {"error": {"code": "Forbidden"}}
            elif not state:
                status, body = 400, {
                    "error": {"innererror": {"code": "DefaultsNotSet"}}
                }
            else:
                body = {"modelDeployments": state}
                if patched and scenario == "readback-mismatch":
                    body = {
                        "modelDeployments": {
                            name: value
                            for name, value in state.items()
                            if name != "unrelated-model"
                        }
                    }
        else:
            body = {"supportedModels": models}
        response = io.BytesIO(json.dumps(body).encode())
        response.code = status
        response.headers = {"apim-request-id": "setup-request"}
        if status != 200:
            raise HTTPError(
                request.full_url, status, "Rejected", response.headers, response
            )
        return response

    monkeypatch.setattr(
        diagnostic, "build_opener", lambda *args: Mock(open=open_request)
    )
    result = diagnostic.configure_defaults(
        diagnostic.ROOT, tmp_path, "https://example.invalid", "synthetic-key"
    )
    report = json.loads((tmp_path / "defaults.json").read_text())
    success = scenario in ("unset", "merge", "unchanged")
    assert result == (0 if success else 1)
    assert (report["status"] != "failed") == success
    methods = [request.get_method() for request in requests]
    if scenario in ("unset", "merge", "readback-mismatch"):
        assert methods == ["GET", "GET", "PATCH", "GET"]
    elif scenario == "patch-rejected":
        assert methods == ["GET", "GET", "PATCH"]
        assert report["serviceError"]["code"] == "InvalidRequest"
    elif scenario == "unchanged":
        assert methods == ["GET", "GET", "GET"]
    else:
        assert methods == ["GET", "GET"]
    if success:
        assert report["after"] == {**before, **expected}
    assert "synthetic-key" not in (tmp_path / "defaults.json").read_text()
