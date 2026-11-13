import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tools.ci import diagnose_content_understanding as diagnostic


@pytest.mark.parametrize("status", [400, 202, 302, 500])
def test_notebook_request_and_redacted_response(tmp_path, capsys, status):
    key = 'synthetic-"key'
    requests = []
    service_error = {
        "code": "InvalidRequest",
        "message": f"Rejected {key}",
        "innererror": {"code": "InvalidParameter", "message": "Service explanation"},
    }

    class Handler(BaseHTTPRequestHandler):
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
            self.send_header("operation-location", "not-for-diagnostic-evidence")
            self.end_headers()
            body = json.dumps(
                {"error": service_error} if status == 400 else {"status": "creating"}
            ).encode()
            self.wfile.write(body if status != 500 else b"not-json")

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
    assert definition == report["request"]["definition"]
    assert definition["baseAnalyzerId"] == "prebuilt-document"
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
