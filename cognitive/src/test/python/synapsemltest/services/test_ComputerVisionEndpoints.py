# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.vision.AnalyzeImage import AnalyzeImage

spark = init_spark()


class TestComputerVisionEndpoints(unittest.TestCase):
    def test_generated_wrapper_uses_v32_and_preserves_explicit_url_on_copy(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                uri = urlsplit(self.path)
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((uri.path, parse_qs(uri.query), body))
                if uri.path in ("/vision/v3.2/analyze", "/explicit/analyze"):
                    status = 200
                    response = {
                        "requestId": "local-request",
                        "metadata": {"width": 100, "height": 50, "format": "Jpeg"},
                        "description": {"tags": ["cat"], "captions": []},
                        "modelVersion": "2021-05-01",
                    }
                else:
                    status = 410
                    response = {"error": {"code": "ApiVersionRetired"}}
                payload = json.dumps(response).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            endpoint = "http://127.0.0.1:{}".format(server.server_port)
            stage = (
                AnalyzeImage()
                .setEndpoint(endpoint + "/")
                .setImageUrlCol("image")
                .setVisualFeatures(["Description"])
                .setOutputCol("response")
                .setErrorCol("error")
                .setConcurrency(1)
            )
            image_url = "https://example.org/image.jpg"
            data = spark.createDataFrame([(image_url,)], ["image"]).coalesce(1)
            default_result = stage.transform(data).head()
            self.assertIsNone(default_result.error)
            self.assertEqual(default_result.response.description.tags, ["cat"])

            explicit_url = endpoint + "/explicit/analyze"
            copied = stage.setLocation("eastus").setUrl(explicit_url).copy()
            self.assertEqual(copied.getUrl(), explicit_url)
            override_result = copied.transform(data).head()
            self.assertIsNone(override_result.error)
            self.assertEqual(override_result.response.description.tags, ["cat"])
            self.assertEqual(
                requests,
                [
                    (path, {"visualFeatures": ["Description"]}, {"url": image_url})
                    for path in ("/vision/v3.2/analyze", "/explicit/analyze")
                ],
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
