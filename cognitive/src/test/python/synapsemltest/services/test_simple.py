# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql.functions import struct
from pyspark.sql.types import StringType, StructType
from synapse.ml.core.init_spark import init_spark
from synapse.ml.io.http import (
    JSONInputParser,
    JSONOutputParser,
    SimpleHTTPTransformer,
)

spark = init_spark()


class SimpleHTTPTransformerSmokeTest(unittest.TestCase):
    def test_simple(self) -> None:
        requests: list[tuple[str, dict[str, str]]] = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((self.path, body))
                payload = json.dumps(
                    {"status": "success", "message": body["data"]}
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        values = [f"row-{index}" for index in range(20)]
        df = spark.createDataFrame([(value,) for value in values], ["data"]).withColumn(
            "inputs",
            struct("data"),
        )
        response_schema = (
            StructType().add("status", StringType()).add("message", StringType())
        )

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = (
                    SimpleHTTPTransformer()
                    .setInputCol("inputs")
                    .setInputParser(JSONInputParser())
                    .setOutputParser(JSONOutputParser().setDataType(response_schema))
                    .setOutputCol("results")
                    .setErrorCol("error")
                    .setUrl(f"http://127.0.0.1:{server.server_port}/echo")
                )
                rows = client.transform(df).select("data", "results", "error").collect()
                self.assertCountEqual([row.data for row in rows], values)
                for row in rows:
                    self.assertIsNone(row.error)
                    self.assertIsNotNone(row.results)
                    self.assertEqual(row.results.status, "success")
                    self.assertEqual(row.results.message, row.data)
                self.assertCountEqual(
                    requests, [("/echo", {"data": value}) for value in values]
                )
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    result = unittest.main()
