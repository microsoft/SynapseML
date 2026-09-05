# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in live Content Understanding tests using synthetic documents only.

Set CONTENT_UNDERSTANDING_ENDPOINT and either CONTENT_UNDERSTANDING_API_KEY
or CONTENT_UNDERSTANDING_AAD_TOKEN through the test process environment.
CONTENT_UNDERSTANDING_TEST_PREVIEW=1 enables the preview case.
On Fabric, set CONTENT_UNDERSTANDING_TEST_FORMAT=delta and
CONTENT_UNDERSTANDING_TEST_OUTPUT_ROOT to a scratch lakehouse Files path.
The suite creates and removes one uniquely named table and one journal path.
It never provisions analyzers or changes model deployments or resource defaults.
"""

import json
import os
import tempfile
import unittest
import uuid
from contextlib import contextmanager

from py4j.protocol import Py4JJavaError

from synapsemltest.services.content_understanding_fixtures import (
    DOCX_FIRST_PAGE,
    DOCX_MIME_TYPE,
    DOCX_SECOND_PAGE,
    synthetic_docx,
    synthetic_pdf,
)


GA_VERSION = "2025-11-01"
PREVIEW_VERSION = "2026-06-01-preview"


class TestContentUnderstandingE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.environ.get("CONTENT_UNDERSTANDING_ENDPOINT")
        cls.key = os.environ.get("CONTENT_UNDERSTANDING_API_KEY")
        cls.token = os.environ.get("CONTENT_UNDERSTANDING_AAD_TOKEN")
        if not cls.endpoint or not (cls.key or cls.token):
            raise unittest.SkipTest(
                "Content Understanding live tests require an explicit endpoint "
                "and API key or AAD token in the test process environment."
            )
        from synapse.ml.core.init_spark import init_spark
        from synapse.ml.services.contentunderstanding import ContentUnderstanding

        cls.spark = init_spark()
        cls.stage_type = ContentUnderstanding
        cls.format = os.environ.get("CONTENT_UNDERSTANDING_TEST_FORMAT", "parquet")
        if cls.format not in ("delta", "parquet"):
            raise ValueError(
                "CONTENT_UNDERSTANDING_TEST_FORMAT must be delta or parquet"
            )

    def analyzer(self):
        stage = (
            self.stage_type()
            .setEndpoint(self.endpoint)
            .setAnalyzerId("prebuilt-read")
            .setDocumentBytesCol("body")
            .setDocumentNameCol("documentId")
            .setMimeTypeCol("mimeType")
            .setRangeCol("pageRange")
            .setApiVersionCol("apiVersion")
            .setOutputCol("analysis")
            .setErrorCol("requestError")
            .setMaxPollAttempts(120)
            .setPollingDelay(1000)
        )
        if self.key:
            return stage.setSubscriptionKey(self.key)
        return stage.setAADToken(self.token)

    def documents(self, rows):
        return self.spark.createDataFrame(
            rows,
            "documentId string, body binary, mimeType string, "
            "pageRange string, apiVersion string",
        ).coalesce(1)

    def pdf_document(self, pages="2"):
        return self.documents(
            [
                (
                    "synthetic.pdf",
                    bytearray(synthetic_pdf()),
                    "application/pdf",
                    pages,
                    GA_VERSION,
                )
            ]
        )

    def docx_document(self, version=GA_VERSION):
        return self.documents(
            [
                (
                    "synthetic.docx",
                    bytearray(synthetic_docx()),
                    DOCX_MIME_TYPE,
                    None,
                    version,
                )
            ]
        )

    def successful_body(self, response):
        self.assertEqual(response.status, "Succeeded")
        self.assertIsNone(response.error)
        body = json.loads(response.rawResponse)
        self.assertEqual(body["status"], "Succeeded")
        self.assertTrue(body["result"]["contents"])
        self.assertTrue(body["usage"])
        return body

    def assert_docx_content(self, body):
        contents = body["result"]["contents"]
        markdown = "\n".join(content["markdown"] for content in contents)
        for text in (
            DOCX_FIRST_PAGE,
            DOCX_SECOND_PAGE,
            "Synthetic widgets",
            "42.00 USD",
        ):
            self.assertIn(text, markdown)
        self.assertEqual(contents[0]["mimeType"], DOCX_MIME_TYPE)

    def record(self, case, body, **facts):
        contents = body["result"]["contents"]
        print(
            "CONTENT_UNDERSTANDING_E2E="
            + json.dumps(
                {
                    "case": case,
                    "status": body["status"],
                    "apiVersion": body["result"]["apiVersion"],
                    "usage": body["usage"],
                    "pages": [
                        page["pageNumber"]
                        for content in contents
                        for page in content.get("pages", [])
                    ],
                    **facts,
                },
                sort_keys=True,
            )
        )

    @contextmanager
    def output_path(self):
        root = os.environ.get("CONTENT_UNDERSTANDING_TEST_OUTPUT_ROOT")
        if not root:
            with tempfile.TemporaryDirectory(prefix="cu-e2e-") as directory:
                yield os.path.join(directory, "journal")
        else:
            path = root.rstrip("/") + "/journal-" + uuid.uuid4().hex
            jpath = self.spark._jvm.org.apache.hadoop.fs.Path(path)
            fs = jpath.getFileSystem(
                self.spark._jsparkSession.sessionState().newHadoopConf()
            )
            if fs.exists(jpath):
                raise RuntimeError(
                    "The unique Content Understanding test path already exists"
                )
            try:
                yield path
            finally:
                if fs.exists(jpath) and not fs.delete(jpath, True):
                    raise RuntimeError(
                        "Could not remove the Content Understanding test journal"
                    )

    def test_pdf_range_through_generated_transformer(self):
        row = self.analyzer().transform(self.pdf_document()).first()
        self.assertIsNone(row.requestError)
        body = self.successful_body(row.analysis)
        pages = [
            page["pageNumber"]
            for content in body["result"]["contents"]
            for page in content["pages"]
        ]
        self.assertEqual(pages, [2])
        self.assertEqual(body["usage"]["documentPagesBasic"], 1)
        markdown = "\n".join(
            content["markdown"] for content in body["result"]["contents"]
        )
        self.assertIn("CU-002", markdown)
        self.assertNotIn("CU-001", markdown)
        self.record("pdf-ga-range", body)

    def test_docx_text_and_table_through_generated_transformer(self):
        row = self.analyzer().transform(self.docx_document()).first()
        self.assertIsNone(row.requestError)
        body = self.successful_body(row.analysis)
        self.assert_docx_content(body)
        self.record("docx-ga-whole-document", body)

    @unittest.skipUnless(
        os.environ.get("CONTENT_UNDERSTANDING_TEST_PREVIEW") == "1",
        "Set CONTENT_UNDERSTANDING_TEST_PREVIEW=1 to opt into the preview API",
    )
    def test_preview_docx_layout_preserves_metadata(self):
        stage = self.analyzer().setAnalyzerId("prebuilt-layout")
        row = stage.transform(self.docx_document(PREVIEW_VERSION)).first()
        body = self.successful_body(row.analysis)
        self.assert_docx_content(body)
        self.assertEqual(body["result"]["apiVersion"], PREVIEW_VERSION)
        self.assertTrue(
            any("metadata" in content for content in body["result"]["contents"])
        )
        self.record("docx-preview-layout", body, metadataPreserved=True)

    def test_completed_docx_survives_a_later_pdf_response_limit_and_resumes(self):
        table = "cu_e2e_" + uuid.uuid4().hex
        documents = self.documents(
            [
                (
                    "a.docx",
                    bytearray(synthetic_docx()),
                    DOCX_MIME_TYPE,
                    None,
                    GA_VERSION,
                ),
                (
                    "b.pdf",
                    bytearray(synthetic_pdf()),
                    "application/pdf",
                    "3-4",
                    GA_VERSION,
                ),
            ]
        )
        stage = (
            self.analyzer()
            .setMaxResponseBytes(2048)
            .setOutputCol("body")
            .setErrorCol("mimeType")
        )
        try:
            with self.assertRaises(Py4JJavaError) as raised:
                stage.writeToTable(documents, "documentId", table, self.format)
            error = raised.exception.java_exception
            self.assertEqual(
                str(error.getClass().getName()),
                "com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstandingException",
            )
            self.assertEqual(
                json.loads(str(error.response().error().get()))["code"],
                "ResponseTooLarge",
            )
            partial = {
                row.documentId: row
                for row in stage.readTable(self.spark, table).collect()
            }
            self.assertEqual(partial["a.docx"].status, "Succeeded")
            self.assertEqual(partial["b.pdf"].status, "Running")
            self.assertTrue(partial["b.pdf"].operationLocation)
            self.assertEqual(partial["b.pdf"].sequence, 0)
            self.assert_docx_content(self.successful_body(partial["a.docx"]))

            stage.setMaxResponseBytes(32 * 1024 * 1024)
            resumed = {
                row.documentId: row
                for row in stage.writeToTable(
                    documents, "documentId", table, self.format
                ).collect()
            }
            for document_id, response in resumed.items():
                self.assertEqual(response.status, "Succeeded")
                self.assertEqual(
                    response.operationLocation, partial[document_id].operationLocation
                )
                self.assertEqual(response.sequence, 1)
            self.assertEqual(
                resumed["a.docx"].rawResponse, partial["a.docx"].rawResponse
            )
            body = self.successful_body(resumed["b.pdf"])
            pages = [
                page["pageNumber"]
                for content in body["result"]["contents"]
                for page in content["pages"]
            ]
            self.assertEqual(pages, [3, 4])
            self.assertEqual(self.spark.table(table).count(), 4)
            self.record(
                "mixed-table-recovery",
                body,
                preservedDocx=True,
                sameHandles=True,
                journalRows=4,
            )
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS `{table}`")

    def test_submit_only_path_resumes_the_original_pdf_operation(self):
        stage = (
            self.analyzer()
            .setOperationMode("submit")
            .setOutputCol("body")
            .setErrorCol("documentId")
        )
        documents = self.pdf_document()
        with self.output_path() as path:
            submitted = stage.writeToPath(
                documents, "documentId", path, self.format
            ).first()
            self.assertEqual(submitted.sequence, 0)
            self.assertTrue(submitted.operationLocation)
            self.assertEqual(submitted.status, "Running")
            stage.setOperationMode("analyze")
            resumed = stage.writeToPath(
                documents, "documentId", path, self.format
            ).first()
            self.assertEqual(resumed.operationLocation, submitted.operationLocation)
            self.assertEqual(resumed.sequence, 1)
            body = self.successful_body(resumed)
            stage.writeToPath(documents, "documentId", path, self.format)
            self.assertEqual(stage.readPath(self.spark, path, self.format).count(), 1)
            self.assertEqual(self.spark.read.format(self.format).load(path).count(), 2)
            self.record(
                "submit-only-path-recovery", body, sameHandle=True, journalRows=2
            )


if __name__ == "__main__":
    unittest.main()
