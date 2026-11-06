# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Opt-in live CU tests using synthetic documents, without provisioning resources.

Set CONTENT_UNDERSTANDING_ENDPOINT and either CONTENT_UNDERSTANDING_API_KEY
or CONTENT_UNDERSTANDING_AAD_TOKEN in the test process environment.
CONTENT_UNDERSTANDING_TEST_PREVIEW=1 enables the preview case.
This suite makes billable analysis requests but creates no tables or analyzers.
The invalid-key and missing-analyzer cases are rejected before analysis.
"""

import json
import os
import unittest
import uuid

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
                "Live CU tests require an explicit endpoint and key or AAD token."
            )
        from synapse.ml.core.init_spark import init_spark
        from synapse.ml.services.contentunderstanding import ContentUnderstanding

        cls.spark = init_spark()
        cls.stage_type = ContentUnderstanding

    def analyzer(self, key=None):
        stage = (
            self.stage_type()
            .setEndpoint(self.endpoint)
            .setDocumentBytesCol("body")
            .setDocumentNameCol("documentId")
            .setMimeTypeCol("mimeType")
            .setRangeCol("pageRange")
            .setApiVersionCol("apiVersion")
            .setOutputCol("analysis")
            .setErrorCol("requestError")
        )
        key = key or self.key
        return stage.setSubscriptionKey(key) if key else stage.setAADToken(self.token)

    def documents(self, content, name, mime_type, pages=None, version=GA_VERSION):
        return self.spark.createDataFrame(
            [(name, bytearray(content), mime_type, pages, version)],
            "documentId string, body binary, mimeType string, "
            "pageRange string, apiVersion string",
        ).coalesce(1)

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

    def test_pdf_range_through_generated_transformer(self):
        documents = self.documents(
            synthetic_pdf(), "synthetic.pdf", "application/pdf", "2"
        )
        row = self.analyzer().transform(documents).first()
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

    def test_docx_text_and_table_through_generated_transformer(self):
        documents = self.documents(synthetic_docx(), "synthetic.docx", DOCX_MIME_TYPE)
        row = self.analyzer().transform(documents).first()
        self.assertIsNone(row.requestError)
        self.assert_docx_content(self.successful_body(row.analysis))

    @unittest.skipUnless(
        os.environ.get("CONTENT_UNDERSTANDING_TEST_PREVIEW") == "1",
        "Set CONTENT_UNDERSTANDING_TEST_PREVIEW=1 to opt into the preview API.",
    )
    def test_preview_docx_layout_preserves_metadata(self):
        documents = self.documents(
            synthetic_docx(), "synthetic.docx", DOCX_MIME_TYPE, version=PREVIEW_VERSION
        )
        row = (
            self.analyzer()
            .setAnalyzerId("prebuilt-layout")
            .transform(documents)
            .first()
        )
        body = self.successful_body(row.analysis)
        self.assert_docx_content(body)
        self.assertEqual(body["result"]["apiVersion"], PREVIEW_VERSION)
        self.assertTrue(
            any("metadata" in content for content in body["result"]["contents"])
        )

    def test_submit_then_poll_uses_the_original_operation(self):
        stage = self.analyzer().setOperationMode("submit")
        documents = self.documents(
            synthetic_pdf(), "synthetic.pdf", "application/pdf", "2"
        )
        submitted = stage.transform(documents).first().analysis
        self.assertTrue(submitted.operationLocation)
        poller = (
            stage.copy({})
            .setOperationMode("poll")
            .setOperationLocation(submitted.operationLocation)
        )
        resumed = poller.transform(self.spark.range(1)).first().analysis
        self.assertEqual(resumed.operationLocation, submitted.operationLocation)
        self.successful_body(resumed)

    def test_mixed_batch_keeps_row_identity_across_partitions(self):
        pdf = bytearray(synthetic_pdf())
        documents = self.spark.createDataFrame(
            [
                ("first", pdf, "application/pdf", "1", GA_VERSION),
                ("missing", None, "application/pdf", None, GA_VERSION),
                ("third", pdf, "application/pdf", "3", GA_VERSION),
            ],
            "documentId string, body binary, mimeType string, "
            "pageRange string, apiVersion string",
        ).repartition(2)
        rows = {
            row.documentId: row
            for row in self.analyzer().transform(documents).collect()
        }
        self.assertEqual(rows["missing"].analysis.status, "Failed")
        self.assertEqual(rows["missing"].analysis.httpStatus, 0)
        self.assertIn("InvalidInput", rows["missing"].analysis.error)
        self.assertIsNotNone(rows["missing"].requestError)
        for name, page in (("first", 1), ("third", 3)):
            self.assertIsNone(rows[name].requestError)
            body = self.successful_body(rows[name].analysis)
            contents = body["result"]["contents"]
            pages = [p["pageNumber"] for c in contents for p in c["pages"]]
            self.assertEqual(pages, [page])
            markdown = "\n".join(content["markdown"] for content in contents)
            self.assertIn(f"CU-{page:03d}", markdown)

    def assert_shared_failure(self, stage):
        documents = self.documents(synthetic_pdf(1), "synthetic.pdf", "application/pdf")
        row = stage.transform(documents).collect()[0]
        self.assertIn("SharedConfigurationFailure", row.analysis.error)
        self.assertIsNone(row.analysis.operationLocation)

    def test_invalid_shared_key_returns_shared_failure(self):
        self.assert_shared_failure(self.analyzer(key="synapseml-invalid-key"))

    def test_missing_shared_analyzer_returns_shared_failure(self):
        missing = "synapseml-missing-" + uuid.uuid4().hex[:12]
        self.assert_shared_failure(
            self.analyzer().setApiVersion(GA_VERSION).setAnalyzerId(missing)
        )


if __name__ == "__main__":
    unittest.main()
