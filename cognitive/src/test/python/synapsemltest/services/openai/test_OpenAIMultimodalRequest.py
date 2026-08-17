# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from synapse.ml.core.init_spark import init_spark
from synapse.ml.services.openai.OpenAIChatCompletion import OpenAIChatCompletion
from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt
from synapse.ml.services.openai.OpenAIResponses import OpenAIResponses

spark = init_spark()


class EchoOpenAIHandler(BaseHTTPRequestHandler):
    request_bodies = []

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        request_body = self.rfile.read(content_length).decode("utf-8")
        type(self).request_bodies.append(request_body)
        if self.path.endswith("/responses"):
            response = {
                "id": "resp_test",
                "object": "response",
                "created_at": "1",
                "model": "gpt-5.1",
                "output": [
                    {
                        "content": [{"type": "output_text", "text": request_body}],
                        "status": "completed",
                    }
                ],
                "system_fingerprint": None,
                "usage": None,
            }
        else:
            response = {
                "id": "chatcmpl_test",
                "object": "chat.completion",
                "created": "1",
                "model": "gpt-5.1",
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": request_body,
                            "name": None,
                        },
                        "index": 0,
                        "finish_reason": "stop",
                    }
                ],
                "system_fingerprint": None,
                "usage": None,
            }
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *_args):
        return


class TestOpenAIMultimodalRequest(unittest.TestCase):
    data_image = (
        "data:image/png;base64,"
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/"
        "x8AAusB9Y9Zl1sAAAAASUVORK5CYII="
    )

    @classmethod
    def setUpClass(cls):
        EchoOpenAIHandler.request_bodies = []
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), EchoOpenAIHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}/openai/v1"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join()

    def test_generated_direct_wrappers_preserve_api_specific_image_shapes(self):
        chat_image_schema = StructType(
            [
                StructField("url", StringType(), True),
                StructField("detail", StringType(), True),
            ]
        )
        chat_part_schema = StructType(
            [
                StructField("type", StringType(), False),
                StructField("text", StringType(), True),
                StructField("image_url", chat_image_schema, True),
            ]
        )
        responses_part_schema = StructType(
            [
                StructField("type", StringType(), False),
                StructField("text", StringType(), True),
                StructField("image_url", StringType(), True),
                StructField("detail", StringType(), True),
            ]
        )

        def message_schema(part_schema):
            return StructType(
                [
                    StructField("role", StringType(), False),
                    StructField("content", ArrayType(part_schema, True), True),
                    StructField("name", StringType(), True),
                ]
            )

        chat_messages = [
            Row(
                role="user",
                content=[
                    Row(type="text", text="Describe.", image_url=None),
                    Row(
                        type="image_url",
                        text=None,
                        image_url=Row(url=self.data_image, detail="low"),
                    ),
                ],
                name=None,
            )
        ]
        responses_messages = [
            Row(
                role="user",
                content=[
                    Row(
                        type="input_text",
                        text="Describe.",
                        image_url=None,
                        detail=None,
                    ),
                    Row(
                        type="input_image",
                        text=None,
                        image_url=self.data_image,
                        detail="low",
                    ),
                ],
                name=None,
            )
        ]

        cases = [
            (
                OpenAIChatCompletion(),
                chat_messages,
                message_schema(chat_part_schema),
                "messages",
            ),
            (
                OpenAIResponses(),
                responses_messages,
                message_schema(responses_part_schema),
                "input",
            ),
        ]
        for transformer, messages, nested_schema, payload_field in cases:
            with self.subTest(payload_field=payload_field):
                input_df = spark.createDataFrame(
                    [Row(messages=messages)],
                    StructType(
                        [
                            StructField(
                                "messages",
                                ArrayType(nested_schema, True),
                                True,
                            )
                        ]
                    ),
                )
                result = (
                    transformer.setUrl(self.base_url)
                    .setDeploymentName("gpt-5.1")
                    .setMessagesCol("messages")
                    .setSubscriptionKey("unused")
                    .setOutputCol("output")
                    .setErrorCol("error")
                    .setConcurrency(1)
                    .transform(input_df)
                    .head()
                )
                self.assertIsNone(result.error)
                response_text = (
                    result.output.choices[0].message.content
                    if payload_field == "messages"
                    else result.output.output[-1].content[0].text
                )
                payload = json.loads(response_text)
                parts = payload[payload_field][0]["content"]
                self.assertEqual(
                    [part["type"] for part in parts],
                    ["text", "image_url"]
                    if payload_field == "messages"
                    else ["input_text", "input_image"],
                )

    def test_generated_openai_prompt_wrapper_sends_data_images(self):
        cases = [
            ("responses", "input", ["input_text", "input_image"]),
            ("chat_completions", "messages", ["text", "image_url"]),
        ]
        for api_type, payload_field, expected_types in cases:
            with self.subTest(api_type=api_type):
                input_df = spark.createDataFrame(
                    [("Describe.", self.data_image)],
                    ["prompt", "image"],
                )
                result = (
                    OpenAIPrompt()
                    .setUrl(self.base_url)
                    .setApiType(api_type)
                    .setDeploymentName("gpt-5.1")
                    .setSubscriptionKey("unused")
                    .setPromptTemplate("{prompt}")
                    .setColumnTypes({"image": "path"})
                    .setOutputCol("output")
                    .setErrorCol("error")
                    .setConcurrency(1)
                    .transform(input_df)
                    .head()
                )

                self.assertIsNone(result.error)
                self.assertEqual(result.output, EchoOpenAIHandler.request_bodies[-1])
                payload = json.loads(result.output)
                parts = payload[payload_field][1]["content"]
                self.assertEqual([part["type"] for part in parts], expected_types)
                if api_type == "responses":
                    self.assertEqual(parts[1]["image_url"], self.data_image)
                else:
                    self.assertEqual(
                        parts[1]["image_url"]["url"],
                        self.data_image,
                    )

        self.assertEqual(
            OpenAIPrompt().setFileSizeLimitMB(1.5).getFileSizeLimitMB(),
            1.5,
        )


if __name__ == "__main__":
    unittest.main()
