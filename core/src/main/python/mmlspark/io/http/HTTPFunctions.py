from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, udf
import json


def requests_to_spark(p):
    return {
        "requestLine": {
            "method": p.method,
            "uri": p.url},
        "headers": [{"name": name, "value": value} for name, value in p.headers.items() if name != "Content-Length"],
        "entity": None if p.body is None else (
            {"content": p.body,
             "isChunked": False,
             "isRepeatable": True,
             "isStreaming": False}
        )
    }


# SparkContext._active_spark_context._jvm.com.microsoft.ml.spark.io.http.HTTPRequestData.schema().json()
# TODO figure out why we cannot just grab from SparkContext on databricks
HTTPRequestDataType = StructType().fromJson(json.loads(
    '{"type":"struct","fields":[{"name":"requestLine","type":{"type":"struct","fields":[{"name":"method",'
    '"type":"string","nullable":true,"metadata":{}},{"name":"uri","type":"string","nullable":true,"metadata":{}},'
    '{"name":"protocolVersion","type":{"type":"struct","fields":[{"name":"protocol","type":"string",'
    '"nullable":true,"metadata":{}},{"name":"major","type":"integer","nullable":false,"metadata":{}},'
    '{"name":"minor","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]},'
    '"nullable":true,"metadata":{}},{"name":"headers","type":{"type":"array","elementType":{"type":"struct",'
    '"fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"value","type":"string",'
    '"nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"entity",'
    '"type":{"type":"struct","fields":[{"name":"content","type":"binary","nullable":true,"metadata":{}},'
    '{"name":"contentEncoding","type":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,'
    '"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},'
    '{"name":"contentLength","type":"long","nullable":true,"metadata":{}},{"name":"contentType",'
    '"type":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},'
    '{"name":"value","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},'
    '{"name":"isChunked","type":"boolean","nullable":false,"metadata":{}},'
    '{"name":"isRepeatable","type":"boolean","nullable":false,"metadata":{}},'
    '{"name":"isStreaming","type":"boolean","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]}'
))


def http_udf(func):
    def composition(*args):
        return requests_to_spark(func(*args).prepare())
    return udf(composition, HTTPRequestDataType)
