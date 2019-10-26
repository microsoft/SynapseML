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


HTTPRequestDataType = StructType().fromJson(json.loads(
    SparkContext._active_spark_context._jvm.com.microsoft.ml.spark.io.http.HTTPRequestData.schema().json()
))


def http_udf(func):
    def composition(*args):
        return requests_to_spark(func(*args).prepare())
    return udf(composition, HTTPRequestDataType)
