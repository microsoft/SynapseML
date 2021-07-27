import pyspark
from pyspark import SparkContext
from pyspark.sql.column import Column

def _http_schema():
    return SparkContext._active_spark_context._jvm.com.microsoft.ml.spark.io.http.HTTPSchema

def string_to_response(c):
    return Column(_http_schema().string_to_response(c._jc))

def request_to_string(c):
    return Column(_http_schema().request_to_string(c._jc))
