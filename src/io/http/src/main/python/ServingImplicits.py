import pyspark
from pyspark import SparkContext
from pyspark.sql import DataFrame

serving_source = "org.apache.spark.sql.execution.streaming.HTTPSourceProvider"
serving_sink = "org.apache.spark.sql.execution.streaming.HTTPSinkProvider"
distributed_serving_source = "org.apache.spark.sql.execution.streaming.DistributedHTTPSourceProvider"
distributed_serving_sink = "org.apache.spark.sql.execution.streaming.DistributedHTTPSinkProvider"

def _readServer(self):
    return self.format(serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'server', _readServer)

def _readDistServer(self):
    return self.format(distributed_serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'distributedServer', _readDistServer)

def _address(self, host, port, api):
    return self.option("host", host) \
        .option("port", port) \
        .option("name", api)

setattr(pyspark.sql.streaming.DataStreamReader, 'address', _address)

def _writeServer(self):
    return self.format(serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'server', _writeServer)


def _replyTo(self, name):
    return self.option("name", name)

setattr(pyspark.sql.streaming.DataStreamWriter, 'replyTo', _replyTo)


def _writeDistServer(self):
    return self.format(distributed_serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'distributedServer', _writeDistServer)

def _parseRequest(self,schema,
                 idCol="id",requestCol="request"):
    ctx = SparkContext.getOrCreate()
    jvm = ctx._jvm
    extended = jvm.com.microsoft.ml.spark.DataFrameServingExtensions(self._jdf)
    dt = jvm.org.apache.spark.sql.types.DataType
    jResult = extended.parseRequest(dt.fromJson(schema.json()), idCol, requestCol)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jResult, sql_ctx)

setattr(pyspark.sql.DataFrame, 'parseRequest', _parseRequest)

def _makeReply(self, replyCol, name="reply"):
    ctx = SparkContext.getOrCreate()
    jvm = ctx._jvm
    extended = jvm.com.microsoft.ml.spark.DataFrameServingExtensions(self._jdf)
    jResult = extended.makeReply(replyCol, name)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jResult, sql_ctx)

setattr(pyspark.sql.DataFrame, 'makeReply', _makeReply)
