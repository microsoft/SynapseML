import pyspark
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField
from mmlspark.opencv.ImageTransformer import ImageSchema


image_source = "org.apache.spark.ml.source.image.PatchedImageFileFormat"
image_sink = image_source
binary_source = "org.apache.spark.binary.BinaryFileFormat"
binary_sink = binary_source
serving_source = "org.apache.spark.sql.execution.streaming.HTTPSourceProvider"
serving_sink = "org.apache.spark.sql.execution.streaming.HTTPSinkProvider"
distributed_serving_source = "org.apache.spark.sql.execution.streaming.DistributedHTTPSourceProvider"
distributed_serving_sink = "org.apache.spark.sql.execution.streaming.DistributedHTTPSinkProvider"
continuous_serving_source = "org.apache.spark.sql.execution.streaming.continuous.HTTPSourceProviderV2"
continuous_serving_sink = "org.apache.spark.sql.execution.streaming.continuous.HTTPSinkProviderV2"

def _readServer(self):
    return self.format(serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'server', _readServer)

def _readDistServer(self):
    return self.format(distributed_serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'distributedServer', _readDistServer)

def _readContServer(self):
    return self.format(continuous_serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'continuousServer', _readContServer)

def _address(self, host, port, api):
    return self.option("host", host) \
        .option("port", port) \
        .option("path", api)

setattr(pyspark.sql.streaming.DataStreamReader, 'address', _address)

def _replyTo(self, name):
    return self.option("name", name)

setattr(pyspark.sql.streaming.DataStreamWriter, 'replyTo', _replyTo)

def _writeServer(self):
    return self.format(serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'server', _writeServer)

def _writeDistServer(self):
    return self.format(distributed_serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'distributedServer', _writeDistServer)

def _writeContServer(self):
    return self.format(continuous_serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'continuousServer', _writeContServer)

def _parseRequest(self, apiName, schema,
                  idCol="id", requestCol="request", parsingCheck = "none"):
    ctx = SparkContext.getOrCreate()
    jvm = ctx._jvm
    extended = jvm.com.microsoft.ml.spark.io.DataFrameExtensions(self._jdf)
    dt = jvm.org.apache.spark.sql.types.DataType
    jResult = extended.parseRequest(apiName, dt.fromJson(schema.json()), idCol, requestCol, parsingCheck)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jResult, sql_ctx)

setattr(pyspark.sql.DataFrame, 'parseRequest', _parseRequest)

def _makeReply(self, replyCol, name="reply"):
    ctx = SparkContext.getOrCreate()
    jvm = ctx._jvm
    extended = jvm.com.microsoft.ml.spark.io.DataFrameExtensions(self._jdf)
    jResult = extended.makeReply(replyCol, name)
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    return DataFrame(jResult, sql_ctx)

setattr(pyspark.sql.DataFrame, 'makeReply', _makeReply)

def _readImage(self):
    return self.format(image_source).schema(StructType([StructField("image",ImageSchema,True)]))

setattr(pyspark.sql.streaming.DataStreamReader, 'image', _readImage)
setattr(pyspark.sql.DataFrameReader, 'image', _readImage)

def _writeImage(self):
    return self.format(image_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'image', _writeImage)
setattr(pyspark.sql.DataFrameWriter, 'image', _writeImage)

def _readBinary(self):
    return self.format(binary_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'binary', _readBinary)
setattr(pyspark.sql.DataFrameReader, 'binary', _readBinary)

def _writeBinary(self):
    return self.format(binary_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'binary', _writeBinary)
setattr(pyspark.sql.DataFrameWriter, 'binary', _writeBinary)
