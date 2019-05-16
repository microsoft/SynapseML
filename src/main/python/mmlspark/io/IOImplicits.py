import pyspark
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField
from mmlspark.opencv.ImageTransformer import ImageSchema


image_source = "org.apache.spark.ml.source.image.PatchedImageFileFormat"
image_sink = image_source
binary_source = "org.apache.spark.binary.BinaryFileFormat"
binary_sink = binary_source

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
