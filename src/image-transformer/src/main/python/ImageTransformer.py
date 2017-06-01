# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

import pyspark
from pyspark.ml.common import inherit_doc
from pyspark.sql.types import *
from pyspark.sql.types import Row, _create_row
import numpy as np
from mmlspark._ImageTransformer import _ImageTransformer

ImageFields = ["path", "height", "width", "type", "bytes"]

ImageSchema = StructType([
    StructField(ImageFields[0], StringType(),  True),
    StructField(ImageFields[1], IntegerType(), True),
    StructField(ImageFields[2], IntegerType(), True),
    StructField(ImageFields[3], IntegerType(), True),                 # OpenCV type: CV_8U in most cases
    StructField(ImageFields[4], BinaryType(), True) ])   # OpenCV bytes: row-wise BGR in most cases

def toNDArray(image):
    return np.asarray(image.bytes, dtype = np.uint8).reshape((image.height, image.width, 3))[:,:,(2,1,0)]

def toImage(array, path = "", ocvType = 16):
    length = np.prod(array.shape)

    data = bytearray(array.astype(dtype=np.int8)[:,:,(2,1,0)].reshape(length))
    height = array.shape[0]
    width = array.shape[1]
    # Creating new Row with _create_row(), because Row(name = value, ... ) orders fields by name,
    # which conflicts with expected ImageSchema order when the new DataFrame is created by UDF
    return  _create_row(ImageFields, [path, height, width, ocvType, data])

from pyspark.ml.common import inherit_doc
@inherit_doc
class ImageTransformer(_ImageTransformer):
    """
    Resizes the image to the given width and height
    :param int height: The height to resize to (>=0)
    :param int width: The width to resize to (>=0)
    """
    def resize(self, height, width):
        self._java_obj.resize(height, width)
        return self

    """
    Crops the image given the starting x,y coordinates
    and the width and height
    :param int x: The initial x coordinate (>=0)
    :param int y: The initial y coordinate (>=0)
    :param int height: The height to crop to (>=0)
    :param int width: The width to crop to (>=0)
    """
    def crop(self, x, y, height, width):
        self._java_obj.crop(x,y,height,width)
        return self

    """
    Formats the image to the given image format
    :param int format: The format to convert to, please see OpenCV cvtColor function documentation for all formats
    """
    def colorFormat(self, format):
        self._java_obj.colorFormat(format)
        return self

    """
    Blurs the image using a normalized box filter
    :param double height: The height of the box filter (>= 0)
    :param double width: The width of the box filter (>= 0)
    """
    def blur(self, height, width):
        self._java_obj.blur(height, width)
        return self

    """
    Thresholds the image, please see OpenCV threshold function documentation for more information
    :param double threshold: The threshold value
    :param double maxVal: The maximum value to use
    :param double thresholdType: The type of threshold, can be binary, binary_inv, trunc, zero, zero_inv
    """
    def threshold(self, threshold, maxVal, thresholdType):
        self._java_obj.threshold(threshold, maxVal, thresholdType)
        return self

    """
    Blurs the image by applying a gaussian kernel
    :param double appertureSize: The aperture size, which should be odd and positive
    :param double sigma: The standard deviation of the gaussian
    """
    def gaussianKernel(self, appertureSize, sigma):
        self._java_obj.gaussianKernel(appertureSize, sigma)
        return self
