# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class FixedMiniBatchTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        batchSize (int): The max size of the buffer
        buffered (bool): Whether or not to buffer batches in memory (default: false)
        maxBufferSize (int): The max size of the buffer (default: 2147483647)
    """

    @keyword_only
    def __init__(self, batchSize=None, buffered=False, maxBufferSize=2147483647):
        super(FixedMiniBatchTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.FixedMiniBatchTransformer")
        self.batchSize = Param(self, "batchSize", "batchSize: The max size of the buffer")
        self.buffered = Param(self, "buffered", "buffered: Whether or not to buffer batches in memory (default: false)")
        self._setDefault(buffered=False)
        self.maxBufferSize = Param(self, "maxBufferSize", "maxBufferSize: The max size of the buffer (default: 2147483647)")
        self._setDefault(maxBufferSize=2147483647)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, batchSize=None, buffered=False, maxBufferSize=2147483647):
        """
        Set the (keyword only) parameters

        Args:

            batchSize (int): The max size of the buffer
            buffered (bool): Whether or not to buffer batches in memory (default: false)
            maxBufferSize (int): The max size of the buffer (default: 2147483647)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBatchSize(self, value):
        """

        Args:

            batchSize (int): The max size of the buffer

        """
        self._set(batchSize=value)
        return self


    def getBatchSize(self):
        """

        Returns:

            int: The max size of the buffer
        """
        return self.getOrDefault(self.batchSize)


    def setBuffered(self, value):
        """

        Args:

            buffered (bool): Whether or not to buffer batches in memory (default: false)

        """
        self._set(buffered=value)
        return self


    def getBuffered(self):
        """

        Returns:

            bool: Whether or not to buffer batches in memory (default: false)
        """
        return self.getOrDefault(self.buffered)


    def setMaxBufferSize(self, value):
        """

        Args:

            maxBufferSize (int): The max size of the buffer (default: 2147483647)

        """
        self._set(maxBufferSize=value)
        return self


    def getMaxBufferSize(self):
        """

        Returns:

            int: The max size of the buffer (default: 2147483647)
        """
        return self.getOrDefault(self.maxBufferSize)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.FixedMiniBatchTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=FixedMiniBatchTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".FixedMiniBatchTransformer"
        return from_java(java_stage, module_name)
