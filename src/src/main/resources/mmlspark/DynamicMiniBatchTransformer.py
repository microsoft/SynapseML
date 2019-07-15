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
class DynamicMiniBatchTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        maxBatchSize (int): The max size of the buffer (default: 2147483647)
    """

    @keyword_only
    def __init__(self, maxBatchSize=2147483647):
        super(DynamicMiniBatchTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.DynamicMiniBatchTransformer")
        self.maxBatchSize = Param(self, "maxBatchSize", "maxBatchSize: The max size of the buffer (default: 2147483647)")
        self._setDefault(maxBatchSize=2147483647)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, maxBatchSize=2147483647):
        """
        Set the (keyword only) parameters

        Args:

            maxBatchSize (int): The max size of the buffer (default: 2147483647)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setMaxBatchSize(self, value):
        """

        Args:

            maxBatchSize (int): The max size of the buffer (default: 2147483647)

        """
        self._set(maxBatchSize=value)
        return self


    def getMaxBatchSize(self):
        """

        Returns:

            int: The max size of the buffer (default: 2147483647)
        """
        return self.getOrDefault(self.maxBatchSize)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.DynamicMiniBatchTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=DynamicMiniBatchTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".DynamicMiniBatchTransformer"
        return from_java(java_stage, module_name)
