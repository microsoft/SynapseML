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
class FastVectorAssembler(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    A fast vector assembler.  The columns given must be ordered such that
    categorical columns come first.  Otherwise, Spark learners will give
    categorical attributes to the wrong index.  The assembler does not keep
    spurious numeric data which can significantly slow down computations
    when there are millions of columns.

    To use this ``FastVectorAssemble`` you must import the
    ``org.apache.spark.ml.feature`` package.

    Args:

        inputCols (list): input column names
        outputCol (str): output column name (default: [self.uid]__output)
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(FastVectorAssembler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.FastVectorAssembler")
        self.inputCols = Param(self, "inputCols", "inputCols: input column names")
        self.outputCol = Param(self, "outputCol", "outputCol: output column name (default: [self.uid]__output)")
        self._setDefault(outputCol=self.uid + "__output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCols (list): input column names
            outputCol (str): output column name (default: [self.uid]__output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value):
        """

        Args:

            inputCols (list): input column names

        """
        self._set(inputCols=value)
        return self


    def getInputCols(self):
        """

        Returns:

            list: input column names
        """
        return self.getOrDefault(self.inputCols)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): output column name (default: [self.uid]__output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: output column name (default: [self.uid]__output)
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "org.apache.spark.ml.feature.FastVectorAssembler"

    @staticmethod
    def _from_java(java_stage):
        module_name=FastVectorAssembler.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".FastVectorAssembler"
        return from_java(java_stage, module_name)
