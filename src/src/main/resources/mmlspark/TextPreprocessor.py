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
class TextPreprocessor(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        inputCol (str): The name of the input column
        map (dict): Map of substring match to replacement
        normFunc (str): Name of normalization function to apply
        outputCol (str): The name of the output column
    """

    @keyword_only
    def __init__(self, inputCol=None, map=None, normFunc=None, outputCol=None):
        super(TextPreprocessor, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TextPreprocessor")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.map = Param(self, "map", "map: Map of substring match to replacement")
        self.normFunc = Param(self, "normFunc", "normFunc: Name of normalization function to apply")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, map=None, normFunc=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            map (dict): Map of substring match to replacement
            normFunc (str): Name of normalization function to apply
            outputCol (str): The name of the output column
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column
        """
        return self.getOrDefault(self.inputCol)


    def setMap(self, value):
        """

        Args:

            map (dict): Map of substring match to replacement

        """
        self._set(map=value)
        return self


    def getMap(self):
        """

        Returns:

            dict: Map of substring match to replacement
        """
        return self.getOrDefault(self.map)


    def setNormFunc(self, value):
        """

        Args:

            normFunc (str): Name of normalization function to apply

        """
        self._set(normFunc=value)
        return self


    def getNormFunc(self):
        """

        Returns:

            str: Name of normalization function to apply
        """
        return self.getOrDefault(self.normFunc)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TextPreprocessor"

    @staticmethod
    def _from_java(java_stage):
        module_name=TextPreprocessor.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TextPreprocessor"
        return from_java(java_stage, module_name)
