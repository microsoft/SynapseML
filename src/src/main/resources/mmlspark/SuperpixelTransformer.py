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
class SuperpixelTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        cellSize (double): Number that controls the size of the superpixels (default: 16.0)
        inputCol (str): The name of the input column
        modifier (double): Controls the trade-off spatial and color distance (default: 130.0)
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, cellSize=16.0, inputCol=None, modifier=130.0, outputCol=None):
        super(SuperpixelTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SuperpixelTransformer")
        self.cellSize = Param(self, "cellSize", "cellSize: Number that controls the size of the superpixels (default: 16.0)")
        self._setDefault(cellSize=16.0)
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.modifier = Param(self, "modifier", "modifier: Controls the trade-off spatial and color distance (default: 130.0)")
        self._setDefault(modifier=130.0)
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cellSize=16.0, inputCol=None, modifier=130.0, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            cellSize (double): Number that controls the size of the superpixels (default: 16.0)
            inputCol (str): The name of the input column
            modifier (double): Controls the trade-off spatial and color distance (default: 130.0)
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCellSize(self, value):
        """

        Args:

            cellSize (double): Number that controls the size of the superpixels (default: 16.0)

        """
        self._set(cellSize=value)
        return self


    def getCellSize(self):
        """

        Returns:

            double: Number that controls the size of the superpixels (default: 16.0)
        """
        return self.getOrDefault(self.cellSize)


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


    def setModifier(self, value):
        """

        Args:

            modifier (double): Controls the trade-off spatial and color distance (default: 130.0)

        """
        self._set(modifier=value)
        return self


    def getModifier(self):
        """

        Returns:

            double: Controls the trade-off spatial and color distance (default: 130.0)
        """
        return self.getOrDefault(self.modifier)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.SuperpixelTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=SuperpixelTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SuperpixelTransformer"
        return from_java(java_stage, module_name)
