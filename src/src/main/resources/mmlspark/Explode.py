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
class Explode(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        inputCol (str): The name of the input column
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(Explode, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Explode")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            outputCol (str): The name of the output column (default: [self.uid]_output)
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
        return "com.microsoft.ml.spark.Explode"

    @staticmethod
    def _from_java(java_stage):
        module_name=Explode.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Explode"
        return from_java(java_stage, module_name)
