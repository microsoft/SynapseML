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
from mmlspark.TypeConversionUtils import generateTypeConverter, complexTypeConverter

@inherit_doc
class CustomOutputParser(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        inputCol (str): The name of the input column
        outputCol (str): The name of the output column
        udfPython (object): User Defined Python Function to be applied to the DF input col
        udfScala (object): User Defined Function to be applied to the DF input col
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, udfPython=None, udfScala=None):
        super(CustomOutputParser, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.CustomOutputParser")
        self._cache = {}
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.udfPython = Param(self, "udfPython", "udfPython: User Defined Python Function to be applied to the DF input col")
        self.udfScala = Param(self, "udfScala", "udfScala: User Defined Function to be applied to the DF input col", generateTypeConverter("udfScala", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, udfPython=None, udfScala=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            outputCol (str): The name of the output column
            udfPython (object): User Defined Python Function to be applied to the DF input col
            udfScala (object): User Defined Function to be applied to the DF input col
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


    def setUdfPython(self, value):
        """

        Args:

            udfPython (object): User Defined Python Function to be applied to the DF input col

        """
        self._set(udfPython=value)
        return self


    def getUdfPython(self):
        """

        Returns:

            object: User Defined Python Function to be applied to the DF input col
        """
        return self.getOrDefault(self.udfPython)


    def setUdfScala(self, value):
        """

        Args:

            udfScala (object): User Defined Function to be applied to the DF input col

        """
        self._set(udfScala=value)
        return self


    def getUdfScala(self):
        """

        Returns:

            object: User Defined Function to be applied to the DF input col
        """
        return self._cache.get("udfScala", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.CustomOutputParser"

    @staticmethod
    def _from_java(java_stage):
        module_name=CustomOutputParser.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CustomOutputParser"
        return from_java(java_stage, module_name)
