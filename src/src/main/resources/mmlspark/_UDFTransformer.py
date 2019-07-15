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
class _UDFTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        inputCol (str): The name of the input column
        inputCols (list): The names of the input columns
        outputCol (str): The name of the output column (default: [self.uid]_output)
        udf (object): User Defined Python Function to be applied to the DF input col
        udfScala (object): User Defined Function to be applied to the DF input col
    """

    @keyword_only
    def __init__(self, inputCol=None, inputCols=None, outputCol=None, udf=None, udfScala=None):
        super(_UDFTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.UDFTransformer")
        self._cache = {}
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.inputCols = Param(self, "inputCols", "inputCols: The names of the input columns")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.udf = Param(self, "udf", "udf: User Defined Python Function to be applied to the DF input col")
        self.udfScala = Param(self, "udfScala", "udfScala: User Defined Function to be applied to the DF input col", generateTypeConverter("udfScala", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, inputCols=None, outputCol=None, udf=None, udfScala=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            inputCols (list): The names of the input columns
            outputCol (str): The name of the output column (default: [self.uid]_output)
            udf (object): User Defined Python Function to be applied to the DF input col
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


    def setInputCols(self, value):
        """

        Args:

            inputCols (list): The names of the input columns

        """
        self._set(inputCols=value)
        return self


    def getInputCols(self):
        """

        Returns:

            list: The names of the input columns
        """
        return self.getOrDefault(self.inputCols)


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


    def setUdf(self, value):
        """

        Args:

            udf (object): User Defined Python Function to be applied to the DF input col

        """
        self._set(udf=value)
        return self


    def getUdf(self):
        """

        Returns:

            object: User Defined Python Function to be applied to the DF input col
        """
        return self.getOrDefault(self.udf)


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
        return "com.microsoft.ml.spark.UDFTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=_UDFTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".UDFTransformer"
        return from_java(java_stage, module_name)
