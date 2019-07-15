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
class _JSONOutputParser(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        dataType (object): format to parse the column to
        inputCol (str): The name of the input column
        outputCol (str): The name of the output column
        postProcessor (object): optional transformation to postprocess json output
    """

    @keyword_only
    def __init__(self, dataType=None, inputCol=None, outputCol=None, postProcessor=None):
        super(_JSONOutputParser, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.JSONOutputParser")
        self._cache = {}
        self.dataType = Param(self, "dataType", "dataType: format to parse the column to")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.postProcessor = Param(self, "postProcessor", "postProcessor: optional transformation to postprocess json output", generateTypeConverter("postProcessor", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, dataType=None, inputCol=None, outputCol=None, postProcessor=None):
        """
        Set the (keyword only) parameters

        Args:

            dataType (object): format to parse the column to
            inputCol (str): The name of the input column
            outputCol (str): The name of the output column
            postProcessor (object): optional transformation to postprocess json output
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDataType(self, value):
        """

        Args:

            dataType (object): format to parse the column to

        """
        self._set(dataType=value)
        return self


    def getDataType(self):
        """

        Returns:

            object: format to parse the column to
        """
        return self.getOrDefault(self.dataType)


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


    def setPostProcessor(self, value):
        """

        Args:

            postProcessor (object): optional transformation to postprocess json output

        """
        self._set(postProcessor=value)
        return self


    def getPostProcessor(self):
        """

        Returns:

            object: optional transformation to postprocess json output
        """
        return self._cache.get("postProcessor", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.JSONOutputParser"

    @staticmethod
    def _from_java(java_stage):
        module_name=_JSONOutputParser.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".JSONOutputParser"
        return from_java(java_stage, module_name)
