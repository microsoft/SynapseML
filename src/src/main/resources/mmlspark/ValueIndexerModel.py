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
class ValueIndexerModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Model produced by ``ValueIndexer``.

    Args:

        dataType (str): The datatype of the levels as a Json string (default: string)
        inputCol (str): The name of the input column (default: input)
        levels (object): Levels in categorical array
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, dataType="string", inputCol="input", levels=None, outputCol=None):
        super(ValueIndexerModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ValueIndexerModel")
        self.dataType = Param(self, "dataType", "dataType: The datatype of the levels as a Json string (default: string)")
        self._setDefault(dataType="string")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column (default: input)")
        self._setDefault(inputCol="input")
        self.levels = Param(self, "levels", "levels: Levels in categorical array")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, dataType="string", inputCol="input", levels=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            dataType (str): The datatype of the levels as a Json string (default: string)
            inputCol (str): The name of the input column (default: input)
            levels (object): Levels in categorical array
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDataType(self, value):
        """

        Args:

            dataType (str): The datatype of the levels as a Json string (default: string)

        """
        self._set(dataType=value)
        return self


    def getDataType(self):
        """

        Returns:

            str: The datatype of the levels as a Json string (default: string)
        """
        return self.getOrDefault(self.dataType)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column (default: input)

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column (default: input)
        """
        return self.getOrDefault(self.inputCol)


    def setLevels(self, value):
        """

        Args:

            levels (object): Levels in categorical array

        """
        self._set(levels=value)
        return self


    def getLevels(self):
        """

        Returns:

            object: Levels in categorical array
        """
        return self.getOrDefault(self.levels)


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
        return "com.microsoft.ml.spark.ValueIndexerModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=ValueIndexerModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ValueIndexerModel"
        return from_java(java_stage, module_name)
