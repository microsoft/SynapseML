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
class MultiColumnAdapter(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Takes a unary pipeline stage and a list of input/output column pairs and
    applies the pipeline stage to each column after a fit

    Args:

        baseStage (object): base pipeline stage to apply to every column
        inputCols (list): list of column names encoded as a string
        outputCols (list): list of column names encoded as a string
    """

    @keyword_only
    def __init__(self, baseStage=None, inputCols=None, outputCols=None):
        super(MultiColumnAdapter, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.MultiColumnAdapter")
        self._cache = {}
        self.baseStage = Param(self, "baseStage", "baseStage: base pipeline stage to apply to every column", generateTypeConverter("baseStage", self._cache, complexTypeConverter))
        self.inputCols = Param(self, "inputCols", "inputCols: list of column names encoded as a string")
        self.outputCols = Param(self, "outputCols", "outputCols: list of column names encoded as a string")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, baseStage=None, inputCols=None, outputCols=None):
        """
        Set the (keyword only) parameters

        Args:

            baseStage (object): base pipeline stage to apply to every column
            inputCols (list): list of column names encoded as a string
            outputCols (list): list of column names encoded as a string
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBaseStage(self, value):
        """

        Args:

            baseStage (object): base pipeline stage to apply to every column

        """
        self._set(baseStage=value)
        return self


    def getBaseStage(self):
        """

        Returns:

            object: base pipeline stage to apply to every column
        """
        return self._cache.get("baseStage", None)


    def setInputCols(self, value):
        """

        Args:

            inputCols (list): list of column names encoded as a string

        """
        self._set(inputCols=value)
        return self


    def getInputCols(self):
        """

        Returns:

            list: list of column names encoded as a string
        """
        return self.getOrDefault(self.inputCols)


    def setOutputCols(self, value):
        """

        Args:

            outputCols (list): list of column names encoded as a string

        """
        self._set(outputCols=value)
        return self


    def getOutputCols(self):
        """

        Returns:

            list: list of column names encoded as a string
        """
        return self.getOrDefault(self.outputCols)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.MultiColumnAdapter"

    @staticmethod
    def _from_java(java_stage):
        module_name=MultiColumnAdapter.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".MultiColumnAdapter"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return PipelineModel(java_model)


class PipelineModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`MultiColumnAdapter`.

    This class is left empty on purpose.
    All necessary methods are exposed through inheritance.
    """

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "org.apache.spark.ml.PipelineModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=PipelineModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".PipelineModel"
        return from_java(java_stage, module_name)

