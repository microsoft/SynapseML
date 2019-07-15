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
class CleanMissingData(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Removes missing values from input dataset.

    The following modes are supported:

    - Mean   - replaces missings with the mean of fit column
    - Median - replaces missings with approximate median of fit column
    - Custom - replaces missings with custom value specified by user

    For mean and median modes, only numeric column types are supported,
    specifically:

    - int
    - long
    - float
    - double

    For custom mode, the types above are supported and additionally:

    - str
    - bool

    Args:

        cleaningMode (str): Cleaning mode (default: Mean)
        customValue (str): Custom value for replacement
        inputCols (list): The names of the input columns
        outputCols (list): The names of the output columns
    """

    @keyword_only
    def __init__(self, cleaningMode="Mean", customValue=None, inputCols=None, outputCols=None):
        super(CleanMissingData, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.CleanMissingData")
        self.cleaningMode = Param(self, "cleaningMode", "cleaningMode: Cleaning mode (default: Mean)")
        self._setDefault(cleaningMode="Mean")
        self.customValue = Param(self, "customValue", "customValue: Custom value for replacement")
        self.inputCols = Param(self, "inputCols", "inputCols: The names of the input columns")
        self.outputCols = Param(self, "outputCols", "outputCols: The names of the output columns")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cleaningMode="Mean", customValue=None, inputCols=None, outputCols=None):
        """
        Set the (keyword only) parameters

        Args:

            cleaningMode (str): Cleaning mode (default: Mean)
            customValue (str): Custom value for replacement
            inputCols (list): The names of the input columns
            outputCols (list): The names of the output columns
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCleaningMode(self, value):
        """

        Args:

            cleaningMode (str): Cleaning mode (default: Mean)

        """
        self._set(cleaningMode=value)
        return self


    def getCleaningMode(self):
        """

        Returns:

            str: Cleaning mode (default: Mean)
        """
        return self.getOrDefault(self.cleaningMode)


    def setCustomValue(self, value):
        """

        Args:

            customValue (str): Custom value for replacement

        """
        self._set(customValue=value)
        return self


    def getCustomValue(self):
        """

        Returns:

            str: Custom value for replacement
        """
        return self.getOrDefault(self.customValue)


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


    def setOutputCols(self, value):
        """

        Args:

            outputCols (list): The names of the output columns

        """
        self._set(outputCols=value)
        return self


    def getOutputCols(self):
        """

        Returns:

            list: The names of the output columns
        """
        return self.getOrDefault(self.outputCols)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.CleanMissingData"

    @staticmethod
    def _from_java(java_stage):
        module_name=CleanMissingData.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CleanMissingData"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return CleanMissingDataModel(java_model)


class CleanMissingDataModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`CleanMissingData`.

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
        return "com.microsoft.ml.spark.CleanMissingDataModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=CleanMissingDataModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CleanMissingDataModel"
        return from_java(java_stage, module_name)

