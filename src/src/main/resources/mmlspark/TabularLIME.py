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
class TabularLIME(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        inputCol (str): The name of the input column
        model (object): Model to try to locally approximate
        nSamples (int): The number of samples to generate (default: 1000)
        outputCol (str): The name of the output column
        predictionCol (str): prediction column name (default: prediction)
        regularization (double): regularization param for the lasso (default: 0.0)
        samplingFraction (double): The fraction of superpixels to keep on (default: 0.3)
    """

    @keyword_only
    def __init__(self, inputCol=None, model=None, nSamples=1000, outputCol=None, predictionCol="prediction", regularization=0.0, samplingFraction=0.3):
        super(TabularLIME, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TabularLIME")
        self._cache = {}
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.model = Param(self, "model", "model: Model to try to locally approximate", generateTypeConverter("model", self._cache, complexTypeConverter))
        self.nSamples = Param(self, "nSamples", "nSamples: The number of samples to generate (default: 1000)")
        self._setDefault(nSamples=1000)
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.predictionCol = Param(self, "predictionCol", "predictionCol: prediction column name (default: prediction)")
        self._setDefault(predictionCol="prediction")
        self.regularization = Param(self, "regularization", "regularization: regularization param for the lasso (default: 0.0)")
        self._setDefault(regularization=0.0)
        self.samplingFraction = Param(self, "samplingFraction", "samplingFraction: The fraction of superpixels to keep on (default: 0.3)")
        self._setDefault(samplingFraction=0.3)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, model=None, nSamples=1000, outputCol=None, predictionCol="prediction", regularization=0.0, samplingFraction=0.3):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            model (object): Model to try to locally approximate
            nSamples (int): The number of samples to generate (default: 1000)
            outputCol (str): The name of the output column
            predictionCol (str): prediction column name (default: prediction)
            regularization (double): regularization param for the lasso (default: 0.0)
            samplingFraction (double): The fraction of superpixels to keep on (default: 0.3)
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


    def setModel(self, value):
        """

        Args:

            model (object): Model to try to locally approximate

        """
        self._set(model=value)
        return self


    def getModel(self):
        """

        Returns:

            object: Model to try to locally approximate
        """
        return self._cache.get("model", None)


    def setNSamples(self, value):
        """

        Args:

            nSamples (int): The number of samples to generate (default: 1000)

        """
        self._set(nSamples=value)
        return self


    def getNSamples(self):
        """

        Returns:

            int: The number of samples to generate (default: 1000)
        """
        return self.getOrDefault(self.nSamples)


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


    def setPredictionCol(self, value):
        """

        Args:

            predictionCol (str): prediction column name (default: prediction)

        """
        self._set(predictionCol=value)
        return self


    def getPredictionCol(self):
        """

        Returns:

            str: prediction column name (default: prediction)
        """
        return self.getOrDefault(self.predictionCol)


    def setRegularization(self, value):
        """

        Args:

            regularization (double): regularization param for the lasso (default: 0.0)

        """
        self._set(regularization=value)
        return self


    def getRegularization(self):
        """

        Returns:

            double: regularization param for the lasso (default: 0.0)
        """
        return self.getOrDefault(self.regularization)


    def setSamplingFraction(self, value):
        """

        Args:

            samplingFraction (double): The fraction of superpixels to keep on (default: 0.3)

        """
        self._set(samplingFraction=value)
        return self


    def getSamplingFraction(self):
        """

        Returns:

            double: The fraction of superpixels to keep on (default: 0.3)
        """
        return self.getOrDefault(self.samplingFraction)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TabularLIME"

    @staticmethod
    def _from_java(java_stage):
        module_name=TabularLIME.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TabularLIME"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return TabularLIMEModel(java_model)


class TabularLIMEModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`TabularLIME`.

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
        return "com.microsoft.ml.spark.TabularLIMEModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=TabularLIMEModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TabularLIMEModel"
        return from_java(java_stage, module_name)

