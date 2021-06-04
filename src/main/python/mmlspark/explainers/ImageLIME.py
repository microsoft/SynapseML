# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from mmlspark.core.serialize.java_params_patch import *
from pyspark.ml.common import inherit_doc
from mmlspark.core.schema.Utils import *
from pyspark.ml.param import TypeConverters
from mmlspark.explainers._LocalExplainer import _LocalExplainer


@inherit_doc
class ImageLIME(_LocalExplainer):
    """
    Args:
        cellSize (float): Number that controls the size of the superpixels
        inputCol (object): input column name
        kernelWidth (float): Kernel width. Default value: sqrt (number of features) * 0.75
        metricsCol (object): Column name for fitting metrics
        model (object): The model to be interpreted.
        modifier (float): Controls the trade-off spatial and color distance
        numSamples (int): Number of samples to generate.
        outputCol (object): output column name
        regularization (float): Regularization param for the lasso. Default value: 0.
        samplingFraction (float): The fraction of superpixels (for image) or tokens (for text) to keep on
        superpixelCol (object): The column holding the superpixel decompositions
        targetClass (int): The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        targetClassCol (object): The name of the column that specifies the index of the class for multinomial classification models.
        targetCol (object): The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
    """

    cellSize = Param(
        Params._dummy(),
        "cellSize",
        "Number that controls the size of the superpixels",
        typeConverter=TypeConverters.toFloat,
    )

    inputCol = Param(Params._dummy(), "inputCol", "input column name")

    kernelWidth = Param(
        Params._dummy(),
        "kernelWidth",
        "Kernel width. Default value: sqrt (number of features) * 0.75",
        typeConverter=TypeConverters.toFloat,
    )

    metricsCol = Param(Params._dummy(), "metricsCol", "Column name for fitting metrics")

    model = Param(Params._dummy(), "model", "The model to be interpreted.")

    modifier = Param(
        Params._dummy(),
        "modifier",
        "Controls the trade-off spatial and color distance",
        typeConverter=TypeConverters.toFloat,
    )

    numSamples = Param(
        Params._dummy(), "numSamples", "Number of samples to generate.", typeConverter=TypeConverters.toInt
    )

    outputCol = Param(Params._dummy(), "outputCol", "output column name")

    regularization = Param(
        Params._dummy(),
        "regularization",
        "Regularization param for the lasso. Default value: 0.",
        typeConverter=TypeConverters.toFloat,
    )

    samplingFraction = Param(
        Params._dummy(),
        "samplingFraction",
        "The fraction of superpixels (for image) or tokens (for text) to keep on",
        typeConverter=TypeConverters.toFloat,
    )

    superpixelCol = Param(Params._dummy(), "superpixelCol", "The column holding the superpixel decompositions")

    targetClass = Param(
        Params._dummy(),
        "targetClass",
        "The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.",
        typeConverter=TypeConverters.toInt,
    )

    targetClassCol = Param(
        Params._dummy(),
        "targetClassCol",
        "The name of the column that specifies the index of the class for multinomial classification models.",
    )

    targetCol = Param(
        Params._dummy(),
        "targetCol",
        'The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability',
    )

    @keyword_only
    def __init__(
        self,
        java_obj=None,
        cellSize=16.0,
        inputCol=None,
        kernelWidth=0.75,
        metricsCol="r2",
        model=None,
        modifier=130.0,
        numSamples=900,
        outputCol="ImageLIME_81c562015a7b__output",
        regularization=0.0,
        samplingFraction=0.7,
        superpixelCol="superpixels",
        targetClass=0,
        targetClassCol=None,
        targetCol="probability",
    ):
        super(ImageLIME, self).__init__()
        if java_obj is None:
            self._java_obj = self._new_java_obj("com.microsoft.ml.spark.explainers.ImageLIME", self.uid)
        else:
            self._java_obj = java_obj
        self._setDefault(cellSize=16.0)
        self._setDefault(kernelWidth=0.75)
        self._setDefault(metricsCol="r2")
        self._setDefault(modifier=130.0)
        self._setDefault(numSamples=900)
        self._setDefault(outputCol="ImageLIME_81c562015a7b__output")
        self._setDefault(regularization=0.0)
        self._setDefault(samplingFraction=0.7)
        self._setDefault(superpixelCol="superpixels")
        self._setDefault(targetClass=0)
        self._setDefault(targetCol="probability")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs

        if java_obj is None:
            for k, v in kwargs.items():
                if v is not None:
                    getattr(self, "set" + k[0].upper() + k[1:])(v)

    @keyword_only
    def setParams(
        self,
        cellSize=16.0,
        inputCol=None,
        kernelWidth=0.75,
        metricsCol="r2",
        model=None,
        modifier=130.0,
        numSamples=900,
        outputCol="ImageLIME_81c562015a7b__output",
        regularization=0.0,
        samplingFraction=0.7,
        superpixelCol="superpixels",
        targetClass=0,
        targetClassCol=None,
        targetCol="probability",
    ):
        """
        Set the (keyword only) parameters
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.explainers.ImageLIME"

    @staticmethod
    def _from_java(java_stage):
        module_name = ImageLIME.__module__
        module_name = module_name.rsplit(".", 1)[0] + ".ImageLIME"
        return from_java(java_stage, module_name)

    def setCellSize(self, value):
        """
        Args:
            cellSize: Number that controls the size of the superpixels
        """

        self._set(cellSize=value)
        return self

    def setInputCol(self, value):
        """
        Args:
            inputCol: input column name
        """

        self._set(inputCol=value)
        return self

    def setKernelWidth(self, value):
        """
        Args:
            kernelWidth: Kernel width. Default value: sqrt (number of features) * 0.75
        """

        self._set(kernelWidth=value)
        return self

    def setMetricsCol(self, value):
        """
        Args:
            metricsCol: Column name for fitting metrics
        """

        self._set(metricsCol=value)
        return self

    def setModel(self, value):
        """
        Args:
            model: The model to be interpreted.
        """

        self._set(model=value)
        return self

    def setModifier(self, value):
        """
        Args:
            modifier: Controls the trade-off spatial and color distance
        """

        self._set(modifier=value)
        return self

    def setNumSamples(self, value):
        """
        Args:
            numSamples: Number of samples to generate.
        """

        self._set(numSamples=value)
        return self

    def setOutputCol(self, value):
        """
        Args:
            outputCol: output column name
        """

        self._set(outputCol=value)
        return self

    def setRegularization(self, value):
        """
        Args:
            regularization: Regularization param for the lasso. Default value: 0.
        """

        self._set(regularization=value)
        return self

    def setSamplingFraction(self, value):
        """
        Args:
            samplingFraction: The fraction of superpixels (for image) or tokens (for text) to keep on
        """

        self._set(samplingFraction=value)
        return self

    def setSuperpixelCol(self, value):
        """
        Args:
            superpixelCol: The column holding the superpixel decompositions
        """

        self._set(superpixelCol=value)
        return self

    def setTargetClass(self, value):
        """
        Args:
            targetClass: The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        """

        self._set(targetClass=value)
        return self

    def setTargetClassCol(self, value):
        """
        Args:
            targetClassCol: The name of the column that specifies the index of the class for multinomial classification models.
        """

        self._set(targetClassCol=value)
        return self

    def setTargetCol(self, value):
        """
        Args:
            targetCol: The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        """

        self._set(targetCol=value)
        return self

    def getCellSize(self):
        """
        Returns:
            cellSize: Number that controls the size of the superpixels
        """

        return self.getOrDefault(self.cellSize)

    def getInputCol(self):
        """
        Returns:
            inputCol: input column name
        """

        return self.getOrDefault(self.inputCol)

    def getKernelWidth(self):
        """
        Returns:
            kernelWidth: Kernel width. Default value: sqrt (number of features) * 0.75
        """

        return self.getOrDefault(self.kernelWidth)

    def getMetricsCol(self):
        """
        Returns:
            metricsCol: Column name for fitting metrics
        """

        return self.getOrDefault(self.metricsCol)

    def getModel(self):
        """
        Returns:
            model: The model to be interpreted.
        """

        return JavaParams._from_java(self._java_obj.getModel())

    def getModifier(self):
        """
        Returns:
            modifier: Controls the trade-off spatial and color distance
        """

        return self.getOrDefault(self.modifier)

    def getNumSamples(self):
        """
        Returns:
            numSamples: Number of samples to generate.
        """

        return self.getOrDefault(self.numSamples)

    def getOutputCol(self):
        """
        Returns:
            outputCol: output column name
        """

        return self.getOrDefault(self.outputCol)

    def getRegularization(self):
        """
        Returns:
            regularization: Regularization param for the lasso. Default value: 0.
        """

        return self.getOrDefault(self.regularization)

    def getSamplingFraction(self):
        """
        Returns:
            samplingFraction: The fraction of superpixels (for image) or tokens (for text) to keep on
        """

        return self.getOrDefault(self.samplingFraction)

    def getSuperpixelCol(self):
        """
        Returns:
            superpixelCol: The column holding the superpixel decompositions
        """

        return self.getOrDefault(self.superpixelCol)

    def getTargetClass(self):
        """
        Returns:
            targetClass: The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        """

        return self.getOrDefault(self.targetClass)

    def getTargetClassCol(self):
        """
        Returns:
            targetClassCol: The name of the column that specifies the index of the class for multinomial classification models.
        """

        return self.getOrDefault(self.targetClassCol)

    def getTargetCol(self):
        """
        Returns:
            targetCol: The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        """

        return self.getOrDefault(self.targetCol)
