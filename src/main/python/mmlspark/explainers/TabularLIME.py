# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys

if sys.version >= "3":
    basestring = str

from pyspark.sql import DataFrame
from pyspark.ml.param.shared import *
from pyspark import keyword_only
from mmlspark.core.serialize.java_params_patch import *
from pyspark.ml.common import inherit_doc
from mmlspark.core.schema.Utils import *
from pyspark.ml.param import TypeConverters
from mmlspark.explainers._LocalExplainer import _LocalExplainer


@inherit_doc
class TabularLIME(_LocalExplainer):
    """
    Args:
        categoricalFeatures (list): Name of features that should be treated as categorical variables.
        inputCols (list): input column names
        kernelWidth (float): Kernel width. Default value: sqrt (number of features) * 0.75
        metricsCol (object): Column name for fitting metrics
        model (object): The model to be interpreted.
        numSamples (int): Number of samples to generate.
        outputCol (object): output column name
        regularization (float): Regularization param for the lasso. Default value: 0.
        targetClass (int): The index of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        targetClassCol (object): The name of the column that specifies the index of the class for multinomial classification models.
        targetCol (object): The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
    """

    categoricalFeatures = Param(
        Params._dummy(),
        "categoricalFeatures",
        "Name of features that should be treated as categorical variables.",
        typeConverter=TypeConverters.toListString,
    )

    inputCols = Param(Params._dummy(), "inputCols", "input column names", typeConverter=TypeConverters.toListString)

    kernelWidth = Param(
        Params._dummy(),
        "kernelWidth",
        "Kernel width. Default value: sqrt (number of features) * 0.75",
        typeConverter=TypeConverters.toFloat,
    )

    metricsCol = Param(Params._dummy(), "metricsCol", "Column name for fitting metrics")

    model = Param(Params._dummy(), "model", "The model to be interpreted.")

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
        categoricalFeatures=[],
        inputCols=None,
        kernelWidth=0.75,
        metricsCol="r2",
        model=None,
        numSamples=1000,
        outputCol="TabularLIME_fece885d7340__output",
        regularization=0.0,
        targetClass=0,
        targetClassCol=None,
        targetCol="probability",
        backgroundData: DataFrame = None,
    ):
        super(TabularLIME, self).__init__()
        if java_obj is None:
            self._java_obj = self._new_java_obj("com.microsoft.ml.spark.explainers.TabularLIME", self.uid)
        else:
            self._java_obj = java_obj
        self._setDefault(categoricalFeatures=[])
        self._setDefault(kernelWidth=0.75)
        self._setDefault(metricsCol="r2")
        self._setDefault(numSamples=1000)
        self._setDefault(outputCol="TabularLIME_fece885d7340__output")
        self._setDefault(regularization=0.0)
        self._setDefault(targetClass=0)
        self._setDefault(targetCol="probability")
        self.setBackgroundData(backgroundData)
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
        categoricalFeatures=[],
        inputCols=None,
        kernelWidth=0.75,
        metricsCol="r2",
        model=None,
        numSamples=1000,
        outputCol="TabularLIME_fece885d7340__output",
        regularization=0.0,
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
        return "com.microsoft.ml.spark.explainers.TabularLIME"

    @staticmethod
    def _from_java(java_stage):
        module_name = TabularLIME.__module__
        module_name = module_name.rsplit(".", 1)[0] + ".TabularLIME"
        return from_java(java_stage, module_name)

    def setBackgroundData(self, value: DataFrame):
        """
        Args:
            value: A Spark DataFrame holding the background dataset.
        """
        if isinstance(value, DataFrame):
            self._java_obj.setBackgroundDataset(value._jdf)
        else:
            raise ValueError("The value is not a Spark DataFrame.")

    def setCategoricalFeatures(self, value):
        """
        Args:
            categoricalFeatures: Name of features that should be treated as categorical variables.
        """

        self._set(categoricalFeatures=value)
        return self

    def setInputCols(self, value):
        """
        Args:
            inputCols: input column names
        """

        self._set(inputCols=value)
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

    def getCategoricalFeatures(self):
        """
        Returns:
            categoricalFeatures: Name of features that should be treated as categorical variables.
        """

        return self.getOrDefault(self.categoricalFeatures)

    def getInputCols(self):
        """
        Returns:
            inputCols: input column names
        """

        return self.getOrDefault(self.inputCols)

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
