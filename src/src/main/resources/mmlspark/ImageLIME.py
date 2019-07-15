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
class ImageLIME(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        cellSize (double): Number that controls the size of the superpixels (default: 16.0)
        inputCol (str): The name of the input column
        model (object): Model to try to locally approximate
        modifier (double): Controls the trade-off spatial and color distance (default: 130.0)
        nSamples (int): The number of samples to generate (default: 900)
        outputCol (str): The name of the output column
        predictionCol (str): prediction column name (default: prediction)
        regularization (double): regularization param for the lasso (default: 0.0)
        samplingFraction (double): The fraction of superpixels to keep on (default: 0.3)
        superpixelCol (str): The column holding the superpixel decompositions (default: superpixels)
    """

    @keyword_only
    def __init__(self, cellSize=16.0, inputCol=None, model=None, modifier=130.0, nSamples=900, outputCol=None, predictionCol="prediction", regularization=0.0, samplingFraction=0.3, superpixelCol="superpixels"):
        super(ImageLIME, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ImageLIME")
        self._cache = {}
        self.cellSize = Param(self, "cellSize", "cellSize: Number that controls the size of the superpixels (default: 16.0)")
        self._setDefault(cellSize=16.0)
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.model = Param(self, "model", "model: Model to try to locally approximate", generateTypeConverter("model", self._cache, complexTypeConverter))
        self.modifier = Param(self, "modifier", "modifier: Controls the trade-off spatial and color distance (default: 130.0)")
        self._setDefault(modifier=130.0)
        self.nSamples = Param(self, "nSamples", "nSamples: The number of samples to generate (default: 900)")
        self._setDefault(nSamples=900)
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.predictionCol = Param(self, "predictionCol", "predictionCol: prediction column name (default: prediction)")
        self._setDefault(predictionCol="prediction")
        self.regularization = Param(self, "regularization", "regularization: regularization param for the lasso (default: 0.0)")
        self._setDefault(regularization=0.0)
        self.samplingFraction = Param(self, "samplingFraction", "samplingFraction: The fraction of superpixels to keep on (default: 0.3)")
        self._setDefault(samplingFraction=0.3)
        self.superpixelCol = Param(self, "superpixelCol", "superpixelCol: The column holding the superpixel decompositions (default: superpixels)")
        self._setDefault(superpixelCol="superpixels")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cellSize=16.0, inputCol=None, model=None, modifier=130.0, nSamples=900, outputCol=None, predictionCol="prediction", regularization=0.0, samplingFraction=0.3, superpixelCol="superpixels"):
        """
        Set the (keyword only) parameters

        Args:

            cellSize (double): Number that controls the size of the superpixels (default: 16.0)
            inputCol (str): The name of the input column
            model (object): Model to try to locally approximate
            modifier (double): Controls the trade-off spatial and color distance (default: 130.0)
            nSamples (int): The number of samples to generate (default: 900)
            outputCol (str): The name of the output column
            predictionCol (str): prediction column name (default: prediction)
            regularization (double): regularization param for the lasso (default: 0.0)
            samplingFraction (double): The fraction of superpixels to keep on (default: 0.3)
            superpixelCol (str): The column holding the superpixel decompositions (default: superpixels)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCellSize(self, value):
        """

        Args:

            cellSize (double): Number that controls the size of the superpixels (default: 16.0)

        """
        self._set(cellSize=value)
        return self


    def getCellSize(self):
        """

        Returns:

            double: Number that controls the size of the superpixels (default: 16.0)
        """
        return self.getOrDefault(self.cellSize)


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


    def setModifier(self, value):
        """

        Args:

            modifier (double): Controls the trade-off spatial and color distance (default: 130.0)

        """
        self._set(modifier=value)
        return self


    def getModifier(self):
        """

        Returns:

            double: Controls the trade-off spatial and color distance (default: 130.0)
        """
        return self.getOrDefault(self.modifier)


    def setNSamples(self, value):
        """

        Args:

            nSamples (int): The number of samples to generate (default: 900)

        """
        self._set(nSamples=value)
        return self


    def getNSamples(self):
        """

        Returns:

            int: The number of samples to generate (default: 900)
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


    def setSuperpixelCol(self, value):
        """

        Args:

            superpixelCol (str): The column holding the superpixel decompositions (default: superpixels)

        """
        self._set(superpixelCol=value)
        return self


    def getSuperpixelCol(self):
        """

        Returns:

            str: The column holding the superpixel decompositions (default: superpixels)
        """
        return self.getOrDefault(self.superpixelCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ImageLIME"

    @staticmethod
    def _from_java(java_stage):
        module_name=ImageLIME.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ImageLIME"
        return from_java(java_stage, module_name)
