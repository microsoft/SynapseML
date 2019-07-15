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
class _ImageFeaturizer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        cntkModel (object): The internal CNTK model used in the featurizer
        cutOutputLayers (int): The number of layers to cut off the end of the network, 0 leaves the network intact, 1 removes the output layer, etc (default: 1)
        dropNa (bool): Whether to drop na values before mapping (default: true)
        inputCol (str): The name of the input column
        layerNames (list): Array with valid CNTK nodes to choose from, the first entries of this array should be closer to the output node
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, cntkModel=None, cutOutputLayers=1, dropNa=True, inputCol=None, layerNames=None, outputCol=None):
        super(_ImageFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ImageFeaturizer")
        self._cache = {}
        self.cntkModel = Param(self, "cntkModel", "cntkModel: The internal CNTK model used in the featurizer", generateTypeConverter("cntkModel", self._cache, complexTypeConverter))
        self.cutOutputLayers = Param(self, "cutOutputLayers", "cutOutputLayers: The number of layers to cut off the end of the network, 0 leaves the network intact, 1 removes the output layer, etc (default: 1)")
        self._setDefault(cutOutputLayers=1)
        self.dropNa = Param(self, "dropNa", "dropNa: Whether to drop na values before mapping (default: true)")
        self._setDefault(dropNa=True)
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.layerNames = Param(self, "layerNames", "layerNames: Array with valid CNTK nodes to choose from, the first entries of this array should be closer to the output node")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cntkModel=None, cutOutputLayers=1, dropNa=True, inputCol=None, layerNames=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            cntkModel (object): The internal CNTK model used in the featurizer
            cutOutputLayers (int): The number of layers to cut off the end of the network, 0 leaves the network intact, 1 removes the output layer, etc (default: 1)
            dropNa (bool): Whether to drop na values before mapping (default: true)
            inputCol (str): The name of the input column
            layerNames (list): Array with valid CNTK nodes to choose from, the first entries of this array should be closer to the output node
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCntkModel(self, value):
        """

        Args:

            cntkModel (object): The internal CNTK model used in the featurizer

        """
        self._set(cntkModel=value)
        return self


    def getCntkModel(self):
        """

        Returns:

            object: The internal CNTK model used in the featurizer
        """
        return self._cache.get("cntkModel", None)


    def setCutOutputLayers(self, value):
        """

        Args:

            cutOutputLayers (int): The number of layers to cut off the end of the network, 0 leaves the network intact, 1 removes the output layer, etc (default: 1)

        """
        self._set(cutOutputLayers=value)
        return self


    def getCutOutputLayers(self):
        """

        Returns:

            int: The number of layers to cut off the end of the network, 0 leaves the network intact, 1 removes the output layer, etc (default: 1)
        """
        return self.getOrDefault(self.cutOutputLayers)


    def setDropNa(self, value):
        """

        Args:

            dropNa (bool): Whether to drop na values before mapping (default: true)

        """
        self._set(dropNa=value)
        return self


    def getDropNa(self):
        """

        Returns:

            bool: Whether to drop na values before mapping (default: true)
        """
        return self.getOrDefault(self.dropNa)


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


    def setLayerNames(self, value):
        """

        Args:

            layerNames (list): Array with valid CNTK nodes to choose from, the first entries of this array should be closer to the output node

        """
        self._set(layerNames=value)
        return self


    def getLayerNames(self):
        """

        Returns:

            list: Array with valid CNTK nodes to choose from, the first entries of this array should be closer to the output node
        """
        return self.getOrDefault(self.layerNames)


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
        return "com.microsoft.ml.spark.ImageFeaturizer"

    @staticmethod
    def _from_java(java_stage):
        module_name=_ImageFeaturizer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ImageFeaturizer"
        return from_java(java_stage, module_name)
