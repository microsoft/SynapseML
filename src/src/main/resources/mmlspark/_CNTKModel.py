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
class _CNTKModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    The ``CNTKModel`` evaluates a pre-trained CNTK model in parallel.  The
    ``CNTKModel`` takes a path to a model and automatically loads and
    distributes the model to workers for parallel evaluation using CNTK's
    java bindings.

    The ``CNTKModel`` loads the pretrained model into the ``Function`` class
    of CNTK.  One can decide which node of the CNTK Function computation
    graph to evaluate by passing in the name of the output node with the
    output node parameter.  Currently the ``CNTKModel`` supports single
    input single output models.

    The ``CNTKModel`` takes an input column which should be a column of
    spark vectors and returns a column of spark vectors representing the
    activations of the selected node.  By default, the CNTK model defaults
    to using the model's first input and first output node.

    Args:

        batchInput (bool): whether to use a batcher (default: true)
        convertOutputToDenseVector (bool): whether to convert the output to dense vectors (default: true)
        feedDict (dict): Map of CNTK Variable names (keys) and Column Names (values) (default: Map(ARGUMENT_0 -> ARGUMENT_0))
        fetchDict (dict): Map of Column Names (keys) and CNTK Variable names (values) (default: Map(OUTPUT_0 -> OUTPUT_0))
        miniBatcher (object): Minibatcher to use (default: FixedMiniBatchTransformer_2f2aa10badc5)
        model (object): Array of bytes containing the serialized CNTKModel
        shapeOutput (bool): whether to shape the output (default: false)
    """

    @keyword_only
    def __init__(self, batchInput=True, convertOutputToDenseVector=True, feedDict={"ARGUMENT_0":"ARGUMENT_0"}, fetchDict={"OUTPUT_0":"OUTPUT_0"}, miniBatcher=None, model=None, shapeOutput=False):
        super(_CNTKModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.CNTKModel")
        self._cache = {}
        self.batchInput = Param(self, "batchInput", "batchInput: whether to use a batcher (default: true)")
        self._setDefault(batchInput=True)
        self.convertOutputToDenseVector = Param(self, "convertOutputToDenseVector", "convertOutputToDenseVector: whether to convert the output to dense vectors (default: true)")
        self._setDefault(convertOutputToDenseVector=True)
        self.feedDict = Param(self, "feedDict", "feedDict:  Map of CNTK Variable names (keys) and Column Names (values) (default: Map(ARGUMENT_0 -> ARGUMENT_0))")
        self._setDefault(feedDict={"ARGUMENT_0":"ARGUMENT_0"})
        self.fetchDict = Param(self, "fetchDict", "fetchDict:  Map of Column Names (keys) and CNTK Variable names (values) (default: Map(OUTPUT_0 -> OUTPUT_0))")
        self._setDefault(fetchDict={"OUTPUT_0":"OUTPUT_0"})
        self.miniBatcher = Param(self, "miniBatcher", "miniBatcher: Minibatcher to use (default: FixedMiniBatchTransformer_2f2aa10badc5)", generateTypeConverter("miniBatcher", self._cache, complexTypeConverter))
        self.model = Param(self, "model", "model: Array of bytes containing the serialized CNTKModel")
        self.shapeOutput = Param(self, "shapeOutput", "shapeOutput: whether to shape the output (default: false)")
        self._setDefault(shapeOutput=False)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, batchInput=True, convertOutputToDenseVector=True, feedDict={"ARGUMENT_0":"ARGUMENT_0"}, fetchDict={"OUTPUT_0":"OUTPUT_0"}, miniBatcher=None, model=None, shapeOutput=False):
        """
        Set the (keyword only) parameters

        Args:

            batchInput (bool): whether to use a batcher (default: true)
            convertOutputToDenseVector (bool): whether to convert the output to dense vectors (default: true)
            feedDict (dict): Map of CNTK Variable names (keys) and Column Names (values) (default: Map(ARGUMENT_0 -> ARGUMENT_0))
            fetchDict (dict): Map of Column Names (keys) and CNTK Variable names (values) (default: Map(OUTPUT_0 -> OUTPUT_0))
            miniBatcher (object): Minibatcher to use (default: FixedMiniBatchTransformer_2f2aa10badc5)
            model (object): Array of bytes containing the serialized CNTKModel
            shapeOutput (bool): whether to shape the output (default: false)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBatchInput(self, value):
        """

        Args:

            batchInput (bool): whether to use a batcher (default: true)

        """
        self._set(batchInput=value)
        return self


    def getBatchInput(self):
        """

        Returns:

            bool: whether to use a batcher (default: true)
        """
        return self.getOrDefault(self.batchInput)


    def setConvertOutputToDenseVector(self, value):
        """

        Args:

            convertOutputToDenseVector (bool): whether to convert the output to dense vectors (default: true)

        """
        self._set(convertOutputToDenseVector=value)
        return self


    def getConvertOutputToDenseVector(self):
        """

        Returns:

            bool: whether to convert the output to dense vectors (default: true)
        """
        return self.getOrDefault(self.convertOutputToDenseVector)


    def setFeedDict(self, value):
        """

        Args:

            feedDict (dict): Map of CNTK Variable names (keys) and Column Names (values) (default: Map(ARGUMENT_0 -> ARGUMENT_0))

        """
        self._set(feedDict=value)
        return self


    def getFeedDict(self):
        """

        Returns:

            dict: Map of CNTK Variable names (keys) and Column Names (values) (default: Map(ARGUMENT_0 -> ARGUMENT_0))
        """
        return self.getOrDefault(self.feedDict)


    def setFetchDict(self, value):
        """

        Args:

            fetchDict (dict): Map of Column Names (keys) and CNTK Variable names (values) (default: Map(OUTPUT_0 -> OUTPUT_0))

        """
        self._set(fetchDict=value)
        return self


    def getFetchDict(self):
        """

        Returns:

            dict: Map of Column Names (keys) and CNTK Variable names (values) (default: Map(OUTPUT_0 -> OUTPUT_0))
        """
        return self.getOrDefault(self.fetchDict)


    def setMiniBatcher(self, value):
        """

        Args:

            miniBatcher (object): Minibatcher to use (default: FixedMiniBatchTransformer_2f2aa10badc5)

        """
        self._set(miniBatcher=value)
        return self


    def getMiniBatcher(self):
        """

        Returns:

            object: Minibatcher to use (default: FixedMiniBatchTransformer_2f2aa10badc5)
        """
        return self._cache.get("miniBatcher", None)


    def setModel(self, value):
        """

        Args:

            model (object): Array of bytes containing the serialized CNTKModel

        """
        self._set(model=value)
        return self


    def getModel(self):
        """

        Returns:

            object: Array of bytes containing the serialized CNTKModel
        """
        return self.getOrDefault(self.model)


    def setShapeOutput(self, value):
        """

        Args:

            shapeOutput (bool): whether to shape the output (default: false)

        """
        self._set(shapeOutput=value)
        return self


    def getShapeOutput(self):
        """

        Returns:

            bool: whether to shape the output (default: false)
        """
        return self.getOrDefault(self.shapeOutput)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.CNTKModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_CNTKModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CNTKModel"
        return from_java(java_stage, module_name)
