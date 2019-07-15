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
class _ImageTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Implements an image processing stage.  Provides an interface to OpenCV
    image processing functionality.  Use ``ImageTransform`` to set the
    parameters for the image processing stage, then use the
    ``ImageTransformer`` to specify the input and output columns for
    processing and apply the tranformations.

    Examples can be found in the sample notebook.

    Args:

        inputCol (str): The name of the input column (default: image)
        outputCol (str): The name of the output column (default: [self.uid]_output)
        stages (object): Image transformation stages
    """

    @keyword_only
    def __init__(self, inputCol="image", outputCol=None, stages=None):
        super(_ImageTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ImageTransformer")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column (default: image)")
        self._setDefault(inputCol="image")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.stages = Param(self, "stages", "stages: Image transformation stages")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol="image", outputCol=None, stages=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column (default: image)
            outputCol (str): The name of the output column (default: [self.uid]_output)
            stages (object): Image transformation stages
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column (default: image)

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column (default: image)
        """
        return self.getOrDefault(self.inputCol)


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


    def setStages(self, value):
        """

        Args:

            stages (object): Image transformation stages

        """
        self._set(stages=value)
        return self


    def getStages(self):
        """

        Returns:

            object: Image transformation stages
        """
        return self.getOrDefault(self.stages)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ImageTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=_ImageTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ImageTransformer"
        return from_java(java_stage, module_name)
