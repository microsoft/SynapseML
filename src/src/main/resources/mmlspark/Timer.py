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
class Timer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        disableMaterialization (bool): Whether to disable timing (so that one can turn it off for evaluation) (default: true)
        logToScala (bool): Whether to output the time to the scala console (default: true)
        stage (object): The stage to time
    """

    @keyword_only
    def __init__(self, disableMaterialization=True, logToScala=True, stage=None):
        super(Timer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Timer")
        self._cache = {}
        self.disableMaterialization = Param(self, "disableMaterialization", "disableMaterialization: Whether to disable timing (so that one can turn it off for evaluation) (default: true)")
        self._setDefault(disableMaterialization=True)
        self.logToScala = Param(self, "logToScala", "logToScala: Whether to output the time to the scala console (default: true)")
        self._setDefault(logToScala=True)
        self.stage = Param(self, "stage", "stage: The stage to time", generateTypeConverter("stage", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, disableMaterialization=True, logToScala=True, stage=None):
        """
        Set the (keyword only) parameters

        Args:

            disableMaterialization (bool): Whether to disable timing (so that one can turn it off for evaluation) (default: true)
            logToScala (bool): Whether to output the time to the scala console (default: true)
            stage (object): The stage to time
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDisableMaterialization(self, value):
        """

        Args:

            disableMaterialization (bool): Whether to disable timing (so that one can turn it off for evaluation) (default: true)

        """
        self._set(disableMaterialization=value)
        return self


    def getDisableMaterialization(self):
        """

        Returns:

            bool: Whether to disable timing (so that one can turn it off for evaluation) (default: true)
        """
        return self.getOrDefault(self.disableMaterialization)


    def setLogToScala(self, value):
        """

        Args:

            logToScala (bool): Whether to output the time to the scala console (default: true)

        """
        self._set(logToScala=value)
        return self


    def getLogToScala(self):
        """

        Returns:

            bool: Whether to output the time to the scala console (default: true)
        """
        return self.getOrDefault(self.logToScala)


    def setStage(self, value):
        """

        Args:

            stage (object): The stage to time

        """
        self._set(stage=value)
        return self


    def getStage(self):
        """

        Returns:

            object: The stage to time
        """
        return self._cache.get("stage", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.Timer"

    @staticmethod
    def _from_java(java_stage):
        module_name=Timer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Timer"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return TimerModel(java_model)


class TimerModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`Timer`.

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
        return "com.microsoft.ml.spark.TimerModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=TimerModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TimerModel"
        return from_java(java_stage, module_name)

