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
class CheckpointData(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    ``CheckpointData`` persists data to disk as well as memory.

    Storage level is MEMORY_AND_DISK if true, else MEMORY_ONLY.
    The default is false (MEMORY_ONLY).

    Use the removeCheckpoint parameter to reverse the cache operation.

    Args:

        diskIncluded (bool): Persist to disk as well as memory (default: false)
        removeCheckpoint (bool): Unpersist a cached dataset (default: false)
    """

    @keyword_only
    def __init__(self, diskIncluded=False, removeCheckpoint=False):
        super(CheckpointData, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.CheckpointData")
        self.diskIncluded = Param(self, "diskIncluded", "diskIncluded: Persist to disk as well as memory (default: false)")
        self._setDefault(diskIncluded=False)
        self.removeCheckpoint = Param(self, "removeCheckpoint", "removeCheckpoint: Unpersist a cached dataset (default: false)")
        self._setDefault(removeCheckpoint=False)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, diskIncluded=False, removeCheckpoint=False):
        """
        Set the (keyword only) parameters

        Args:

            diskIncluded (bool): Persist to disk as well as memory (default: false)
            removeCheckpoint (bool): Unpersist a cached dataset (default: false)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setDiskIncluded(self, value):
        """

        Args:

            diskIncluded (bool): Persist to disk as well as memory (default: false)

        """
        self._set(diskIncluded=value)
        return self


    def getDiskIncluded(self):
        """

        Returns:

            bool: Persist to disk as well as memory (default: false)
        """
        return self.getOrDefault(self.diskIncluded)


    def setRemoveCheckpoint(self, value):
        """

        Args:

            removeCheckpoint (bool): Unpersist a cached dataset (default: false)

        """
        self._set(removeCheckpoint=value)
        return self


    def getRemoveCheckpoint(self):
        """

        Returns:

            bool: Unpersist a cached dataset (default: false)
        """
        return self.getOrDefault(self.removeCheckpoint)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.CheckpointData"

    @staticmethod
    def _from_java(java_stage):
        module_name=CheckpointData.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CheckpointData"
        return from_java(java_stage, module_name)
