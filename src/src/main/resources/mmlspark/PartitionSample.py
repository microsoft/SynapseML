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
class PartitionSample(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Sampling mode.  The options are:

        - AssignToPartition
        - RandomSample
        - Head

    The default is RandomSample.

    Relevant parameters for the different modes are:

    - When the mode is AssignToPartition:
        - seed - the seed for random partition assignment.
        - numParts - the number of partitions.  The Default is 10.
        - newColName - the name of the partition column.
          The default is "Partition".
    - When the mode is RandomSample:
        - mode - Absolute or Percentage
        - count - the number of rows to assign to each partition when
          Absolute
        - percent - the percentage per partition when Percentage
    - When the mode is Head:
        - count - the number of rows

    Args:

        count (long): Number of rows to return (default: 1000)
        mode (str): AssignToPartition, RandomSample, or Head (default: RandomSample)
        newColName (str): Name of the partition column (default: Partition)
        numParts (int): Number of partitions (default: 10)
        percent (double): Percent of rows to return (default: 0.01)
        rsMode (str): Absolute or Percentage (default: Percentage)
        seed (long): Seed for random operations (default: -1)
    """

    @keyword_only
    def __init__(self, count=1000, mode="RandomSample", newColName="Partition", numParts=10, percent=0.01, rsMode="Percentage", seed=-1):
        super(PartitionSample, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.PartitionSample")
        self.count = Param(self, "count", "count: Number of rows to return (default: 1000)")
        self._setDefault(count=1000)
        self.mode = Param(self, "mode", "mode: AssignToPartition, RandomSample, or Head (default: RandomSample)")
        self._setDefault(mode="RandomSample")
        self.newColName = Param(self, "newColName", "newColName: Name of the partition column (default: Partition)")
        self._setDefault(newColName="Partition")
        self.numParts = Param(self, "numParts", "numParts: Number of partitions (default: 10)")
        self._setDefault(numParts=10)
        self.percent = Param(self, "percent", "percent: Percent of rows to return (default: 0.01)")
        self._setDefault(percent=0.01)
        self.rsMode = Param(self, "rsMode", "rsMode: Absolute or Percentage (default: Percentage)")
        self._setDefault(rsMode="Percentage")
        self.seed = Param(self, "seed", "seed: Seed for random operations (default: -1)")
        self._setDefault(seed=-1)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, count=1000, mode="RandomSample", newColName="Partition", numParts=10, percent=0.01, rsMode="Percentage", seed=-1):
        """
        Set the (keyword only) parameters

        Args:

            count (long): Number of rows to return (default: 1000)
            mode (str): AssignToPartition, RandomSample, or Head (default: RandomSample)
            newColName (str): Name of the partition column (default: Partition)
            numParts (int): Number of partitions (default: 10)
            percent (double): Percent of rows to return (default: 0.01)
            rsMode (str): Absolute or Percentage (default: Percentage)
            seed (long): Seed for random operations (default: -1)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCount(self, value):
        """

        Args:

            count (long): Number of rows to return (default: 1000)

        """
        self._set(count=value)
        return self


    def getCount(self):
        """

        Returns:

            long: Number of rows to return (default: 1000)
        """
        return self.getOrDefault(self.count)


    def setMode(self, value):
        """

        Args:

            mode (str): AssignToPartition, RandomSample, or Head (default: RandomSample)

        """
        self._set(mode=value)
        return self


    def getMode(self):
        """

        Returns:

            str: AssignToPartition, RandomSample, or Head (default: RandomSample)
        """
        return self.getOrDefault(self.mode)


    def setNewColName(self, value):
        """

        Args:

            newColName (str): Name of the partition column (default: Partition)

        """
        self._set(newColName=value)
        return self


    def getNewColName(self):
        """

        Returns:

            str: Name of the partition column (default: Partition)
        """
        return self.getOrDefault(self.newColName)


    def setNumParts(self, value):
        """

        Args:

            numParts (int): Number of partitions (default: 10)

        """
        self._set(numParts=value)
        return self


    def getNumParts(self):
        """

        Returns:

            int: Number of partitions (default: 10)
        """
        return self.getOrDefault(self.numParts)


    def setPercent(self, value):
        """

        Args:

            percent (double): Percent of rows to return (default: 0.01)

        """
        self._set(percent=value)
        return self


    def getPercent(self):
        """

        Returns:

            double: Percent of rows to return (default: 0.01)
        """
        return self.getOrDefault(self.percent)


    def setRsMode(self, value):
        """

        Args:

            rsMode (str): Absolute or Percentage (default: Percentage)

        """
        self._set(rsMode=value)
        return self


    def getRsMode(self):
        """

        Returns:

            str: Absolute or Percentage (default: Percentage)
        """
        return self.getOrDefault(self.rsMode)


    def setSeed(self, value):
        """

        Args:

            seed (long): Seed for random operations (default: -1)

        """
        self._set(seed=value)
        return self


    def getSeed(self):
        """

        Returns:

            long: Seed for random operations (default: -1)
        """
        return self.getOrDefault(self.seed)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.PartitionSample"

    @staticmethod
    def _from_java(java_stage):
        module_name=PartitionSample.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".PartitionSample"
        return from_java(java_stage, module_name)
