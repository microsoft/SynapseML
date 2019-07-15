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
class _CNTKLearner(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    ``CNTKLearner`` trains a model on a dataset on a GPU edge node.  The
    result is a ``CNTKModel``.

    Args:

        brainScript (str): String of BrainScript config
        dataFormat (str): Transfer format (default: text)
        dataTransfer (str): Transfer strategy (default: local)
        featureCount (int): Number of features for reduction (default: 1)
        featuresColumnName (str): Features column name (default: features)
        gpuMachines (list): GPU machines to train on (default: [Ljava.lang.String;@17c8b8f0)
        labelsColumnName (str): Label column name (default: labels)
        localHdfsMount (str): Local mount point for hdfs:///
        parallelTrain (bool): Train using an MPI ring (default: true)
        username (str): Username for the GPU VM (default: sshuser)
        weightPrecision (str): Weights (default: float)
        workingDir (str): Working directory for CNTK (default: tmp)
    """

    @keyword_only
    def __init__(self, brainScript=None, dataFormat="text", dataTransfer="local", featureCount=1, featuresColumnName="features", gpuMachines=["127.0.0.1,1"], labelsColumnName="labels", localHdfsMount=None, parallelTrain=True, username="sshuser", weightPrecision="float", workingDir="tmp"):
        super(_CNTKLearner, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.CNTKLearner")
        self.brainScript = Param(self, "brainScript", "brainScript: String of BrainScript config")
        self.dataFormat = Param(self, "dataFormat", "dataFormat: Transfer format (default: text)")
        self._setDefault(dataFormat="text")
        self.dataTransfer = Param(self, "dataTransfer", "dataTransfer: Transfer strategy (default: local)")
        self._setDefault(dataTransfer="local")
        self.featureCount = Param(self, "featureCount", "featureCount: Number of features for reduction (default: 1)")
        self._setDefault(featureCount=1)
        self.featuresColumnName = Param(self, "featuresColumnName", "featuresColumnName: Features column name (default: features)")
        self._setDefault(featuresColumnName="features")
        self.gpuMachines = Param(self, "gpuMachines", "gpuMachines: GPU machines to train on (default: [Ljava.lang.String;@17c8b8f0)")
        self._setDefault(gpuMachines=["127.0.0.1,1"])
        self.labelsColumnName = Param(self, "labelsColumnName", "labelsColumnName: Label column name (default: labels)")
        self._setDefault(labelsColumnName="labels")
        self.localHdfsMount = Param(self, "localHdfsMount", "localHdfsMount: Local mount point for hdfs:///")
        self.parallelTrain = Param(self, "parallelTrain", "parallelTrain: Train using an MPI ring (default: true)")
        self._setDefault(parallelTrain=True)
        self.username = Param(self, "username", "username: Username for the GPU VM (default: sshuser)")
        self._setDefault(username="sshuser")
        self.weightPrecision = Param(self, "weightPrecision", "weightPrecision: Weights (default: float)")
        self._setDefault(weightPrecision="float")
        self.workingDir = Param(self, "workingDir", "workingDir: Working directory for CNTK (default: tmp)")
        self._setDefault(workingDir="tmp")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, brainScript=None, dataFormat="text", dataTransfer="local", featureCount=1, featuresColumnName="features", gpuMachines=["127.0.0.1,1"], labelsColumnName="labels", localHdfsMount=None, parallelTrain=True, username="sshuser", weightPrecision="float", workingDir="tmp"):
        """
        Set the (keyword only) parameters

        Args:

            brainScript (str): String of BrainScript config
            dataFormat (str): Transfer format (default: text)
            dataTransfer (str): Transfer strategy (default: local)
            featureCount (int): Number of features for reduction (default: 1)
            featuresColumnName (str): Features column name (default: features)
            gpuMachines (list): GPU machines to train on (default: [Ljava.lang.String;@17c8b8f0)
            labelsColumnName (str): Label column name (default: labels)
            localHdfsMount (str): Local mount point for hdfs:///
            parallelTrain (bool): Train using an MPI ring (default: true)
            username (str): Username for the GPU VM (default: sshuser)
            weightPrecision (str): Weights (default: float)
            workingDir (str): Working directory for CNTK (default: tmp)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBrainScript(self, value):
        """

        Args:

            brainScript (str): String of BrainScript config

        """
        self._set(brainScript=value)
        return self


    def getBrainScript(self):
        """

        Returns:

            str: String of BrainScript config
        """
        return self.getOrDefault(self.brainScript)


    def setDataFormat(self, value):
        """

        Args:

            dataFormat (str): Transfer format (default: text)

        """
        self._set(dataFormat=value)
        return self


    def getDataFormat(self):
        """

        Returns:

            str: Transfer format (default: text)
        """
        return self.getOrDefault(self.dataFormat)


    def setDataTransfer(self, value):
        """

        Args:

            dataTransfer (str): Transfer strategy (default: local)

        """
        self._set(dataTransfer=value)
        return self


    def getDataTransfer(self):
        """

        Returns:

            str: Transfer strategy (default: local)
        """
        return self.getOrDefault(self.dataTransfer)


    def setFeatureCount(self, value):
        """

        Args:

            featureCount (int): Number of features for reduction (default: 1)

        """
        self._set(featureCount=value)
        return self


    def getFeatureCount(self):
        """

        Returns:

            int: Number of features for reduction (default: 1)
        """
        return self.getOrDefault(self.featureCount)


    def setFeaturesColumnName(self, value):
        """

        Args:

            featuresColumnName (str): Features column name (default: features)

        """
        self._set(featuresColumnName=value)
        return self


    def getFeaturesColumnName(self):
        """

        Returns:

            str: Features column name (default: features)
        """
        return self.getOrDefault(self.featuresColumnName)


    def setGpuMachines(self, value):
        """

        Args:

            gpuMachines (list): GPU machines to train on (default: [Ljava.lang.String;@17c8b8f0)

        """
        self._set(gpuMachines=value)
        return self


    def getGpuMachines(self):
        """

        Returns:

            list: GPU machines to train on (default: [Ljava.lang.String;@17c8b8f0)
        """
        return self.getOrDefault(self.gpuMachines)


    def setLabelsColumnName(self, value):
        """

        Args:

            labelsColumnName (str): Label column name (default: labels)

        """
        self._set(labelsColumnName=value)
        return self


    def getLabelsColumnName(self):
        """

        Returns:

            str: Label column name (default: labels)
        """
        return self.getOrDefault(self.labelsColumnName)


    def setLocalHdfsMount(self, value):
        """

        Args:

            localHdfsMount (str): Local mount point for hdfs:///

        """
        self._set(localHdfsMount=value)
        return self


    def getLocalHdfsMount(self):
        """

        Returns:

            str: Local mount point for hdfs:///
        """
        return self.getOrDefault(self.localHdfsMount)


    def setParallelTrain(self, value):
        """

        Args:

            parallelTrain (bool): Train using an MPI ring (default: true)

        """
        self._set(parallelTrain=value)
        return self


    def getParallelTrain(self):
        """

        Returns:

            bool: Train using an MPI ring (default: true)
        """
        return self.getOrDefault(self.parallelTrain)


    def setUsername(self, value):
        """

        Args:

            username (str): Username for the GPU VM (default: sshuser)

        """
        self._set(username=value)
        return self


    def getUsername(self):
        """

        Returns:

            str: Username for the GPU VM (default: sshuser)
        """
        return self.getOrDefault(self.username)


    def setWeightPrecision(self, value):
        """

        Args:

            weightPrecision (str): Weights (default: float)

        """
        self._set(weightPrecision=value)
        return self


    def getWeightPrecision(self):
        """

        Returns:

            str: Weights (default: float)
        """
        return self.getOrDefault(self.weightPrecision)


    def setWorkingDir(self, value):
        """

        Args:

            workingDir (str): Working directory for CNTK (default: tmp)

        """
        self._set(workingDir=value)
        return self


    def getWorkingDir(self):
        """

        Returns:

            str: Working directory for CNTK (default: tmp)
        """
        return self.getOrDefault(self.workingDir)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.CNTKLearner"

    @staticmethod
    def _from_java(java_stage):
        module_name=_CNTKLearner.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CNTKLearner"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _CNTKModel(java_model)


class _CNTKModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_CNTKLearner`.

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
        return "com.microsoft.ml.spark.CNTKModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_CNTKModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".CNTKModel"
        return from_java(java_stage, module_name)

