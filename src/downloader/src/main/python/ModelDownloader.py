# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from mmlspark.Utils import *

DEFAULT_URL = "https://mmlspark.azureedge.net/datasets/CNTKModels/"


class ModelSchema:
    def __init__(self, name, dataset, modelType, uri, hash, size, inputNode, numLayers, layerNames):
        """
        An object that represents a model.

        :param name: Name of the model
        :param dataset: Dataset it was trained on
        :param modelType: Domain that the model operates on
        :param uri: The location of the model's bytes
        :param hash: The sha256 hash of the models bytes
        :param size: the size of the model in bytes
        :param inputNode: the node which represents the input
        :param numLayers: the number of layers of the model
        :param layerNames: the names of nodes that represent layers in the network
        """
        self.name = name
        self.dataset = dataset
        self.modelType = modelType
        self.uri = uri
        self.hash = hash
        self.size = size
        self.inputNode = inputNode
        self.numLayers = numLayers
        self.layerNames = layerNames

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "ModelSchema<name: {}, dataset: {}, loc: {}>".format(self.name, self.dataset, self.uri)

    def toJava(self, sparkSession):
        ctx = sparkSession.sparkContext
        uri = ctx._jvm.java.net.URI(self.uri)
        return ctx._jvm.com.microsoft.ml.spark.ModelSchema(
            self.name, self.dataset, self.modelType,
            uri, self.hash, self.size, self.inputNode,
            self.numLayers, self.layerNames)

    @staticmethod
    def fromJava(jobj):
        return ModelSchema(jobj.name(), jobj.dataset(),
                           jobj.modelType(), jobj.uri().toString(),
                           jobj.hash(), jobj.size(), jobj.inputNode(),
                           jobj.numLayers(), list(jobj.layerNames()))


class ModelDownloader:
    def __init__(self, sparkSession, localPath, serverURL=DEFAULT_URL):
        """
        A class for downloading CNTK pretrained models in python. To download all models use the downloadModels
        function. To browse models from the microsoft server please use remoteModels.

        :param sparkSession: A spark session for interfacing between python and java
        :param localPath: The folder to save models to
        :param serverURL: The location of the model Server, beware this default can change!
        """
        self.localPath = localPath
        self.serverURL = serverURL

        self._sparkSession = sparkSession
        self._ctx = sparkSession.sparkContext
        self._model_downloader = self._ctx._jvm.com.microsoft.ml.spark.ModelDownloader(
            sparkSession._jsparkSession, localPath, serverURL)

    def _wrap(self, iter):
        return (ModelSchema.fromJava(s) for s in iter)

    def localModels(self):
        return self._wrap(self._model_downloader.localModels())

    def remoteModels(self):
        return self._wrap(self._model_downloader.remoteModels())

    def downloadModel(self, model):
        model = model.toJava(self._sparkSession)
        return ModelSchema.fromJava(self._model_downloader.downloadModel(model))

    def downloadByName(self, name):
        return ModelSchema.fromJava(self._model_downloader.downloadByName(name))

    def downloadModels(self, models=None):
        if models is None:
            models = self.remoteModels()
        models = (m.toJava(self._sparkSession) for m in models)

        return list(self._wrap(self._model_downloader.downloadModels(models)))
