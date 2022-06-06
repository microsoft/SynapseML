# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from horovod.spark.common.params import EstimatorParams
from pyspark.ml.param.shared import Param, Params, TypeConverters


class LightningParams(EstimatorParams):
    input_shapes = Param(Params._dummy(), "input_shapes", "input layer shapes")

    loss_constructors = Param(
        Params._dummy(), "loss_constructors", "functions that construct the loss"
    )
    train_minibatch_fn = Param(
        Params._dummy(),
        "train_minibatch_fn",
        "functions that construct the minibatch train function for torch",
    )

    inmemory_cache_all = Param(
        Params._dummy(),
        "inmemory_cache_all",
        "Cache the data in memory for training and validation.",
        typeConverter=TypeConverters.toBoolean,
    )

    num_gpus = Param(
        Params._dummy(),
        "num_gpus",
        "Number of gpus per process, default to 1 when CUDA is available in the backend, otherwise 0.",
    )

    logger = Param(Params._dummy(), "logger", "optional, pytorch lightning logger.")

    log_every_n_steps = Param(
        Params._dummy(),
        "log_every_n_steps",
        "control the frequency of logging",
        typeConverter=TypeConverters.toInt,
    )

    data_module = Param(
        Params._dummy(),
        "data_module",
        "(Optional) Lightning datamodule used for training and validadation, if not set, lightning trainer will use PetastormDataModule as default..",
    )

    loader_num_epochs = Param(
        Params._dummy(),
        "loader_num_epochs",
        "An epoch is a single pass over all rows in the dataset. Default to None, which means reader will be in infinite loop mode, and generate unlimite data as needed. ",
    )

    terminate_on_nan = Param(
        Params._dummy(),
        "terminate_on_nan",
        "terminate on encountering NaN",
        typeConverter=TypeConverters.toBoolean,
    )

    profiler = Param(Params._dummy(), "profiler", "lightning profiler to use")

    def __init__(self):
        super(LightningParams, self).__init__()

        self._setDefault(
            input_shapes=None,
            loss_constructors=None,
            train_minibatch_fn=None,
            inmemory_cache_all=False,
            num_gpus=None,
            logger=None,
            log_every_n_steps=50,
            data_module=None,
            loader_num_epochs=None,
            terminate_on_nan=False,
            profiler=None,
        )

    def setInputShapes(self, value):
        return self._set(input_shapes=value)

    def getInputShapes(self):
        return self.getOrDefault(self.input_shapes)

    def setLossConstructors(self, value):
        return self._set(loss_constructors=value)

    def getLossConstructors(self):
        return self.getOrDefault(self.loss_constructors)

    def setTrainMinibatchFn(self, value):
        return self._set(train_minibatch_fn=value)

    def getTrainMinibatchFn(self):
        return self.getOrDefault(self.train_minibatch_fn)

    def setInMemoryCacheAll(self, value):
        return self._set(inmemory_cache_all=value)

    def getInMemoryCacheAll(self):
        return self.getOrDefault(self.inmemory_cache_all)

    def setNumGPUs(self, value):
        return self._set(num_gpus=value)

    def getNumGPUs(self):
        return self.getOrDefault(self.num_gpus)

    def setLogger(self, value):
        return self._set(logger=value)

    def getLogger(self):
        return self.getOrDefault(self.logger)

    def setLogEveryNSteps(self, value):
        return self._set(log_every_n_steps=value)

    def getLogEveryNSteps(self):
        return self.getOrDefault(self.log_every_n_steps)

    def setDataModule(self, value):
        return self._set(data_module=value)

    def getDataModule(self):
        return self.getOrDefault(self.data_module)

    def setLoaderNumEpochs(self, value):
        return self._set(loader_num_epochs=value)

    def getLoaderNumEpochs(self):
        return self.getOrDefault(self.loader_num_epochs)

    def setTerminateOnNan(self, value):
        return self._set(terminate_on_nan=value)

    def getTerminateOnNan(self):
        return self.getOrDefault(self.terminate_on_nan)

    def getProfiler(self):
        return self.getOrDefault(self.profiler)
