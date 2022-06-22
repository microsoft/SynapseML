# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark import keyword_only
from pyspark.ml.param.shared import Param, Params
from synapse.ml.dl.LightningEstimator import LightningEstimator
from synapse.ml.dl.LitDeepVisionModel import LitDeepVisionModel


class DeepVisionClassifier(LightningEstimator):

    backbone = Param(
        Params._dummy(), "backbone", "backbone of the deep vision classifier"
    )

    num_layers_to_fine_tune = Param(
        Params._dummy(),
        "num_layers_to_fine_tune",
        "number of last layers to fine tune for the model, should be smaller or equal to 3",
    )

    num_classes = Param(Params._dummy(), "num_classes", "number of target classes")

    loss_name = Param(
        Params._dummy(),
        "loss_name",
        "string representation of torch.nn.functional loss function for the underlying pytorch_lightning model, e.g. binary_cross_entropy",
    )

    optimizer_name = Param(
        Params._dummy(),
        "optimizer_name",
        "string representation of optimizer function for the underlying pytorch_lightning model",
    )

    pretrained = Param(
        Params._dummy(),
        "pretrained",
        "True/False, whether to use pretrained weights for the backbone model or not",
    )

    feature_extracting = Param(
        Params._dummy(),
        "feature_extracting",
        "True/False, whether to apply feature extracting on the backbone model or not",
    )

    dropout_aux = Param(
        Params._dummy(),
        "dropout_aux",
        "numeric value that's applied to googlenet InceptionAux module's dropout layer only: probability of an element to be zeroed",
    )

    @keyword_only
    def __init__(
        self,
        backbone,
        num_layers_to_fine_tune=0,
        num_classes=None,
        num_proc=None,
        backend=None,
        store=None,
        loss=None,
        optimizer=None,
        metrics=None,
        loss_weights=None,
        sample_weight_col=None,
        feature_cols=None,
        input_shapes=None,
        validation=None,
        label_cols=None,
        callbacks=None,
        batch_size=None,
        val_batch_size=None,
        epochs=None,
        verbose=1,
        shuffle_buffer_size=None,
        partitions_per_process=10,
        run_id=None,
        train_minibatch_fn=None,
        train_steps_per_epoch=None,
        validation_steps_per_epoch=None,
        transformation_fn=None,
        train_reader_num_workers=None,
        val_reader_num_workers=None,
        reader_pool_type=None,
        label_shapes=None,
        inmemory_cache_all=False,
        num_gpus=None,
        logger=None,
        log_every_n_steps=50,
        data_module=None,
        loader_num_epochs=None,
        terminate_on_nan=False,
        profiler=None,
        # diff from horovod
        optimizer_name="adam",
        loss_name="cross_entropy",
        pretrained=True,
        feature_extracting=True,
        dropout_aux=0.7,
    ):
        super(DeepVisionClassifier, self).__init__()
        self._setDefault(
            num_layers_to_fine_tune=0,
            optimizer_name="adam",
            loss_name="cross_entropy",
            pretrained=True,
            feature_extracting=True,
            dropout_aux=0.7,
        )

        # override those keyword args
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

        self.model = LitDeepVisionModel(
            backbone=self.getBackbone(),
            num_layers_to_train=self.getNumLayersToFineTune(),
            num_classes=self.getNumClasses(),
            input_shapes=self.getInputShapes(),
            optimizer_name=self.getOptimizerName(),
            loss_name=self.getLossName(),
            label_cols=self.getLabelCols(),
            feature_cols=self.getFeatureCols(),
            loss_weights=self.getLossWeights(),
            pretrained=self.getPretrained(),
            feature_extracting=self.getFeatureExtracting(),
            dropout_aux=self.getDropoutAUX(),
        )

    def setBackbone(self, value):
        return self._set(backbone=value)

    def getBackbone(self):
        return self.getOrDefault(self.backbone)

    def setNumLayersToFineTune(self, value):
        return self._set(num_layers_to_fine_tune=value)

    def getNumLayersToFineTune(self):
        return self.getOrDefault(self.num_layers_to_fine_tune)

    def setNumClasses(self, value):
        return self._set(num_classes=value)

    def getNumClasses(self):
        return self.getOrDefault(self.num_classes)

    def setLossName(self, value):
        return self._set(loss_name=value)

    def getLossName(self):
        return self.getOrDefault(self.loss_name)

    def setOptimizerName(self, value):
        return self._set(optimizer_name=value)

    def getOptimizerName(self):
        return self.getOrDefault(self.optimizer_name)

    def setPretrained(self, value):
        return self._set(pretrained=value)

    def getPretrained(self):
        return self.getOrDefault(self.pretrained)

    def setFeatureExtracting(self, value):
        return self._set(feature_extracting=value)

    def getFeatureExtracting(self):
        return self.getOrDefault(self.feature_extracting)

    def setDropoutAUX(self, value):
        return self._set(dropout_aux=value)

    def getDropoutAUX(self):
        return self.getOrDefault(self.dropout_aux)

    def _fit(self, dataset):
        return super()._fit(dataset)
