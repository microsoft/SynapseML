# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

import torchvision.transforms as transforms
from horovod.spark.common.backend import SparkBackend
from horovod.spark.lightning import TorchEstimator
from PIL import Image
from pyspark.context import SparkContext
from pyspark.ml.param.shared import Param, Params
from pytorch_lightning.utilities import _module_available
from synapse.ml.dl.DeepVisionModel import DeepVisionModel
from synapse.ml.dl.LitDeepVisionModel import LitDeepVisionModel
from synapse.ml.dl.utils import keywords_catch, get_or_create_backend
from synapse.ml.dl.PredictionParams import VisionPredictionParams

_HOROVOD_AVAILABLE = _module_available("horovod")
if _HOROVOD_AVAILABLE:
    import horovod

    _HOROVOD_EQUAL_0_28_1 = horovod.__version__ == "0.28.1"
    if not _HOROVOD_EQUAL_0_28_1:
        raise RuntimeError(
            "horovod should be of version 0.28.1, found: {}".format(horovod.__version__)
        )
else:
    raise ModuleNotFoundError("module not found: horovod")


class DeepVisionClassifier(TorchEstimator, VisionPredictionParams):
    backbone = Param(
        Params._dummy(), "backbone", "backbone of the deep vision classifier"
    )

    additional_layers_to_train = Param(
        Params._dummy(),
        "additional_layers_to_train",
        "number of last layers to fine tune for the model, should be between 0 and 3",
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

    dropout_aux = Param(
        Params._dummy(),
        "dropout_aux",
        "numeric value that's applied to googlenet InceptionAux module's dropout layer only: probability of an element to be zeroed",
    )

    transform_fn = Param(
        Params._dummy(),
        "transform_fn",
        "A composition of transforms used to transform and augnment the input image, should be of type torchvision.transforms.Compose",
    )

    @keywords_catch
    def __init__(
        self,
        backbone=None,
        additional_layers_to_train=0,
        num_classes=None,
        optimizer_name="adam",
        loss_name="cross_entropy",
        dropout_aux=0.7,
        transform_fn=None,
        # Classifier args
        label_col="label",
        image_col="image",
        prediction_col="prediction",
        # TorchEstimator args
        num_proc=None,
        backend=None,
        store=None,
        metrics=None,
        loss_weights=None,
        sample_weight_col=None,
        gradient_compression=None,
        input_shapes=None,
        validation=None,
        callbacks=None,
        batch_size=None,
        val_batch_size=None,
        epochs=None,
        verbose=1,
        random_seed=None,
        shuffle_buffer_size=None,
        partitions_per_process=None,
        run_id=None,
        train_minibatch_fn=None,
        train_steps_per_epoch=None,
        validation_steps_per_epoch=None,
        transformation_fn=None,
        train_reader_num_workers=None,
        trainer_args=None,
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
        debug_data_loader=False,
        train_async_data_loader_queue_size=None,
        val_async_data_loader_queue_size=None,
        use_gpu=True,
        mp_start_method=None,
    ):
        super(DeepVisionClassifier, self).__init__()

        self._setDefault(
            backbone=None,
            additional_layers_to_train=0,
            num_classes=None,
            optimizer_name="adam",
            loss_name="cross_entropy",
            dropout_aux=0.7,
            transform_fn=None,
            feature_cols=["image"],
            label_cols=["label"],
            label_col="label",
            image_col="image",
            prediction_col="prediction",
        )

        kwargs = self._kwargs
        self._set(**kwargs)

        self._update_input_shapes()
        self._update_cols()
        self._update_transformation_fn()

        model = LitDeepVisionModel(
            backbone=self.getBackbone(),
            additional_layers_to_train=self.getAdditionalLayersToTrain(),
            num_classes=self.getNumClasses(),
            input_shape=self.getInputShapes()[0],
            optimizer_name=self.getOptimizerName(),
            loss_name=self.getLossName(),
            label_col=self.getLabelCol(),
            image_col=self.getImageCol(),
            dropout_aux=self.getDropoutAUX(),
        )
        self._set(model=model)

    def setBackbone(self, value):
        return self._set(backbone=value)

    def getBackbone(self):
        return self.getOrDefault(self.backbone)

    def setAdditionalLayersToTrain(self, value):
        return self._set(additional_layers_to_train=value)

    def getAdditionalLayersToTrain(self):
        return self.getOrDefault(self.additional_layers_to_train)

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

    def setDropoutAUX(self, value):
        return self._set(dropout_aux=value)

    def getDropoutAUX(self):
        return self.getOrDefault(self.dropout_aux)

    def setTransformFn(self, value):
        return self._set(transform_fn=value)

    def getTransformFn(self):
        return self.getOrDefault(self.transform_fn)

    def _update_input_shapes(self):
        if self.getInputShapes() is None:
            if self.getBackbone().startswith("inception"):
                self.setInputShapes([[-1, 3, 299, 299]])
            else:
                self.setInputShapes([[-1, 3, 224, 224]])

    def _update_cols(self):
        self.setFeatureCols([self.getImageCol()])
        self.setLabelCols([self.getLabelCol()])

    def _fit(self, dataset):
        return super()._fit(dataset)

    # override this method to provide a correct default backend
    def _get_or_create_backend(self):
        return get_or_create_backend(
            self.getBackend(), self.getNumProc(), self.getVerbose(), self.getUseGpu()
        )

    def _update_transformation_fn(self):
        if self.getTransformationFn() is None:
            if self.getTransformFn() is None:
                crop_size = self.getInputShapes()[0][-1]
                transform = transforms.Compose(
                    [
                        transforms.RandomResizedCrop(crop_size),
                        transforms.RandomHorizontalFlip(),
                        transforms.ToTensor(),
                        transforms.Normalize(
                            mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                        ),
                    ]
                )
                self.setTransformFn(transform)

            image_col = self.getImageCol()
            label_col = self.getLabelCol()
            transform = self.getTransformFn()

            def _transform_row(row):
                path = row[image_col]
                label = row[label_col]
                image = Image.open(path).convert("RGB")
                image = transform(image).numpy()
                return {image_col: image, label_col: label}

            self.setTransformationFn(_transform_row)

    def get_model_class(self):
        return DeepVisionModel

    def _get_model_kwargs(self, model, history, optimizer, run_id, metadata):
        return dict(
            history=history,
            model=model,
            optimizer=optimizer,
            input_shapes=self.getInputShapes(),
            run_id=run_id,
            _metadata=metadata,
            loss=self.getLoss(),
            loss_constructors=self.getLossConstructors(),
            label_col=self.getLabelCol(),
            image_col=self.getImageCol(),
            prediction_col=self.getPredictionCol(),
        )
