# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

import torchvision.transforms as transforms
from horovod.spark.common.backend import SparkBackend
from PIL import Image
from pyspark import keyword_only
from pyspark.context import SparkContext
from pyspark.ml.param.shared import Param, Params
from synapse.ml.dl.LitDeepVisionModel import LitDeepVisionModel
from synapse.ml.dl.DeepVisionModel import DeepVisionModel
from horovod.spark.lightning import TorchEstimator
from pytorch_lightning.utilities import _module_available

_HOROVOD_AVAILABLE = _module_available("horovod")
if _HOROVOD_AVAILABLE:
    import horovod

    _HOROVOD_EQUAL_0_25_0 = horovod.__version__ == "0.25.0"
    if not _HOROVOD_EQUAL_0_25_0:
        raise RuntimeError(
            "horovod should be of version 0.25.0, found: {}".format(horovod.__version__)
        )
else:
    raise ModuleNotFoundError("module not found: horovod")


class DeepVisionClassifier(TorchEstimator):

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

    @keyword_only
    def __init__(
        self,
        backbone=None,
        additional_layers_to_train=0,
        num_classes=None,
        optimizer_name="adam",
        loss_name="cross_entropy",
        dropout_aux=0.7,
        transform_fn=None,
        # TorchEstimator args
        num_proc=None,
        backend=None,
        store=None,
        metrics=[],
        loss_weights=None,
        sample_weight_col=None,
        gradient_compression=None,
        feature_cols=["image"],
        input_shapes=None,
        validation=None,
        label_cols=["label"],
        callbacks=[],
        batch_size=32,
        val_batch_size=None,
        epochs=1,
        verbose=1,
        random_seed=None,
        shuffle_buffer_size=None,
        partitions_per_process=10,
        run_id=None,
        train_minibatch_fn=None,
        train_steps_per_epoch=None,
        validation_steps_per_epoch=None,
        transformation_fn=None,
        train_reader_num_workers=2,
        trainer_args=None,
        val_reader_num_workers=2,
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
        super(DeepVisionClassifier, self).__init__(
            num_proc=num_proc,
            backend=backend,
            store=store,
            metrics=metrics,
            loss_weights=loss_weights,
            sample_weight_col=sample_weight_col,
            gradient_compression=gradient_compression,
            feature_cols=feature_cols,
            input_shapes=input_shapes,
            validation=validation,
            label_cols=label_cols,
            callbacks=callbacks,
            batch_size=batch_size,
            val_batch_size=val_batch_size,
            epochs=epochs,
            verbose=verbose,
            random_seed=random_seed,
            shuffle_buffer_size=shuffle_buffer_size,
            partitions_per_process=partitions_per_process,
            run_id=run_id,
            train_minibatch_fn=train_minibatch_fn,
            train_steps_per_epoch=train_steps_per_epoch,
            validation_steps_per_epoch=validation_steps_per_epoch,
            transformation_fn=transformation_fn,
            train_reader_num_workers=train_reader_num_workers,
            trainer_args=trainer_args,
            val_reader_num_workers=val_reader_num_workers,
            reader_pool_type=reader_pool_type,
            label_shapes=label_shapes,
            inmemory_cache_all=inmemory_cache_all,
            num_gpus=num_gpus,
            logger=logger,
            log_every_n_steps=log_every_n_steps,
            data_module=data_module,
            loader_num_epochs=loader_num_epochs,
            terminate_on_nan=terminate_on_nan,
            profiler=profiler,
            debug_data_loader=debug_data_loader,
            train_async_data_loader_queue_size=train_async_data_loader_queue_size,
            val_async_data_loader_queue_size=val_async_data_loader_queue_size,
            use_gpu=use_gpu,
            mp_start_method=mp_start_method,
        )

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
        )

        self._set(backbone=backbone)
        self._set(additional_layers_to_train=additional_layers_to_train)
        self._set(num_classes=num_classes)
        self._set(optimizer_name=optimizer_name)
        self._set(loss_name=loss_name)
        self._set(dropout_aux=dropout_aux)
        self._set(transform_fn=transform_fn)

        self._update_input_shapes()
        self._update_transformation_fn()

        model = LitDeepVisionModel(
            backbone=self.getBackbone(),
            additional_layers_to_train=self.getAdditionalLayersToTrain(),
            num_classes=self.getNumClasses(),
            input_shape=self.getInputShapes()[0],
            optimizer_name=self.getOptimizerName(),
            loss_name=self.getLossName(),
            label_col=self.getLabelCols()[0],
            image_col=self.getFeatureCols()[0],
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
            ## Check if this is needed
            elif self.getBackbone() == "regnet_y_32gf":
                self.setInputShapes([[-1, 3, 384, 384]])
            else:
                self.setInputShapes([[-1, 3, 224, 224]])

    def _fit(self, dataset):
        return super()._fit(dataset)

    # override this method to provide a correct default backend
    def _get_or_create_backend(self):
        backend = self.getBackend()
        num_proc = self.getNumProc()
        if backend is None:
            if num_proc is None:
                num_proc = self._find_num_proc()
            backend = SparkBackend(
                num_proc,
                stdout=sys.stdout,
                stderr=sys.stderr,
                prefix_output_with_timestamp=True,
                verbose=self.getVerbose(),
            )
        elif num_proc is not None:
            raise ValueError(
                'At most one of parameters "backend" and "num_proc" may be specified'
            )
        return backend

    def _find_num_proc(self):
        if self.getUseGpu():
            # set it as number of executors for now (ignoring num_gpus per executor)
            sc = SparkContext.getOrCreate()
            return sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        return None

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

            def _create_transform_row(image_col, label_col, transform):
                def _transform_row(row):
                    path = row[image_col]
                    label = row[label_col]
                    image = Image.open(path).convert("RGB")
                    image = transform(image).numpy()
                    return {image_col: image, label_col: label}

                return _transform_row

            self.setTransformationFn(
                _create_transform_row(
                    self.getFeatureCols()[0],
                    self.getLabelCols()[0],
                    self.getTransformFn(),
                )
            )

    def get_model_class(self):
        return DeepVisionModel
