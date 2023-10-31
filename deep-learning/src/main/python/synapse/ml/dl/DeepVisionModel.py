# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import numpy as np
import torch
import torchvision.transforms as transforms
from horovod.spark.lightning import TorchModel
from PIL import Image
from synapse.ml.dl.PredictionParams import VisionPredictionParams
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from synapse.ml.dl.utils import keywords_catch


class DeepVisionModel(TorchModel, VisionPredictionParams):
    transform_fn = Param(
        Params._dummy(),
        "transform_fn",
        "A composition of transforms used to transform and augnment the input image, should be of type torchvision.transforms.Compose",
    )

    @keywords_catch
    def __init__(
        self,
        history=None,
        model=None,
        input_shapes=None,
        optimizer=None,
        run_id=None,
        _metadata=None,
        loss=None,
        loss_constructors=None,
        # diff from horovod
        transform_fn=None,
        label_col="label",
        image_col="image",
        prediction_col="prediction",
    ):
        super(DeepVisionModel, self).__init__()

        self._setDefault(
            optimizer=None,
            loss=None,
            loss_constructors=None,
            input_shapes=None,
            transform_fn=None,
            label_col="label",
            image_col="image",
            prediction_col="prediction",
            feature_columns=["image"],
            label_columns=["label"],
            outputCols=["output"],
        )

        kwargs = self._kwargs
        self._set(**kwargs)

    def setTransformFn(self, value):
        return self._set(transform_fn=value)

    def getTransformFn(self):
        return self.getOrDefault(self.transform_fn)

    def setTransformationFn(self, value):
        return self._set(transformation_fn=value)

    def getTransformationFn(self):
        return self.getOrDefault(self.transformation_fn)

    def _update_transform_fn(self):
        if self.getTransformFn() is None:
            crop_size = self.getInputShapes()[0][-1]
            transform = transforms.Compose(
                [
                    transforms.CenterCrop(crop_size),
                    transforms.ToTensor(),
                    transforms.Normalize(
                        mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                    ),
                ]
            )
            self.setTransformFn(transform)

    def _update_cols(self):
        self.setFeatureColumns([self.getImageCol()])
        self.setLabelColoumns([self.getLabelCol()])

    # override this to open the image if it's a path
    def get_prediction_fn(self):
        input_shape = self.getInputShapes()[0]
        image_col = self.getImageCol()
        transform = self.getTransformFn()

        def predict_fn(model, row):
            if type(row[image_col]) == str:
                image = Image.open(row[image_col]).convert("RGB")
                data = torch.tensor(transform(image).numpy()).reshape(input_shape)
            else:
                data = torch.tensor([row[image_col]]).reshape(input_shape)

            with torch.no_grad():
                pred = model(data)

            return pred

        return predict_fn

    # pytorch_lightning module has its own optimizer configuration
    def getOptimizer(self):
        return None

    def _transform(self, df):
        self._update_transform_fn()
        self._update_cols()
        output_df = super()._transform(df)
        argmax = udf(lambda v: float(np.argmax(v)), returnType=DoubleType())
        pred_df = output_df.withColumn(
            self.getPredictionCol(), argmax(col(self.getOutputCols()[0]))
        )
        return pred_df
