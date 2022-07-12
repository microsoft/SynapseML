# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from horovod.spark.lightning import TorchModel
from pyspark.ml.param.shared import Param, Params
from pyspark import keyword_only
import torchvision.transforms as transforms
from PIL import Image
import torch
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType
import numpy as np


class DeepVisionModel(TorchModel):

    transform_fn = Param(
        Params._dummy(),
        "transform_fn",
        "A composition of transforms used to transform and augnment the input image, should be of type torchvision.transforms.Compose",
    )

    @keyword_only
    def __init__(
        self,
        history=None,
        model=None,
        feature_columns=None,
        input_shapes=None,
        label_columns=None,
        optimizer=None,
        run_id=None,
        _metadata=None,
        loss=None,
        loss_constructors=None,
        # diff from horovod
        transform_fn=None,
    ):
        super(DeepVisionModel, self).__init__()

        self._setDefault(transform_fn=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

        self._update_transform_fn()

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

    # override this to open the image if it's a path
    def get_prediction_fn(self):
        input_shape = self.getInputShapes()[0]
        feature_col = self.getFeatureColumns()[0]

        def _create_predict_fn(transform):
            def predict_fn(model, row):
                if type(row[feature_col]) == str:
                    image = Image.open(row[feature_col]).convert("RGB")
                    data = transform(image).numpy().reshape(input_shape)
                else:
                    data = torch.tensor([row[feature_col]]).reshape(input_shape)

                with torch.no_grad():
                    pred = model(data)

                return pred

            return predict_fn

        return _create_predict_fn(self.getTransformFn())

    # pytorch_lightning module has its own optimizer configuration
    def getOptimizer(self):
        return None

    def _transform(self, df):
        output_df = super()._transform(df)
        argmax = udf(lambda v: float(np.argmax(v)), returnType=DoubleType())
        pred_df = output_df.withColumn(
            "prediction", argmax(col(self.getOutputCols()[0]))
        )
        return pred_df
