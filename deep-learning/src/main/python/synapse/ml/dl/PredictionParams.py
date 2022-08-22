# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.param import Param, Params, TypeConverters


class PredictionParams(Params):

    label_col = Param(
        Params._dummy(),
        "label_col",
        "label column name.",
        typeConverter=TypeConverters.toString,
    )

    image_col = Param(
        Params._dummy(),
        "image_col",
        "image column name.",
        typeConverter=TypeConverters.toString,
    )

    prediction_col = Param(
        Params._dummy(),
        "prediction_col",
        "prediction column name.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super(PredictionParams, self).__init__()
        self._setDefault(
            label_col="label", image_col="image", prediction_col="prediction"
        )

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`label_col`.
        """
        return self._set(label_col=value)

    def getLabelCol(self):
        """
        Gets the value of label_col or its default value.
        """
        return self.getOrDefault(self.label_col)

    def setImageCol(self, value):
        """
        Sets the value of :py:attr:`image_col`.
        """
        return self._set(image_col=value)

    def getImageCol(self):
        """
        Gets the value of image_col or its default value.
        """
        return self.getOrDefault(self.image_col)

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`prediction_col`.
        """
        return self._set(prediction_col=value)

    def getPredictionCol(self):
        """
        Gets the value of prediction_col or its default value.
        """
        return self.getOrDefault(self.prediction_col)
