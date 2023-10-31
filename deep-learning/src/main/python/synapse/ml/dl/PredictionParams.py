# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml.param import Param, Params, TypeConverters


class HasLabelColParam(Params):
    label_col = Param(
        Params._dummy(),
        "label_col",
        "label column name.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super(HasLabelColParam, self).__init__()
        self._setDefault(label_col="label")

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


class HasImageColParam(Params):
    image_col = Param(
        Params._dummy(),
        "image_col",
        "image column name.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super(HasImageColParam, self).__init__()
        self._setDefault(image_col="image")

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


## TODO: Potentially generalize to support multiple text columns as input
class HasTextColParam(Params):
    text_col = Param(
        Params._dummy(),
        "text_col",
        "text column name.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super(HasTextColParam, self).__init__()
        self._setDefault(text_col="text")

    def setTextCol(self, value):
        """
        Sets the value of :py:attr:`text_col`.
        """
        return self._set(text_col=value)

    def getTextCol(self):
        """
        Gets the value of text_col or its default value.
        """
        return self.getOrDefault(self.text_col)


class HasPredictionColParam(Params):
    prediction_col = Param(
        Params._dummy(),
        "prediction_col",
        "prediction column name.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self):
        super(HasPredictionColParam, self).__init__()
        self._setDefault(prediction_col="prediction")

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


class VisionPredictionParams(HasLabelColParam, HasImageColParam, HasPredictionColParam):
    def __init__(self):
        super(VisionPredictionParams, self).__init__()


class TextPredictionParams(HasLabelColParam, HasTextColParam, HasPredictionColParam):
    def __init__(self):
        super(TextPredictionParams, self).__init__()
