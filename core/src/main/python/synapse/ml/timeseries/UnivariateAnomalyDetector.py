# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Spark-native univariate time series anomaly detection.

This module wraps the open source ``time-series-anomaly-detector`` package
(imported as ``anomaly_detector``), which open sources the algorithms that used
to back the retired Azure AI Anomaly Detector service.

The library itself is single node pandas code, so this transformer supplies the
distribution: series are partitioned with ``groupBy().applyInPandas()`` and each
one is detected independently on an executor.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

__all__ = ["UnivariateAnomalyDetector"]

MISSING_PACKAGE_MESSAGE = (
    "UnivariateAnomalyDetector requires the 'time-series-anomaly-detector' package, "
    "which is not installed. Install it with:\n"
    "    pip install time-series-anomaly-detector\n"
    "Note that the install name and the import name differ: the package installs as "
    "'time-series-anomaly-detector' but imports as 'anomaly_detector'."
)

DETECT_MODE_ENTIRE = "entire"
DETECT_MODE_LATEST = "latest"

# Field names are deliberately the camelCase names the retired Azure AI Anomaly
# Detector transformers emitted, so existing `col("anomalies.isAnomaly")` style
# expressions keep working after migrating.
#
# `severity` is intentionally absent: the retired service returned it, but the
# open source library does not compute it in either detect mode.
_RESULT_FIELDS: List[Tuple[str, str, T.DataType]] = [
    ("isAnomaly", "is_anomaly", T.BooleanType()),
    ("isPositiveAnomaly", "is_positive_anomaly", T.BooleanType()),
    ("isNegativeAnomaly", "is_negative_anomaly", T.BooleanType()),
    ("expectedValue", "expected_value", T.DoubleType()),
    ("upperMargin", "upper_margin", T.DoubleType()),
    ("lowerMargin", "lower_margin", T.DoubleType()),
    ("period", "period", T.IntegerType()),
]

_TEMP_PREFIX = "__synapseml_uvad_"
_ERROR_FIELD = _TEMP_PREFIX + "error"

# Parameters forwarded verbatim to the library. The names match the retired
# service so migrating callers keep their existing tuning values.
_PASSTHROUGH_PARAMS = [
    "granularity",
    "customInterval",
    "period",
    "maxAnomalyRatio",
    "sensitivity",
    "imputeMode",
    "imputeFixedValue",
]


def result_schema() -> T.StructType:
    """Return the struct schema emitted into ``outputCol``."""
    return T.StructType(
        [T.StructField(name, dtype, True) for name, _, dtype in _RESULT_FIELDS]
    )


def _default_detector_factory(detect_mode: str) -> Tuple[Any, Any]:
    """Build a library detector. Imported lazily so the dependency stays optional."""
    try:
        from anomaly_detector.univariate.univariate_anomaly_detection import (
            UnivariateAnomalyDetector as _LibraryDetector,
        )
        from anomaly_detector.univariate.util.fields import DetectType
    except ImportError as exc:  # pragma: no cover - only hit without the dependency
        raise ImportError(MISSING_PACKAGE_MESSAGE) from exc

    detect_type = (
        DetectType.LATEST if detect_mode == DETECT_MODE_LATEST else DetectType.ENTIRE
    )
    return _LibraryDetector(), detect_type


def _coerce(value: Any, dtype: T.DataType) -> Any:
    if value is None:
        return None
    if isinstance(dtype, T.BooleanType):
        return bool(value)
    if isinstance(dtype, T.IntegerType):
        return int(value)
    return float(value)


def detect_series(
    pdf,
    options: Dict[str, Any],
    detector_factory: Optional[Callable[[str], Tuple[Any, Any]]] = None,
):
    """Detect anomalies for a single, complete time series.

    Pure pandas with no Spark dependency, which keeps it directly unit testable.
    Any failure is captured per series so one bad group cannot fail the job.
    """
    import pandas as pd

    timestamp_col = options["timestampCol"]
    value_col = options["valueCol"]
    detect_mode = options["detectMode"]

    pdf = pdf.sort_values(timestamp_col, kind="mergesort").reset_index(drop=True)
    row_count = len(pdf)
    columns: Dict[str, List[Any]] = {
        _TEMP_PREFIX + name: [None] * row_count for name, _, _ in _RESULT_FIELDS
    }
    error: Optional[str] = None

    try:
        if row_count == 0:
            raise ValueError("the series is empty")

        factory = detector_factory or _default_detector_factory
        detector, detect_type = factory(detect_mode)

        params = dict(options["params"])
        params["detect_mode"] = detect_type
        series = pd.DataFrame(
            {
                "timestamp": pdf[timestamp_col].astype(str).tolist(),
                "value": pdf[value_col].astype(float).tolist(),
            }
        )
        response = detector.predict(None, series, params)
        results = [entry.get("result", entry) for entry in response]

        if detect_mode == DETECT_MODE_LATEST:
            # Only the most recent point is scored; earlier rows are the context
            # window the model needed in order to score it.
            results = [None] * (row_count - 1) + [results[-1]]
        elif len(results) != row_count:
            raise ValueError(
                "detector returned {0} results for {1} rows".format(
                    len(results), row_count
                )
            )

        for index, result in enumerate(results):
            if result is None:
                continue
            for name, source, dtype in _RESULT_FIELDS:
                columns[_TEMP_PREFIX + name][index] = _coerce(result.get(source), dtype)
    except Exception as exc:  # noqa: BLE001 - deliberately surfaced through errorCol
        error = "{0}: {1}".format(type(exc).__name__, exc)
        for name, _, _ in _RESULT_FIELDS:
            columns[_TEMP_PREFIX + name] = [None] * row_count

    for column, values in columns.items():
        pdf[column] = values
    pdf[_ERROR_FIELD] = [error] * row_count
    return pdf


class UnivariateAnomalyDetector(
    Transformer, DefaultParamsReadable, DefaultParamsWritable
):
    """Detect anomalies in univariate time series, one series per group.

    Replaces the retired ``DetectAnomalies``, ``DetectLastAnomaly`` and
    ``SimpleDetectAnomalies`` transformers. Parameter names and output field
    names are unchanged from those transformers, so migrating is normally just a
    change of import and constructor.

    Requires the optional ``time-series-anomaly-detector`` package.
    """

    timestampCol = Param(
        Params._dummy(),
        "timestampCol",
        "Name of the column holding the timestamp of each point",
        typeConverter=TypeConverters.toString,
    )

    valueCol = Param(
        Params._dummy(),
        "valueCol",
        "Name of the column holding the measured value of each point",
        typeConverter=TypeConverters.toString,
    )

    groupByCols = Param(
        Params._dummy(),
        "groupByCols",
        "Columns identifying an individual series. Each group is detected "
        "independently and in parallel. When empty the whole DataFrame is "
        "treated as a single series",
        typeConverter=TypeConverters.toListString,
    )

    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "Name of the output struct column holding the detection result",
        typeConverter=TypeConverters.toString,
    )

    errorCol = Param(
        Params._dummy(),
        "errorCol",
        "Name of the output column holding the error message for series that "
        "could not be scored. Null when detection succeeded",
        typeConverter=TypeConverters.toString,
    )

    detectMode = Param(
        Params._dummy(),
        "detectMode",
        "Either 'entire' to score every point, or 'latest' to score only the "
        "most recent point of each series",
        typeConverter=TypeConverters.toString,
    )

    granularity = Param(
        Params._dummy(),
        "granularity",
        "Sampling rate of the series: yearly, monthly, weekly, daily, hourly, "
        "minutely, secondly, microsecond or none",
        typeConverter=TypeConverters.toString,
    )

    customInterval = Param(
        Params._dummy(),
        "customInterval",
        "Custom interval used together with granularity, for example 5 with a "
        "granularity of minutely for a five minute series",
        typeConverter=TypeConverters.toInt,
    )

    period = Param(
        Params._dummy(),
        "period",
        "Known period of the series. Omit to have it inferred",
        typeConverter=TypeConverters.toInt,
    )

    maxAnomalyRatio = Param(
        Params._dummy(),
        "maxAnomalyRatio",
        "Maximum proportion of points that may be flagged, in (0, 0.49]",
        typeConverter=TypeConverters.toFloat,
    )

    sensitivity = Param(
        Params._dummy(),
        "sensitivity",
        "Detection sensitivity from 0 to 99. Lower values widen the margins "
        "and flag fewer points",
        typeConverter=TypeConverters.toInt,
    )

    imputeMode = Param(
        Params._dummy(),
        "imputeMode",
        "How to fill missing points: auto, previous, linear, fixed, zero or notFill",
        typeConverter=TypeConverters.toString,
    )

    imputeFixedValue = Param(
        Params._dummy(),
        "imputeFixedValue",
        "Value used to fill gaps when imputeMode is 'fixed'",
        typeConverter=TypeConverters.toFloat,
    )

    @keyword_only
    def __init__(
        self,
        timestampCol="timestamp",
        valueCol="value",
        groupByCols=None,
        outputCol="anomalies",
        errorCol="error",
        detectMode=DETECT_MODE_ENTIRE,
        granularity=None,
        customInterval=None,
        period=None,
        maxAnomalyRatio=None,
        sensitivity=None,
        imputeMode=None,
        imputeFixedValue=None,
    ):
        super(UnivariateAnomalyDetector, self).__init__()
        self._setDefault(
            timestampCol="timestamp",
            valueCol="value",
            groupByCols=[],
            outputCol="anomalies",
            errorCol="error",
            detectMode=DETECT_MODE_ENTIRE,
        )
        kwargs = self._input_kwargs
        self.setParams(**{k: v for k, v in kwargs.items() if v is not None})

    @keyword_only
    def setParams(self, **kwargs):
        return self._set(**kwargs)

    def setTimestampCol(self, value):
        return self._set(timestampCol=value)

    def getTimestampCol(self):
        return self.getOrDefault(self.timestampCol)

    def setValueCol(self, value):
        return self._set(valueCol=value)

    def getValueCol(self):
        return self.getOrDefault(self.valueCol)

    def setGroupByCols(self, value):
        return self._set(groupByCols=value)

    def getGroupByCols(self):
        return self.getOrDefault(self.groupByCols)

    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)

    def setErrorCol(self, value):
        return self._set(errorCol=value)

    def getErrorCol(self):
        return self.getOrDefault(self.errorCol)

    def setDetectMode(self, value):
        return self._set(detectMode=value)

    def getDetectMode(self):
        return self.getOrDefault(self.detectMode)

    def setGranularity(self, value):
        return self._set(granularity=value)

    def getGranularity(self):
        return self.getOrDefault(self.granularity)

    def setCustomInterval(self, value):
        return self._set(customInterval=value)

    def setPeriod(self, value):
        return self._set(period=value)

    def setMaxAnomalyRatio(self, value):
        return self._set(maxAnomalyRatio=value)

    def setSensitivity(self, value):
        return self._set(sensitivity=value)

    def setImputeMode(self, value):
        return self._set(imputeMode=value)

    def setImputeFixedValue(self, value):
        return self._set(imputeFixedValue=value)

    def _detector_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for name in _PASSTHROUGH_PARAMS:
            param = getattr(self, name)
            if self.isSet(param) or self.hasDefault(param):
                value = self.getOrDefault(param)
                if value is not None:
                    params[name] = value
        return params

    def _options(self) -> Dict[str, Any]:
        return {
            "timestampCol": self.getTimestampCol(),
            "valueCol": self.getValueCol(),
            "detectMode": self.getDetectMode(),
            "params": self._detector_params(),
        }

    def _validate(self, schema: T.StructType) -> None:
        detect_mode = self.getDetectMode()
        if detect_mode not in (DETECT_MODE_ENTIRE, DETECT_MODE_LATEST):
            raise ValueError(
                "detectMode must be '{0}' or '{1}', got '{2}'".format(
                    DETECT_MODE_ENTIRE, DETECT_MODE_LATEST, detect_mode
                )
            )
        known = set(schema.fieldNames())
        for column in [self.getTimestampCol(), self.getValueCol()] + list(
            self.getGroupByCols()
        ):
            if column not in known:
                raise ValueError(
                    "column '{0}' is not present in the input DataFrame".format(column)
                )

    def transformSchema(self, schema: T.StructType) -> T.StructType:
        self._validate(schema)
        return T.StructType(
            list(schema.fields)
            + [
                T.StructField(self.getOutputCol(), result_schema(), True),
                T.StructField(self.getErrorCol(), T.StringType(), True),
            ]
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self._validate(dataset.schema)

        options = self._options()
        output_col = self.getOutputCol()
        error_col = self.getErrorCol()
        group_by_cols = list(self.getGroupByCols())

        working = dataset
        # applyInPandas always needs a grouping key. With no user supplied key the
        # whole DataFrame is a single series, which cannot be parallelized.
        synthetic_key = None
        if not group_by_cols:
            synthetic_key = _TEMP_PREFIX + "group"
            working = working.withColumn(synthetic_key, F.lit(0))
            group_by_cols = [synthetic_key]

        udf_schema = T.StructType(
            list(working.schema.fields)
            + [
                T.StructField(_TEMP_PREFIX + name, dtype, True)
                for name, _, dtype in _RESULT_FIELDS
            ]
            + [T.StructField(_ERROR_FIELD, T.StringType(), True)]
        )
        ordered = [field.name for field in udf_schema.fields]

        def _apply(pdf):
            return detect_series(pdf, options)[ordered]

        detected = working.groupBy(*group_by_cols).applyInPandas(
            _apply, schema=udf_schema
        )

        error_column = F.col(_ERROR_FIELD)
        struct_column = F.struct(
            *[F.col(_TEMP_PREFIX + name).alias(name) for name, _, _ in _RESULT_FIELDS]
        )
        detected = detected.withColumn(
            output_col,
            F.when(error_column.isNull(), struct_column).otherwise(
                F.lit(None).cast(result_schema())
            ),
        ).withColumn(error_col, error_column)

        drop_cols = [_TEMP_PREFIX + name for name, _, _ in _RESULT_FIELDS]
        drop_cols.append(_ERROR_FIELD)
        if synthetic_key is not None:
            drop_cols.append(synthetic_key)
        return detected.drop(*drop_cols)
