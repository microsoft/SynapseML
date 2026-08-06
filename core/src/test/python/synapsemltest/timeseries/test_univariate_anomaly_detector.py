# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

import pandas as pd
import pytest

from synapse.ml.core.init_spark import *
from synapse.ml.timeseries.UnivariateAnomalyDetector import (
    DETECT_MODE_ENTIRE,
    DETECT_MODE_LATEST,
    UnivariateAnomalyDetector,
    detect_series,
    result_schema,
)

spark = init_spark()


def _frame(rows=6, spike_at=None):
    values = [10.0 + i for i in range(rows)]
    if spike_at is not None:
        values[spike_at] = 500.0
    return pd.DataFrame(
        {
            "timestamp": [
                "2024-01-01T{0:02d}:00:00Z".format(hour) for hour in range(rows)
            ],
            "value": values,
        }
    )


def _options(detect_mode=DETECT_MODE_ENTIRE, params=None):
    return {
        "timestampCol": "timestamp",
        "valueCol": "value",
        "detectMode": detect_mode,
        "params": {"granularity": "hourly"} if params is None else params,
    }


class FakeDetector:
    """Stands in for the optional library so the plumbing is testable in CI."""

    def __init__(self, flag_indices=(), recorder=None):
        self.flag_indices = set(flag_indices)
        self.recorder = recorder

    def predict(self, context, data, params):
        if self.recorder is not None:
            self.recorder.append(dict(params))
        return [
            {
                "result": {
                    "is_anomaly": index in self.flag_indices,
                    "is_positive_anomaly": index in self.flag_indices,
                    "is_negative_anomaly": False,
                    "expected_value": float(index),
                    "upper_margin": 1.0,
                    "lower_margin": 2.0,
                    "period": 3,
                }
            }
            for index in range(len(data))
        ]


def _factory(detector):
    return lambda detect_mode: (detector, detect_mode)


class TestDetectSeries(unittest.TestCase):
    def test_entire_mode_maps_every_row(self):
        result = detect_series(
            _frame(), _options(), _factory(FakeDetector(flag_indices=[2]))
        )
        self.assertEqual(len(result), 6)
        flags = result["__synapseml_uvad_isAnomaly"].tolist()
        self.assertEqual(flags, [False, False, True, False, False, False])
        self.assertEqual(result["__synapseml_uvad_expectedValue"].tolist()[2], 2.0)
        self.assertEqual(result["__synapseml_uvad_period"].tolist()[0], 3)
        self.assertTrue(result["__synapseml_uvad_error"].isnull().all())

    def test_latest_mode_scores_only_the_final_row(self):
        # The library returns a single result in latest mode.
        detector = FakeDetector(flag_indices=[0])
        detector.predict = lambda context, data, params: [
            {
                "result": {
                    "is_anomaly": True,
                    "is_positive_anomaly": True,
                    "is_negative_anomaly": False,
                    "expected_value": 42.0,
                    "upper_margin": 1.0,
                    "lower_margin": 2.0,
                    "period": 0,
                }
            }
        ]
        result = detect_series(
            _frame(), _options(DETECT_MODE_LATEST), _factory(detector)
        )
        flags = result["__synapseml_uvad_isAnomaly"].tolist()
        self.assertEqual(flags, [None, None, None, None, None, True])
        self.assertTrue(result["__synapseml_uvad_error"].isnull().all())

    def test_rows_are_sorted_by_timestamp_before_detection(self):
        shuffled = _frame().iloc[[4, 0, 5, 1, 3, 2]].reset_index(drop=True)
        result = detect_series(
            shuffled, _options(), _factory(FakeDetector(flag_indices=[0]))
        )
        self.assertEqual(result["timestamp"].tolist(), _frame()["timestamp"].tolist())
        # Index 0 of the sorted series must be the flagged one.
        self.assertTrue(result["__synapseml_uvad_isAnomaly"].tolist()[0])

    def test_detector_failure_is_isolated_to_the_series(self):
        def exploding(detect_mode):
            raise RuntimeError("boom")

        result = detect_series(_frame(), _options(), exploding)
        self.assertEqual(len(result), 6)
        self.assertTrue(result["__synapseml_uvad_isAnomaly"].isnull().all())
        self.assertTrue(
            all(
                error == "RuntimeError: boom"
                for error in result["__synapseml_uvad_error"]
            )
        )

    def test_empty_series_reports_an_error_rather_than_raising(self):
        empty = _frame().iloc[0:0]
        result = detect_series(empty, _options(), _factory(FakeDetector()))
        self.assertEqual(len(result), 0)

    def test_result_count_mismatch_is_reported(self):
        detector = FakeDetector()
        detector.predict = lambda context, data, params: [
            {"result": {"is_anomaly": False}}
        ]
        result = detect_series(_frame(), _options(), _factory(detector))
        self.assertTrue(
            all(
                "returned 1 results for 6 rows" in e
                for e in result["__synapseml_uvad_error"]
            )
        )

    def test_caller_params_are_not_mutated(self):
        # The library mutates the params dict it is handed, which makes a shared
        # dict single use. detect_series must copy it before every call.
        recorder = []
        options = _options()
        original = dict(options["params"])
        for _ in range(3):
            detect_series(_frame(), options, _factory(FakeDetector(recorder=recorder)))
        self.assertEqual(options["params"], original)
        self.assertEqual(len(recorder), 3)
        for seen in recorder:
            self.assertEqual(seen["granularity"], "hourly")


class TestTransformerContract(unittest.TestCase):
    def _df(self):
        return spark.createDataFrame(
            [("a", "2024-01-01T00:00:00Z", 1.0)],
            "group string, timestamp string, value double",
        )

    def test_transform_schema_appends_output_and_error(self):
        detector = UnivariateAnomalyDetector().setGroupByCols(["group"])
        schema = detector.transformSchema(self._df().schema)
        self.assertIn("anomalies", schema.fieldNames())
        self.assertIn("error", schema.fieldNames())
        self.assertEqual(schema["anomalies"].dataType, result_schema())

    def test_output_struct_preserves_legacy_field_names(self):
        # These are the field names the retired transformers emitted.
        self.assertEqual(
            result_schema().fieldNames(),
            [
                "isAnomaly",
                "isPositiveAnomaly",
                "isNegativeAnomaly",
                "expectedValue",
                "upperMargin",
                "lowerMargin",
                "period",
            ],
        )

    def test_missing_column_is_rejected(self):
        detector = UnivariateAnomalyDetector().setValueCol("absent")
        with self.assertRaises(ValueError):
            detector.transformSchema(self._df().schema)

    def test_invalid_detect_mode_is_rejected(self):
        detector = UnivariateAnomalyDetector().setDetectMode("sometimes")
        with self.assertRaises(ValueError):
            detector.transformSchema(self._df().schema)

    def test_parameters_round_trip(self):
        detector = (
            UnivariateAnomalyDetector()
            .setGranularity("hourly")
            .setSensitivity(90)
            .setMaxAnomalyRatio(0.25)
            .setImputeMode("fixed")
            .setImputeFixedValue(0.0)
            .setCustomInterval(5)
            .setPeriod(24)
        )
        params = detector._detector_params()
        self.assertEqual(params["granularity"], "hourly")
        self.assertEqual(params["sensitivity"], 90)
        self.assertEqual(params["maxAnomalyRatio"], 0.25)
        self.assertEqual(params["imputeMode"], "fixed")
        self.assertEqual(params["customInterval"], 5)
        self.assertEqual(params["period"], 24)

    def test_unset_parameters_are_not_forwarded(self):
        params = UnivariateAnomalyDetector().setGranularity("daily")._detector_params()
        self.assertEqual(list(params.keys()), ["granularity"])


class TestEndToEnd(unittest.TestCase):
    """Runs only where the optional library is installed."""

    def setUp(self):
        pytest.importorskip(
            "anomaly_detector",
            reason="requires the optional time-series-anomaly-detector package",
        )

    def _multi_series(self, series_count=4, points=100):
        rows = []
        for index in range(series_count):
            timestamps = pd.date_range("2024-01-01", periods=points, freq="h")
            for position, timestamp in enumerate(timestamps):
                value = 50.0 + (position % 12)
                if position == 40 + index:
                    value = 500.0
                rows.append(
                    (
                        "series_{0}".format(index),
                        timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        float(value),
                    )
                )
        return spark.createDataFrame(
            rows, "group string, timestamp string, value double"
        )

    def test_detects_planted_spike_in_every_series(self):
        detector = (
            UnivariateAnomalyDetector()
            .setGroupByCols(["group"])
            .setGranularity("hourly")
            .setSensitivity(95)
        )
        result = detector.transform(self._multi_series()).cache()
        self.assertEqual(result.count(), 400)
        self.assertEqual(result.filter("error is not null").count(), 0)
        flagged = result.filter("anomalies.isAnomaly").select("group").distinct()
        self.assertEqual(flagged.count(), 4)

    def test_bad_series_does_not_fail_the_job(self):
        good = self._multi_series(series_count=1)
        bad = spark.createDataFrame(
            [("broken", "2024-01-01T00:00:00Z", 1.0)],
            "group string, timestamp string, value double",
        )
        detector = (
            UnivariateAnomalyDetector()
            .setGroupByCols(["group"])
            .setGranularity("hourly")
        )
        result = detector.transform(good.union(bad)).cache()
        self.assertEqual(
            result.filter("group = 'broken' and error is not null").count(), 1
        )
        self.assertEqual(
            result.filter("group = 'series_0' and error is null").count(), 100
        )


if __name__ == "__main__":
    unittest.main()
