# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Real Spark fits, not generated constructor-only coverage.

The isolated installed-wheel validation sets SYNAPSEML_DML_ARTIFACT_MANIFEST to
its exact branch JAR/Python-module hash manifest (the existing installed-guard
schema: jars, core_jar, python_modules, plus installed_python_root).
Ordinary upstream Python CI can use its
already configured Spark installation. The former is required for the separately
reported installed-artifact claim; absence never produces a provenance PASS.
"""

import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import tempfile
import unittest

from pyspark import SparkContext
from pyspark.ml.regression import GeneralizedLinearRegression, LinearRegression
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from synapse.ml.causal.DoubleMLEstimator import DoubleMLEstimator
from synapse.ml.causal.DoubleMLModel import DoubleMLModel


class DoubleMLMultipleTreatmentsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        manifest_path = os.environ.get("SYNAPSEML_DML_ARTIFACT_MANIFEST")
        manifest = (
            json.loads(Path(manifest_path).read_text()) if manifest_path else None
        )
        cls.owns_spark_context = SparkContext._active_spark_context is None
        if manifest is not None and not cls.owns_spark_context:
            raise AssertionError("Installed validation requires a fresh Spark process")
        builder = SparkSession.builder.master("local[2]").appName(
            "double-ml-multiple-treatments"
        )
        if manifest is not None:
            if not manifest["jars"] or not manifest["python_modules"]:
                raise AssertionError("Empty branch artifact manifest")
            for row in manifest["jars"]:
                actual = hashlib.sha256(Path(row["path"]).read_bytes()).hexdigest()
                if actual != row["sha256"]:
                    raise AssertionError("Branch JAR identity changed")
            verified_jars = [Path(row["path"]).resolve() for row in manifest["jars"]]
            if verified_jars.count(Path(manifest["core_jar"]).resolve()) != 1:
                raise AssertionError("core_jar must be one unique hashed branch JAR")
            builder = builder.config(
                "spark.jars", ",".join(row["path"] for row in manifest["jars"])
            )
        cls.spark = builder.getOrCreate()
        cls.installed_artifact_provenance_verified = False
        if manifest is not None:
            for name, expected in manifest["python_modules"].items():
                module = importlib.import_module(name)
                module_path = Path(module.__file__).resolve()
                installed_root = Path(manifest["installed_python_root"]).resolve()
                if not module_path.is_relative_to(installed_root):
                    raise AssertionError(
                        "Python module is shadowed outside installed wheel"
                    )
                actual = hashlib.sha256(module_path.read_bytes()).hexdigest()
                if actual != expected:
                    raise AssertionError(
                        "Installed branch Python module mismatch: " + name
                    )
            jvm = cls.spark._jvm
            loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
            for class_name in ["DoubleMLModel", "DoubleMLEstimator"]:
                loaded_class = jvm.java.lang.Class.forName(
                    "com.microsoft.azure.synapse.ml.causal." + class_name, True, loader
                )
                origin = Path(
                    loaded_class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI()
                    .getPath()
                )
                if origin.resolve() != Path(manifest["core_jar"]).resolve():
                    raise AssertionError(class_name + " loaded from a different JAR")
            required_modules = {
                "synapse.ml.causal.DoubleMLEstimator",
                "synapse.ml.causal.DoubleMLModel",
                "synapse.ml.causal._DoubleMLModel",
            }
            if not required_modules.issubset(manifest["python_modules"]):
                raise AssertionError(
                    "DML Python identities absent from branch manifest"
                )
            cls.installed_artifact_provenance_verified = True
        rows = []
        for i in range(192):
            x = math.sin(i * 0.113)
            group = "east" if i % 2 == 0 else "west"
            t0 = math.sin((i + 1) * 0.37) + math.cos(i * 0.38) + 0.15 * x
            t1 = math.sin((i + 1) * 0.74) + math.cos(i * 0.57) + 0.15 * x
            y = (
                t0
                + 2 * t1
                + 0.4 * x
                + (0.7 if group == "east" else -0.7)
                + 0.02 * math.sin(i * 0.73)
            )
            rows.append((i, x, group, t0, t1, y))
        cls.data = cls.spark.createDataFrame(
            rows, ["rowId", "x", "group", "t0", "t1", "y"]
        )
        cls.plural = cls.estimator().setMaxIter(3).fit(cls.data)
        cls.singular = (
            DoubleMLEstimator()
            .setTreatmentCol("t0")
            .setOutcomeCol("y")
            .setTreatmentModel(LinearRegression().setRegParam(0.0))
            .setOutcomeModel(LinearRegression().setRegParam(0.0))
            .setMaxIter(3)
            .fit(cls.data)
        )
        print(
            json.dumps(
                {
                    "installed_artifact_provenance_verified": (
                        cls.installed_artifact_provenance_verified
                    )
                }
            )
        )

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "owns_spark_context", False) and hasattr(cls, "spark"):
            cls.spark.stop()

    @staticmethod
    def estimator():
        return (
            DoubleMLEstimator()
            .setTreatmentCols(["t0", "t1"])
            .setOutcomeCol("y")
            .setTreatmentModel(LinearRegression().setRegParam(0.0).setTol(1e-10))
            .setOutcomeModel(LinearRegression().setRegParam(0.0).setTol(1e-10))
        )

    def assert_close(self, actual, expected, tolerance=1e-6):
        self.assertEqual(len(actual), len(expected))
        for left, right in zip(actual, expected):
            self.assertTrue(math.isfinite(left))
            self.assertLessEqual(abs(left - right), tolerance * max(1.0, abs(right)))

    def assert_statistics_equal(self, actual, expected):
        self.assertEqual(
            actual.getFittedTreatmentCols(), expected.getFittedTreatmentCols()
        )
        self.assertEqual(
            actual.getRawTreatmentEffectCount(), expected.getRawTreatmentEffectCount()
        )
        self.assert_close(
            actual.getRawTreatmentEffectsByIteration(),
            expected.getRawTreatmentEffectsByIteration(),
        )
        self.assert_close(
            actual.getAvgTreatmentEffects(), expected.getAvgTreatmentEffects()
        )
        for left, right in zip(actual.getPValues(), expected.getPValues()):
            if math.isnan(right):
                self.assertTrue(math.isnan(left))
            else:
                self.assertAlmostEqual(left, right)
        for left, right in zip(
            actual.getConfidenceIntervals(), expected.getConfidenceIntervals()
        ):
            self.assert_close(left, right)
        self.assertEqual(
            actual.getRawTreatmentEffectRows(), expected.getRawTreatmentEffectRows()
        )

    def test_real_plural_fit_and_all_getter_shapes(self):
        model = self.plural
        self.assertEqual(model.getFittedTreatmentCols(), ["t0", "t1"])
        self.assertEqual(model.getRawTreatmentEffectCount(), 2)
        self.assert_close(model.getAvgTreatmentEffects(), [1.0, 2.0], 0.03)
        self.assertEqual(len(model.getRawTreatmentEffectsByIteration()), 6)
        self.assertEqual(len(model.getRawTreatmentEffectRows()), 3)
        self.assertEqual(
            [len(row) for row in model.getRawTreatmentEffectRows()], [2, 2, 2]
        )
        self.assertEqual(len(model.getPValues()), 2)
        for values, actual in zip(
            zip(*model.getRawTreatmentEffectRows()), model.getPValues()
        ):
            mean = sum(values) / len(values)
            sd = math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))
            if sd == 0:
                self.assertTrue(math.isnan(actual))
            else:
                expected = math.erfc(abs(mean / sd) / math.sqrt(2))
                self.assertAlmostEqual(actual, expected, places=9)
        self.assertEqual([len(row) for row in model.getConfidenceIntervals()], [2, 2])

    def test_python_lists_and_nested_rows_are_defensive_copies(self):
        flat = self.plural.getRawTreatmentEffectsByIteration()
        expected = list(flat)
        flat[0] = 999.0
        rows = self.plural.getRawTreatmentEffectRows()
        rows[0][0] = -999.0
        names = self.plural.getFittedTreatmentCols()
        names[0] = "wrong"
        intervals = self.plural.getConfidenceIntervals()
        intervals[0][0] = -999.0
        self.assertEqual(self.plural.getRawTreatmentEffectsByIteration(), expected)
        self.assertEqual(self.plural.getFittedTreatmentCols(), ["t0", "t1"])
        self.assertNotEqual(self.plural.getConfidenceIntervals()[0][0], -999.0)

    def test_copy_and_reload_preserve_actual_fitted_causal_statistics(self):
        copied = self.plural.copy({})
        self.assert_statistics_equal(copied, self.plural)
        with tempfile.TemporaryDirectory(prefix="double-ml-plural-") as directory:
            path = str(Path(directory) / "model")
            self.plural.write().overwrite().save(path)
            loaded = DoubleMLModel.load(path)
            self.assert_statistics_equal(loaded, self.plural)
            self.assertEqual(loaded.transform(self.data).schema, self.data.schema)
        with tempfile.TemporaryDirectory(prefix="double-ml-singular-") as directory:
            path = str(Path(directory) / "model")
            self.singular.write().overwrite().save(path)
            loaded = DoubleMLModel.load(path)
            self.assertAlmostEqual(
                loaded.getAvgTreatmentEffect(), self.singular.getAvgTreatmentEffect()
            )
            self.assertEqual(
                loaded.getConfidenceInterval(), self.singular.getConfidenceInterval()
            )

    def test_singular_fit_and_getters_remain_available(self):
        model = self.singular
        self.assertTrue(math.isfinite(model.getAvgTreatmentEffect()))
        self.assertEqual(len(model.getConfidenceInterval()), 2)
        self.assertEqual(len(model.getRawTreatmentEffects()), 3)
        self.assertIsInstance(model.getPValue(), float)
        self.assertAlmostEqual(
            model.copy({}).getAvgTreatmentEffect(), model.getAvgTreatmentEffect()
        )

    def test_python_setter_does_not_change_jvm_order_until_transfer(self):
        model = self.plural.copy({})
        model.setTreatmentCols(["renamed", "t1"])
        self.assertEqual(model.getFittedTreatmentCols(), ["t0", "t1"])
        model._transfer_params_to_java()
        with self.assertRaisesRegex(
            Exception,
            "Plural treatment statistics do not match "
            "fitted treatment columns and width",
        ):
            model.getAvgTreatmentEffects()

    def test_width_corruption_and_fit_exclusivity_are_not_silently_accepted(self):
        model = self.plural.copy({})
        model._java_obj.setRawTreatmentEffectCount(3)
        with self.assertRaisesRegex(
            Exception,
            "Plural treatment statistics do not match "
            "fitted treatment columns and width",
        ):
            model.getRawTreatmentEffectRows()
        with self.assertRaisesRegex(
            Exception, "Set exactly one of treatmentCol or treatmentCols"
        ):
            self.estimator().setTreatmentCol("t0").fit(self.data)
        neither = DoubleMLEstimator().setOutcomeCol("y")
        with self.assertRaisesRegex(
            Exception, "Set exactly one of treatmentCol or treatmentCols"
        ):
            neither.fit(self.data)

    def test_param_map_and_loaded_model_same_width_rename(self):
        estimator = (
            DoubleMLEstimator()
            .setOutcomeCol("y")
            .setTreatmentModel(LinearRegression())
            .setOutcomeModel(LinearRegression())
        )
        model = estimator.fit(
            self.data, {estimator.getParam("treatmentCols"): ["t0", "t1"]}
        )
        self.assertEqual(model.getFittedTreatmentCols(), ["t0", "t1"])
        with tempfile.TemporaryDirectory(prefix="double-ml-renamed-") as directory:
            path = str(Path(directory) / "model")
            model.write().overwrite().save(path)
            loaded = DoubleMLModel.load(path)
            loaded.setTreatmentCols(["t1", "t0"])
            loaded._transfer_params_to_java()
            with self.assertRaisesRegex(
                Exception,
                "Plural treatment statistics do not match "
                "fitted treatment columns and width",
            ):
                loaded.getConfidenceIntervals()

    def test_ambiguous_jvm_route_rejects_statistics_after_copy_and_reload(self):
        ambiguous = self.plural.copy({})
        ambiguous._java_obj.setTreatmentCol("t0")
        copied = ambiguous.copy({})
        with tempfile.TemporaryDirectory(prefix="double-ml-ambiguous-") as directory:
            path = str(Path(directory) / "model")
            ambiguous.write().overwrite().save(path)
            loaded = DoubleMLModel.load(path)
            for model in [ambiguous, copied, loaded]:
                getters = [
                    model.getRawTreatmentEffectsByIteration,
                    model.getRawTreatmentEffectRows,
                    model.getFittedTreatmentCols,
                    model.getAvgTreatmentEffects,
                    model.getPValues,
                    model.getConfidenceIntervals,
                    model._java_obj.getRawTreatmentEffectCount,
                ]
                for getter in getters:
                    with self.subTest(getter=str(getter)):
                        with self.assertRaisesRegex(
                            Exception,
                            "Plural treatment statistics are not available on this model",
                        ):
                            getter()
        # Generated Python raw-Param getters are parameter introspection, not JVM statistics.
        self.assert_statistics_equal(self.plural.copy({}), self.plural)

    def test_auxiliary_glr_offset_both_roles_preserve_caller_parameters(self):
        for role, offset in [("treatment", "off.set"), ("outcome", "off`set")]:
            with self.subTest(role=role, offset=offset):
                data = self.data.withColumn(
                    offset, F.lit(0.2) * F.sin(F.col("rowId") * 0.23)
                )
                nuisance = (
                    GeneralizedLinearRegression()
                    .setFamily("gaussian")
                    .setLink("identity")
                    .setRegParam(0.0)
                    .setMaxIter(10)
                    .setOffsetCol(offset)
                )
                nuisance._transfer_params_to_java()
                before = dict(nuisance.extractParamMap())
                java_before = nuisance._java_obj.extractParamMap().toString()
                estimator = self.estimator()
                if role == "treatment":
                    estimator.setTreatmentModel(nuisance)
                else:
                    estimator.setOutcomeModel(nuisance)
                model = estimator.fit(data)
                self.assertEqual(model.getFittedTreatmentCols(), ["t0", "t1"])
                self.assertEqual(len(model.getAvgTreatmentEffects()), 2)
                self.assertTrue(
                    all(math.isfinite(v) for v in model.getAvgTreatmentEffects())
                )
                self.assertEqual(nuisance.getOffsetCol(), offset)
                self.assertEqual(nuisance._java_obj.getOffsetCol(), offset)
                self.assertEqual(dict(nuisance.extractParamMap()), before)
                self.assertEqual(
                    nuisance._java_obj.extractParamMap().toString(), java_before
                )
