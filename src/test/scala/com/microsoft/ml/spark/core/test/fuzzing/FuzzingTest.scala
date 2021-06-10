// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.test.fuzzing

import com.microsoft.ml.spark.core.contracts.{HasFeaturesCol, HasInputCol, HasLabelCol, HasOutputCol}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.utils.JarLoadingUtils
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{MLReadable, MLWritable}

import java.lang.reflect.ParameterizedType
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class FuzzingTest extends TestBase {

  // Use this for more detailed output from the Jar Loader
  val debug = false

  // use this to quickly see all the results for all failing modules
  // Note that this could make the tests pass when they should be failing
  val disableFailure = false

  test("Assert things have been loaded") {
    // Needed because the session in TB is lazy
    spark
    assert(serializationFuzzers.nonEmpty)
    assert(pipelineStages.nonEmpty)
    assert(readers.nonEmpty)
  }

  test("Verify stage fitting and transforming") {
    val exemptions: Set[String] = Set(
      "org.apache.spark.ml.feature.FastVectorAssembler",
      "com.microsoft.ml.spark.featurize.ValueIndexerModel",
      "com.microsoft.ml.spark.cntk.train.CNTKLearner",
      "com.microsoft.ml.spark.automl.TuneHyperparameters",
      "com.microsoft.ml.spark.train.ComputePerInstanceStatistics",
      "com.microsoft.ml.spark.featurize.DataConversion",
      "com.microsoft.ml.spark.core.serialize.TestEstimatorBase",
      "com.microsoft.ml.spark.cognitive.LocalNER",
      "com.microsoft.ml.spark.nn.KNNModel",
      "com.microsoft.ml.spark.nn.ConditionalKNNModel",
      "com.microsoft.ml.spark.train.TrainedRegressorModel",
      "com.microsoft.ml.spark.core.serialize.MixedParamTest",
      "com.microsoft.ml.spark.automl.TuneHyperparametersModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel",
      "com.microsoft.ml.spark.isolationforest.IsolationForestModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitClassificationModel",
      "com.microsoft.ml.spark.core.serialize.ComplexParamTest",
      "com.microsoft.ml.spark.vw.VowpalWabbitRegressionModel",
      "com.microsoft.ml.spark.core.serialize.StandardParamTest",
      "com.microsoft.ml.spark.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.ml.spark.stages.ClassBalancerModel",
      "com.microsoft.ml.spark.featurize.CleanMissingDataModel",
      "com.microsoft.ml.spark.stages.TimerModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMClassificationModel",
      "com.microsoft.ml.spark.train.TrainedClassifierModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRankerModel",
      "com.microsoft.ml.spark.automl.BestModel" //TODO add proper interfaces to all of these
    )
    val applicableStages = pipelineStages.filter(t => !exemptions(t.getClass.getName))
    val applicableClasses = applicableStages.map(_.getClass.asInstanceOf[Class[_]]).toSet
    val classToFuzzer: Map[Class[_], ExperimentFuzzing[_ <: PipelineStage]] =
      experimentFuzzers.map(f =>
        (Class.forName(f.getClass.getMethod("experimentTestObjects")
          .getGenericReturnType.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.getTypeName)
          , f)).toMap
    val classesWithFuzzers = classToFuzzer.keys
    val classesWithoutFuzzers = applicableClasses.diff(classesWithFuzzers.toSet)
    assertOrLog(classesWithoutFuzzers.isEmpty,
      "These classes do not have Experiment fuzzers, \n" +
        "(try extending Estimator/Transformer Fuzzing): \n" +
        classesWithoutFuzzers.mkString("\n"))
  }

  test("Verify all stages can be serialized") {
    val exemptions: Set[String] = Set(
      "com.microsoft.ml.spark.automl.BestModel",
      "com.microsoft.ml.spark.automl.TuneHyperparameters",
      "com.microsoft.ml.spark.automl.TuneHyperparametersModel",
      "com.microsoft.ml.spark.cntk.train.CNTKLearner",
      "com.microsoft.ml.spark.cognitive.LocalNER",
      "com.microsoft.ml.spark.core.serialize.ComplexParamTest",
      "com.microsoft.ml.spark.core.serialize.MixedParamTest",
      "com.microsoft.ml.spark.core.serialize.StandardParamTest",
      "com.microsoft.ml.spark.core.serialize.TestEstimatorBase",
      "com.microsoft.ml.spark.explainers.ImageLIME",
      "com.microsoft.ml.spark.explainers.ImageSHAP",
      "com.microsoft.ml.spark.explainers.TabularLIME",
      "com.microsoft.ml.spark.explainers.TabularSHAP",
      "com.microsoft.ml.spark.explainers.TextLIME",
      "com.microsoft.ml.spark.explainers.TextSHAP",
      "com.microsoft.ml.spark.explainers.VectorLIME",
      "com.microsoft.ml.spark.explainers.VectorSHAP",
      "com.microsoft.ml.spark.featurize.CleanMissingDataModel",
      "com.microsoft.ml.spark.featurize.DataConversion",
      "com.microsoft.ml.spark.featurize.ValueIndexerModel",
      "com.microsoft.ml.spark.isolationforest.IsolationForestModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMClassificationModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRankerModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel",
      "com.microsoft.ml.spark.nn.ConditionalKNNModel",
      "com.microsoft.ml.spark.nn.KNNModel",
      "com.microsoft.ml.spark.stages.ClassBalancerModel",
      "com.microsoft.ml.spark.stages.TimerModel",
      "com.microsoft.ml.spark.train.ComputePerInstanceStatistics",
      "com.microsoft.ml.spark.train.TrainedClassifierModel",
      "com.microsoft.ml.spark.train.TrainedRegressorModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitClassificationModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitRegressionModel"
    )
    val applicableStages = pipelineStages.filter(t => !exemptions(t.getClass.getName))
    val applicableClasses = applicableStages.map(_.getClass.asInstanceOf[Class[_]]).toSet
    val classToFuzzer: Map[Class[_], SerializationFuzzing[_ <: PipelineStage with MLWritable]] =
      serializationFuzzers.map(f =>
        (Class.forName(f.getClass.getMethod("serializationTestObjects")
          .getGenericReturnType.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.getTypeName),
          f)
      ).toMap
    val classesWithFuzzers = classToFuzzer.keys
    val classesWithoutFuzzers = applicableClasses.diff(classesWithFuzzers.toSet)
    assertOrLog(classesWithoutFuzzers.isEmpty,
      "These classes do not have Serialization fuzzers,\n" +
        "(try extending Estimator/Transformer Fuzzing):\n  " +
        classesWithoutFuzzers.mkString("\n  "))
  }

  test("Verify all stages can be tested in python") {
    val exemptions: Set[String] = Set(
      "com.microsoft.ml.spark.automl.TuneHyperparameters",
      "com.microsoft.ml.spark.train.TrainedRegressorModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.ml.spark.train.TrainedClassifierModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitClassificationModel",
      "com.microsoft.ml.spark.isolationforest.IsolationForestModel",
      "com.microsoft.ml.spark.nn.ConditionalKNNModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMClassificationModel",
      "com.microsoft.ml.spark.core.serialize.TestEstimatorBase",
      "com.microsoft.ml.spark.core.serialize.MixedParamTest",
      "com.microsoft.ml.spark.featurize.CleanMissingDataModel",
      "com.microsoft.ml.spark.stages.TimerModel",
      "com.microsoft.ml.spark.featurize.DataConversion",
      "com.microsoft.ml.spark.automl.TuneHyperparametersModel",
      "com.microsoft.ml.spark.automl.BestModel",
      "com.microsoft.ml.spark.nn.KNNModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitRegressionModel",
      "com.microsoft.ml.spark.stages.ClassBalancerModel",
      "com.microsoft.ml.spark.core.serialize.StandardParamTest",
      "com.microsoft.ml.spark.core.serialize.ComplexParamTest",
      "com.microsoft.ml.spark.featurize.ValueIndexerModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRankerModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel",
      "com.microsoft.ml.spark.train.ComputePerInstanceStatistics"
    )
    val applicableStages = pipelineStages.filter(t => !exemptions(t.getClass.getName))
    val applicableClasses = applicableStages.map(_.getClass.asInstanceOf[Class[_]]).toSet
    val classToFuzzer: Map[Class[_], PyTestFuzzing[_ <: PipelineStage]] =
      pytestFuzzers.map(f =>
        (Class.forName(f.getClass.getMethod("pyTestObjects")
          .getGenericReturnType.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.asInstanceOf[ParameterizedType]
          .getActualTypeArguments.head.getTypeName),
          f)
      ).toMap
    val classesWithFuzzers = classToFuzzer.keys
    val classesWithoutFuzzers = applicableClasses.diff(classesWithFuzzers.toSet)
    assertOrLog(classesWithoutFuzzers.isEmpty, classesWithoutFuzzers.mkString("\n"))
  }

  // TODO verify that model UIDs match the class names, perhaps use a Trait

  test("Verify all pipeline stages don't have exotic characters") {
    val badChars = List(",", "\"", "'", ".")
    pipelineStages.foreach { pipelineStage =>
      pipelineStage.params.foreach { param =>
        assertOrLog(!param.name.contains(badChars), param.name)
      }
    }
  }

  test("Verify all pipeline stage values match their param names") {
    val exemptions: Set[String] = Set[String](
      "com.microsoft.ml.spark.stages.UDFTransformer") // needs to hide setters from model
    pipelineStages.foreach { pipelineStage =>
      if (!exemptions(pipelineStage.getClass.getName)) {
        val paramFields =
          pipelineStage.getClass.getDeclaredFields
            .filter(f => classOf[Param[Any]].isAssignableFrom(f.getType))
        val paramNames = paramFields.map { f =>
          f.setAccessible(true)
          val p = f.get(pipelineStage)
          p.asInstanceOf[Param[Any]].name
        }
        val paramFieldNames = paramFields.map(_.getName)
        assertOrLog(paramNames === paramFieldNames,
          paramNames.mkString(",") + "\n" +
            paramFieldNames.mkString(",") + "\n" +
            pipelineStage.getClass.getName)
      }
    }
  }

  test("Verify correct use of mixins") {
    val triggers = Map(
      "inputCol" -> classOf[HasInputCol],
      "inputColumn" -> classOf[HasInputCol],
      "outputCol" -> classOf[HasOutputCol],
      "outputColumn" -> classOf[HasOutputCol],
      "labelCol" -> classOf[HasLabelCol],
      "labelColumn" -> classOf[HasLabelCol],
      "featuresCol" -> classOf[HasFeaturesCol],
      "featuresColumn" -> classOf[HasFeaturesCol]
    )

    val exemptions = Set[String](
      "org.apache.spark.ml.feature.FastVectorAssembler", // In Spark namespace
      "com.microsoft.ml.spark.vw.VowpalWabbitClassifier", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.vw.VowpalWabbitContextualBandit", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.vw.VowpalWabbitRegressor", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.lightgbm.LightGBMClassifier", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.lightgbm.LightGBMRegressor", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.lightgbm.LightGBMRanker", // HasFeaturesCol is part of spark's base class
      "com.microsoft.ml.spark.isolationforest.IsolationForest", // HasFeaturesCol from spark
      "com.microsoft.ml.spark.lightgbm.LightGBMClassificationModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRankerModel",
      "com.microsoft.ml.spark.lightgbm.LightGBMRegressionModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitClassificationModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitRegressionModel",
      "com.microsoft.ml.spark.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.ml.spark.explainers.ImageLIME",
      "com.microsoft.ml.spark.explainers.ImageSHAP",
      "com.microsoft.ml.spark.explainers.TabularLIME",
      "com.microsoft.ml.spark.explainers.TabularSHAP",
      "com.microsoft.ml.spark.explainers.TextLIME",
      "com.microsoft.ml.spark.explainers.TextSHAP",
      "com.microsoft.ml.spark.explainers.VectorLIME",
      "com.microsoft.ml.spark.explainers.VectorSHAP"
    )

    pipelineStages.foreach { stage =>
      if (!exemptions(stage.getClass.getName)) {
        stage.params.foreach { param =>
          triggers.get(param.name) match {
            case Some(clazz) =>
              assertOrLog(clazz.isAssignableFrom(stage.getClass),
                stage.getClass.getName + " needs to extend " + clazz.getName)
            case None =>
          }
        }
      }
    }
  }

  private def assertOrLog(condition: Boolean, hint: String = "",
                          disableFailure: Boolean = disableFailure): Unit = {
    if (disableFailure && !condition) println(hint)
    else assert(condition, hint)
    ()
  }

  // set the context loader to pick up on the jars
  //Thread.currentThread().setContextClassLoader(JarLoadingUtils.classLoader)

  private lazy val readers: List[MLReadable[_]] = JarLoadingUtils.instantiateObjects[MLReadable[_]]

  private lazy val pipelineStages: List[PipelineStage] = JarLoadingUtils.instantiateServices[PipelineStage]

  private lazy val experimentFuzzers: List[ExperimentFuzzing[_ <: PipelineStage]] =
    JarLoadingUtils.instantiateServices[ExperimentFuzzing[_ <: PipelineStage]]

  private lazy val serializationFuzzers: List[SerializationFuzzing[_ <: PipelineStage with MLWritable]] =
    JarLoadingUtils.instantiateServices[SerializationFuzzing[_ <: PipelineStage with MLWritable]]

  private lazy val pytestFuzzers: List[PyTestFuzzing[_ <: PipelineStage]] =
    JarLoadingUtils.instantiateServices[PyTestFuzzing[_ <: PipelineStage]]

}
