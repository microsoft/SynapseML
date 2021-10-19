// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.fuzzing

import com.microsoft.azure.synapse.ml.core.contracts.{HasFeaturesCol, HasInputCol, HasLabelCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils
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
      "com.microsoft.azure.synapse.ml.cognitive.DocumentTranslator",
      "org.apache.spark.ml.feature.FastVectorAssembler",
      "com.microsoft.azure.synapse.ml.featurize.ValueIndexerModel",
      "com.microsoft.azure.synapse.ml.cntk.train.CNTKLearner",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparameters",
      "com.microsoft.azure.synapse.ml.train.ComputePerInstanceStatistics",
      "com.microsoft.azure.synapse.ml.featurize.DataConversion",
      "com.microsoft.azure.synapse.ml.core.serialize.TestEstimatorBase",
      "com.microsoft.azure.synapse.ml.cognitive.LocalNER",
      "com.microsoft.azure.synapse.ml.nn.KNNModel",
      "com.microsoft.azure.synapse.ml.nn.ConditionalKNNModel",
      "com.microsoft.azure.synapse.ml.train.TrainedRegressorModel",
      "com.microsoft.azure.synapse.ml.core.serialize.MixedParamTest",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparametersModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressionModel",
      "com.microsoft.azure.synapse.ml.isolationforest.IsolationForestModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitClassificationModel",
      "com.microsoft.azure.synapse.ml.core.serialize.ComplexParamTest",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitRegressionModel",
      "com.microsoft.azure.synapse.ml.core.serialize.StandardParamTest",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.azure.synapse.ml.stages.ClassBalancerModel",
      "com.microsoft.azure.synapse.ml.featurize.CleanMissingDataModel",
      "com.microsoft.azure.synapse.ml.stages.TimerModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassificationModel",
      "com.microsoft.azure.synapse.ml.train.TrainedClassifierModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRankerModel",
      "com.microsoft.azure.synapse.ml.automl.BestModel" //TODO add proper interfaces to all of these
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
      "com.microsoft.azure.synapse.ml.cognitive.DocumentTranslator",
      "com.microsoft.azure.synapse.ml.automl.BestModel",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparameters",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparametersModel",
      "com.microsoft.azure.synapse.ml.cntk.train.CNTKLearner",
      "com.microsoft.azure.synapse.ml.cognitive.LocalNER",
      "com.microsoft.azure.synapse.ml.core.serialize.ComplexParamTest",
      "com.microsoft.azure.synapse.ml.core.serialize.MixedParamTest",
      "com.microsoft.azure.synapse.ml.core.serialize.StandardParamTest",
      "com.microsoft.azure.synapse.ml.core.serialize.TestEstimatorBase",
      "com.microsoft.azure.synapse.ml.featurize.CleanMissingDataModel",
      "com.microsoft.azure.synapse.ml.featurize.DataConversion",
      "com.microsoft.azure.synapse.ml.featurize.ValueIndexerModel",
      "com.microsoft.azure.synapse.ml.isolationforest.IsolationForestModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassificationModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRankerModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressionModel",
      "com.microsoft.azure.synapse.ml.nn.ConditionalKNNModel",
      "com.microsoft.azure.synapse.ml.nn.KNNModel",
      "com.microsoft.azure.synapse.ml.stages.ClassBalancerModel",
      "com.microsoft.azure.synapse.ml.stages.TimerModel",
      "com.microsoft.azure.synapse.ml.train.ComputePerInstanceStatistics",
      "com.microsoft.azure.synapse.ml.train.TrainedClassifierModel",
      "com.microsoft.azure.synapse.ml.train.TrainedRegressorModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitClassificationModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitRegressionModel"
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
      "com.microsoft.azure.synapse.ml.cognitive.DocumentTranslator",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparameters",
      "com.microsoft.azure.synapse.ml.train.TrainedRegressorModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.azure.synapse.ml.train.TrainedClassifierModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitClassificationModel",
      "com.microsoft.azure.synapse.ml.isolationforest.IsolationForestModel",
      "com.microsoft.azure.synapse.ml.nn.ConditionalKNNModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassificationModel",
      "com.microsoft.azure.synapse.ml.core.serialize.TestEstimatorBase",
      "com.microsoft.azure.synapse.ml.core.serialize.MixedParamTest",
      "com.microsoft.azure.synapse.ml.featurize.CleanMissingDataModel",
      "com.microsoft.azure.synapse.ml.stages.TimerModel",
      "com.microsoft.azure.synapse.ml.featurize.DataConversion",
      "com.microsoft.azure.synapse.ml.automl.TuneHyperparametersModel",
      "com.microsoft.azure.synapse.ml.automl.BestModel",
      "com.microsoft.azure.synapse.ml.nn.KNNModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitRegressionModel",
      "com.microsoft.azure.synapse.ml.stages.ClassBalancerModel",
      "com.microsoft.azure.synapse.ml.core.serialize.StandardParamTest",
      "com.microsoft.azure.synapse.ml.core.serialize.ComplexParamTest",
      "com.microsoft.azure.synapse.ml.featurize.ValueIndexerModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRankerModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressionModel",
      "com.microsoft.azure.synapse.ml.train.ComputePerInstanceStatistics"
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
      "com.microsoft.azure.synapse.ml.stages.UDFTransformer") // needs to hide setters from model
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
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitClassifier", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitContextualBandit", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitRegressor", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRanker", // HasFeaturesCol is part of spark's base class
      "com.microsoft.azure.synapse.ml.isolationforest.IsolationForest", // HasFeaturesCol from spark
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassificationModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRankerModel",
      "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressionModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitClassificationModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitRegressionModel",
      "com.microsoft.azure.synapse.ml.vw.VowpalWabbitContextualBanditModel",
      "com.microsoft.azure.synapse.ml.explainers.ImageLIME",
      "com.microsoft.azure.synapse.ml.explainers.ImageSHAP",
      "com.microsoft.azure.synapse.ml.explainers.TabularLIME",
      "com.microsoft.azure.synapse.ml.explainers.TabularSHAP",
      "com.microsoft.azure.synapse.ml.explainers.TextLIME",
      "com.microsoft.azure.synapse.ml.explainers.TextSHAP",
      "com.microsoft.azure.synapse.ml.explainers.VectorLIME",
      "com.microsoft.azure.synapse.ml.explainers.VectorSHAP",
      "com.microsoft.azure.synapse.ml.exploratory.imbalance.AggregateMeasures",
      "com.microsoft.azure.synapse.ml.exploratory.imbalance.DistributionMeasures",
      "com.microsoft.azure.synapse.ml.exploratory.imbalance.ParityMeasures"
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

  private lazy val readers: List[MLReadable[_]] = JarLoadingUtils.instantiateObjects[MLReadable[_]]()

  private lazy val pipelineStages: List[PipelineStage] = JarLoadingUtils.instantiateServices[PipelineStage]()

  private lazy val experimentFuzzers: List[ExperimentFuzzing[_ <: PipelineStage]] =
    JarLoadingUtils.instantiateServices[ExperimentFuzzing[_ <: PipelineStage]]()

  private lazy val serializationFuzzers: List[SerializationFuzzing[_ <: PipelineStage with MLWritable]] =
    JarLoadingUtils.instantiateServices[SerializationFuzzing[_ <: PipelineStage with MLWritable]]()

  private lazy val pytestFuzzers: List[PyTestFuzzing[_ <: PipelineStage]] =
    JarLoadingUtils.instantiateServices[PyTestFuzzing[_ <: PipelineStage]]()

}
