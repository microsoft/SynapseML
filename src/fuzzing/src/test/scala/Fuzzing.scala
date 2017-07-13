// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.DataFrame
import org.apache.commons.io.FileUtils

import scala.language.existentials
import scala.util.Random

/** Tests to validate fuzzing of modules. */
class Fuzzing extends TestBase {

  // Needed because the session in MTB is lazy
  session

  val numRows = 10
  val numCols = 20
  val numSlotsPerVectorCol = Array(15, 15)
  val randomSeed = new Random()

  // Use this for more detailed output from the Jar Loader
  val debug = false

  // use this to quickly see all the results for all failing modules
  // Note that this could make the tests pass when they should be failing
  val disableFailure = false

  test("Verify all PipelineStages can be saved then loaded") {

    val exemptions: Set[String] = Set()

    val applicableStages = pipelineStages.filter(t => !exemptions(t.getClass.getName))
    applicableStages.foreach(t => if (!readerMap.contains(t.getClass.getName)) {
      assertOrLog(false, s"need to have a companion reader for class ${t.getClass.getName}")
    })

    applicableStages.foreach(t => trySave(t, Some(readerMap(t.getClass.getName))))
  }

  // TODO verify that model UIDs match the class names, perhaps use a Trait

  test("Verify all estimators can be turned into pipelines, saved and loaded") {
    val exemptions: Set[String] = Set(
      "com.microsoft.ml.spark.CNTKLearner",
      "com.microsoft.ml.spark.TrainClassifier",
      "com.microsoft.ml.spark.TrainRegressor",
      "com.microsoft.ml.spark.FindBestModel"
    )
    val applicableEstimators = estimators.filter(t => !exemptions(t.getClass.getName))

    applicableEstimators.foreach(est => {
      val estimatorName = est.getClass.getName
      println()
      println(s"Running estimator: ${est.toString} with name: ${estimatorName}")
      val (dataset, pipelineStage) =
        if (estimatorFuzzers.contains(estimatorName)) {
          println("Generating dataset from estimator fuzzer")
          val estimatorFuzzer = estimatorFuzzers(estimatorName)
          val fitDataset = estimatorFuzzer.createFitDataset
          val estUpdated = estimatorFuzzer.setParams(fitDataset, est.copy(ParamMap()))
          (fitDataset, estUpdated.asInstanceOf[PipelineStage])
        } else {
          println("Generating random dataset")
          (createDataSet, est.copy(ParamMap()).asInstanceOf[PipelineStage])
        }
      tryRun(() => {
        var pipelineModel = new Pipeline().setStages(Array(pipelineStage)).fit(dataset)
        pipelineModel = trySave(pipelineModel,
          Some(PipelineModel.asInstanceOf[MLReadable[Any]])).get.asInstanceOf[PipelineModel]
        val dfTransform =
          if (estimatorFuzzers.contains(estimatorName)) {
            estimatorFuzzers(estimatorName).createTransformDataset
          } else {
            createDataSet
          }
        pipelineModel.transform(dfTransform)
        ()
      })
    })
  }

  test("Verify all transformers can be turned into pipelines, saved and loaded") {
    transformers.foreach(tr => {
      val transformerName = tr.getClass.getName
      println()
      println(s"Running transformer: ${tr.toString} with name: ${transformerName}")
      val (dataset, pipelineStage) =
        if (transformerFuzzers.contains(transformerName)) {
          println("Generating dataset from transformer fuzzer")
          val transformerFuzzer = transformerFuzzers(transformerName)
          val fitDataset = transformerFuzzer.createDataset
          val trUpdated = transformerFuzzer.setParams(fitDataset, tr.copy(ParamMap()))
          (fitDataset, trUpdated.asInstanceOf[PipelineStage])
        } else {
          println("Generating random dataset")
          (createDataSet, tr.copy(ParamMap()).asInstanceOf[PipelineStage])
        }
      tryRun(() => {
        val pipeline = new Pipeline().setStages(Array(pipelineStage))
        val pipelineModel = pipeline.fit(dataset)
        trySave(pipelineModel)
        ()
      })
    })
  }

  test("Verify all pipeline stages dont have exotic characters") {
    val badChars = List(",", "\"", "'", ".")
    pipelineStages.foreach { pipelineStage =>
      pipelineStage.params.foreach { param =>
        assertOrLog(!param.name.contains(badChars))
        assertOrLog(!param.doc.contains("\""))
      }
    }
  }

  test("Verify all pipeline stage values match their param names") {
    val exemptions: Set[String] = Set()
    pipelineStages.foreach { pipelineStage =>
      if (!exemptions(pipelineStage.getClass.getName)) {
        val paramFields = pipelineStage.getClass.getDeclaredFields
          .filter(f => classOf[Param[Any]].isAssignableFrom(f.getType))

        val paramNames = paramFields.map { f =>
          f.setAccessible(true)
          val p = f.get(pipelineStage)
          p.asInstanceOf[Param[Any]].name
        }
        val paramFieldNames = paramFields.map(_.getName)
        assertOrLog(paramNames === paramFieldNames, pipelineStage.getClass.getName)
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
      "com.microsoft.ml.spark.TextFeaturizer" // needs to hide setters from model
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
    if (disableFailure && !condition) {
      println(hint)
    } else {
      assert(condition, hint)
    }
    ()
  }

  private def throwOrLog(e: Throwable, message: String = "",
                         disableFailure: Boolean = disableFailure): Unit = {
    println(message)
    if (disableFailure) {
      println(e.getMessage)
      e.printStackTrace(System.out)
    } else {
      throw e
    }
  }

  // set the context loader to pick up on the jars
  Thread.currentThread().setContextClassLoader(JarLoadingUtils.classLoader)

  private lazy val transformers: List[Transformer] = JarLoadingUtils.loadClass[Transformer](debug = debug)

  private lazy val estimators: List[Estimator[_]] = JarLoadingUtils.loadClass[Estimator[_]](debug = debug)

  private lazy val readers: List[MLReadable[_]] = JarLoadingUtils.loadObject[MLReadable[_]](debug = debug)

  private lazy val pipelineStages: List[PipelineStage] = JarLoadingUtils.loadClass[PipelineStage](debug = debug)

  private lazy val readerMap = readers.map {
    r => (r.getClass.getName.dropRight(1), r.asInstanceOf[MLReadable[Any]])
  }.toMap

  private lazy val transformerFuzzers: Map[String, TransformerFuzzingTest] =
    JarLoadingUtils.loadTestClass[TransformerFuzzingTest](debug = debug)
      .map(tr => (tr.getClassName, tr)).toMap

  private lazy val estimatorFuzzers: Map[String, EstimatorFuzzingTest] =
    JarLoadingUtils.loadTestClass[EstimatorFuzzingTest](debug = debug)
      .map(est => (est.getClassName, est)).toMap

  private def trySave(stage: PipelineStage, reader: Option[MLReadable[Any]] = None,
                      path: String = "testModels"): Option[PipelineStage] = {
    stage match {
      case w: PipelineStage with MLWritable =>
        try {
          w.write.overwrite().save(path)
          reader match {
            case Some(r) =>
              val loaded = r.load(path).asInstanceOf[PipelineStage]
              assertOrLog(loaded.params.sameElements(w.params))
              println(s"Round trip succeeded for ${w.getClass.getName}")
              Some(loaded)
            case None => None
          }
        } catch {
          case e: Throwable =>
            throwOrLog(e, w.getClass.getName + " encounters an error while saving/loading")
            None
        } finally {
          FileUtils.forceDelete(new java.io.File(path))
        }
      case tr =>
        assertOrLog(false, tr.getClass.getName + " needs to extend MLWritable")
        None
    }
  }

  private def createDataSet: DataFrame = {
    GenerateDataset
      .generateDataset(session,
        new BasicDatasetGenerationConstraints(numRows, numCols, numSlotsPerVectorCol),
        randomSeed.nextLong())
  }

  private def tryRun(func: () => Unit): Unit = {
    try {
      func()
    } catch {
      case ne: java.util.NoSuchElementException =>
        throwOrLog(ne, s"Could not transform: $ne", disableFailure=true)
      case th: Throwable =>
        throwOrLog(th, s"Encountered unknown error: $th", disableFailure=true)
    }
  }

}
