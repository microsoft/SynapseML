// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.fuzzing

import com.microsoft.azure.synapse.ml.codegen.CodegenConfig
import com.microsoft.azure.synapse.ml.codegen.GenerationUtils._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param._
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils.capitalize
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
  * Class for holding test information, call by name to avoid uneccesary computations in test generations
  *
  * @param stage         Pipeline stage for testing
  * @param fitDFArg      Dataframe to fit
  * @param transDFArg    Dataframe to transform
  * @param validateDFArg Optional dataframe to validate against
  * @tparam S The type of the stage
  */
class TestObject[S <: PipelineStage](val stage: S,
                                     fitDFArg: => DataFrame,
                                     transDFArg: => DataFrame,
                                     validateDFArg: => Option[DataFrame]) {
  lazy val fitDF: DataFrame = fitDFArg
  lazy val transDF: DataFrame = transDFArg
  lazy val validateDF: Option[DataFrame] = validateDFArg

  def this(stage: S, df: => DataFrame) = {
    this(stage, df, df, None)
  }

  def this(stage: S, fitDF: => DataFrame, transDF: => DataFrame) = {
    this(stage, fitDF, transDF, None)
  }

}

trait TestFuzzingUtil {

  val testClassName: String = this.getClass.getName.split(".".toCharArray).last

  val testFitting = false
}

trait DotnetTestFuzzing[S <: PipelineStage] extends TestBase with DataFrameEquality with TestFuzzingUtil {

  def dotnetTestObjects(): Seq[TestObject[S]]

  def dotnetTestDataDir(conf: CodegenConfig): File = FileUtilities.join(
    conf.dotnetTestDataDir, this.getClass.getName.split(".".toCharArray).last)

  def saveDotnetDataset(conf: CodegenConfig, df: DataFrame, name: String): Unit = {
    df.write.mode("overwrite").parquet(new File(dotnetTestDataDir(conf), s"$name.parquet").toString)
  }

  def saveDotnetModel(conf: CodegenConfig, model: S, name: String): Unit = {
    model match {
      case writable: MLWritable =>
        writable.write.overwrite().save(new File(dotnetTestDataDir(conf), s"$name.model").toString)
      case _ =>
        throw new IllegalArgumentException(s"${model.getClass.getName} is not writable")
    }
  }

  def saveDotnetTestData(conf: CodegenConfig): Unit = {
    dotnetTestDataDir(conf).mkdirs()
    dotnetTestObjects().zipWithIndex.foreach { case (to, i) =>
      saveDotnetModel(conf, to.stage, s"model-$i")
      if (testFitting) {
        saveDotnetDataset(conf, to.fitDF, s"fit-$i")
        saveDotnetDataset(conf, to.transDF, s"trans-$i")
        to.validateDF.foreach(saveDotnetDataset(conf, _, s"val-$i"))
      }
    }
  }

  def dotnetTestInstantiateModel(stage: S, num: Int, testDataDir: String): String = {
    val fullParamMap = stage.extractParamMap().toSeq
    val partialParamMap = stage.extractParamMap().toSeq.filter(pp => stage.get(pp.param).isDefined)
    val fullStageName = stage.getClass.getName
      .replace("com.microsoft.azure.synapse.ml", "Synapse.ML")
      .replace("org.apache.spark.ml", "Microsoft.Spark.ML")
      .split(".".toCharArray).map(capitalize).mkString(".")

    def instantiateModel(paramMap: Seq[ParamPair[_]]): String = {
      val externalLoadlingLines = paramMap.flatMap { pp =>
        pp.param match {
          case pp: PipelineStageWrappable[_] =>
            Some(pp.dotnetLoadLine(num, testDataDir))
          case ep: EvaluatorParam =>
            Some(ep.dotnetLoadLine(num, testDataDir))
          case ep: ExternalDotnetWrappableParam[_] =>
            Some(ep.dotnetLoadLine(num))
          case _ => None
        }
      }.mkString("\n")
      s"""
         |$externalLoadlingLines
         |
         |var model = new $fullStageName()
         |${indent(paramMap.map(dotnetRenderParam(_)).mkString("\n"), 1)};
         |
         |""".stripMargin
    }

    try {
      instantiateModel(fullParamMap)
    } catch {
      case _: NotImplementedError =>
        println(s"could not generate full test for $fullStageName, resorting to partial test")
        instantiateModel(partialParamMap)
    }
  }


  def makeDotnetTests(testObject: TestObject[S], num: Int, testDataDir: String): String = {
    val stage = testObject.stage
    val stageName = stage.getClass.getName.split(".".toCharArray).last
    val fullStageName = stage.getClass.getName
      .replace("com.microsoft.azure.synapse.ml", "Synapse.ML")
      .replace("org.apache.spark.ml", "Microsoft.Spark.ML")
      .split(".".toCharArray).map(capitalize).mkString(".")
    val fittingTest = stage match {
      case _: Estimator[_] if testFitting =>
        s"""
           |var fdf = _spark.Read().Parquet(Path.Combine(TestDataDir, "fit-$num.parquet"));
           |var tdf = _spark.Read().Parquet(Path.Combine(TestDataDir, "trans-$num.parquet"));
           |model.Fit(fdf).Transform(tdf).Show();
           |""".stripMargin
      case _: Transformer if testFitting =>
        s"""
           |var tdf = _spark.Read().Parquet(Path.Combine(TestDataDir, "trans-$num.parquet"));
           |model.Transform(tdf).Show();
           |""".stripMargin
      case _ => ""
    }

    s"""
       |[Fact]
       |public void Test${stageName}Constructor$num()
       |{
       |    void AssertCorrespondence($fullStageName model, string name, int num)
       |    {
       |        model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
       |        _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
       |            "assertEqual", "${stage.getClass.getName}", Path.Combine(TestDataDir, name),
       |            Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
       |    }
       |${indent(dotnetTestInstantiateModel(stage, num, testDataDir), 1)}
       |
       |    AssertCorrespondence(model, "dotnet-constructor-model-$num.model", $num);
       |
       |${indent(fittingTest, 1)}
       |}
       |
       |""".stripMargin
  }

  //noinspection ScalaStyle
  def makeDotnetTestFile(conf: CodegenConfig): Unit = {
    spark
    saveDotnetTestData(conf)
    val testDataDirString = dotnetTestDataDir(conf).toString
    val generatedTests = dotnetTestObjects().zipWithIndex.map { case (to, i) =>
      makeDotnetTests(to, i, testDataDirString) }
    val stage = dotnetTestObjects().head.stage
    val importPath = stage.getClass.getName.split(".".toCharArray).dropRight(1)
    val importPathString = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.azure.synapse.ml", "Synapse.ML")
      .replaceAllLiterally("org.apache.spark.ml", "Microsoft.Spark.ML")
      .split(".".toCharArray).map(capitalize).mkString(".")
    val externalLoaderImports = conf.name match {
      case "synapseml-deep-learning" =>
        s"""using Synapse.ML.Cntk;
           |using Synapse.ML.Stages;
           |""".stripMargin
      case _ => ""
    }
    val namespaceString = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.azure.synapse.ml", "SynapseMLtest")
      .replaceAllLiterally("org.apache.spark.ml", "Microsoft.Spark.ML.Test")
      .split(".".toCharArray).map(capitalize).mkString(".")
    val testClass =
      s"""
         |// Copyright (C) Microsoft Corporation. All rights reserved.
         |// Licensed under the MIT License. See LICENSE in project root for information.
         |
         |using System;
         |using System.IO;
         |using System.Collections.Generic;
         |using Microsoft.Spark.Interop.Ipc;
         |using Microsoft.Spark.ML;
         |using Microsoft.Spark.ML.Classification;
         |using Microsoft.Spark.ML.Feature;
         |using Microsoft.Spark.ML.Feature.Param;
         |using Microsoft.Spark.ML.Recommendation;
         |using Microsoft.Spark.ML.Regression;
         |using Microsoft.Spark.Sql;
         |using Microsoft.Spark.Sql.Types;
         |using SynapseML.Dotnet.Utils;
         |
         |using Xunit;
         |using SynapseMLtest.Utils;
         |using SynapseMLtest.Helper;
         |using $importPathString;
         |$externalLoaderImports
         |
         |namespace $namespaceString
         |{
         |
         |    [Collection("SynapseML Tests")]
         |    public class $testClassName
         |    {
         |        public const string TestDataDir = "${testDataDirString.replaceAllLiterally("\\", "\\\\")}";
         |        private readonly SparkSession _spark;
         |        private readonly IJvmBridge _jvm;
         |        public $testClassName(SparkFixture fixture)
         |        {
         |            _spark = fixture.Spark;
         |            _jvm = fixture.Jvm;
         |        }
         |
         |${indent(generatedTests.mkString("\n\n"), 2)}
         |    }
         |
         |}
         |
         |""".stripMargin

    val testFolders = namespaceString.split(".".toCharArray)
    val testDir = FileUtilities.join((Seq(conf.dotnetTestDir.toString) ++ testFolders.toSeq): _*)
    testDir.mkdirs()
    Files.write(
      FileUtilities.join(testDir, "Test" + testClassName.capitalize + ".cs").toPath,
      testClass.getBytes(StandardCharsets.UTF_8))
  }

}

trait PyTestFuzzing[S <: PipelineStage] extends TestBase with DataFrameEquality with TestFuzzingUtil {

  def pyTestObjects(): Seq[TestObject[S]]

  def pyTestDataDir(conf: CodegenConfig): File = FileUtilities.join(
    conf.pyTestDataDir, this.getClass.getName.split(".".toCharArray).last)

  def savePyDataset(conf: CodegenConfig, df: DataFrame, name: String): Unit = {
    df.write.mode("overwrite").parquet(new File(pyTestDataDir(conf), s"$name.parquet").toString)
  }

  def savePyModel(conf: CodegenConfig, model: S, name: String): Unit = {
    model match {
      case writable: MLWritable =>
        writable.write.overwrite().save(new File(pyTestDataDir(conf), s"$name.model").toString)
      case _ =>
        throw new IllegalArgumentException(s"${model.getClass.getName} is not writable")
    }
  }

  def savePyTestData(conf: CodegenConfig): Unit = {
    pyTestDataDir(conf).mkdirs()
    pyTestObjects().zipWithIndex.foreach { case (to, i) =>
      savePyModel(conf, to.stage, s"model-$i")
      if (testFitting) {
        savePyDataset(conf, to.fitDF, s"fit-$i")
        savePyDataset(conf, to.transDF, s"trans-$i")
        to.validateDF.foreach(savePyDataset(conf, _, s"val-$i"))
      }
    }
  }


  def pyTestInstantiateModel(stage: S, num: Int): String = {
    val fullParamMap = stage.extractParamMap().toSeq
    val partialParamMap = stage.extractParamMap().toSeq.filter(pp => stage.get(pp.param).isDefined)
    val stageName = stage.getClass.getName.split(".".toCharArray).last

    def instantiateModel(paramMap: Seq[ParamPair[_]]) = {
      val externalLoadlingLines = paramMap.flatMap { pp =>
        pp.param match {
          case ep: ExternalPythonWrappableParam[_] =>
            Some(ep.pyLoadLine(num))
          case _ => None
        }
      }.mkString("\n")
      s"""
         |$externalLoadlingLines
         |
         |model = $stageName(
         |${indent(paramMap.map(pyRenderParam(_)).mkString(",\n"), 1)}
         |)
         |
         |""".stripMargin
    }

    try {
      instantiateModel(fullParamMap)
    } catch {
      case _: NotImplementedError =>
        println(s"could not generate full test for ${stageName}, resorting to partial test")
        instantiateModel(partialParamMap)
    }
  }


  //noinspection ScalaStyle
  def makePyTests(testObject: TestObject[S], num: Int): String = {
    val stage = testObject.stage
    val stageName = stage.getClass.getName.split(".".toCharArray).last
    val fittingTest = stage match {
      case _: Estimator[_] if testFitting =>
        s"""
           |fdf = spark.read.parquet(join(test_data_dir, "fit-$num.parquet"))
           |tdf = spark.read.parquet(join(test_data_dir, "trans-$num.parquet"))
           |model.fit(fdf).transform(tdf).show()
           |""".stripMargin
      case _: Transformer if testFitting =>
        s"""
           |tdf = spark.read.parquet(join(test_data_dir, "trans-$num.parquet"))
           |model.transform(tdf).show()
           |""".stripMargin
      case _ => ""
    }
    val mlflowTest = stage match {
      case _: Model[_] =>
        s"""
           |mlflow.spark.save_model(model, "mlflow-save-model-$num")
           |mlflow.spark.log_model(model, "mlflow-log-model-$num")
           |mlflow_model = mlflow.pyfunc.load_model("mlflow-save-model-$num")
           |""".stripMargin
      case _: Transformer => stage.getClass.getName.split(".".toCharArray).dropRight(1).last match {
        case "cognitive" =>
          s"""
             |pipeline_model = PipelineModel(stages=[model])
             |mlflow.spark.save_model(pipeline_model, "mlflow-save-model-$num")
             |mlflow.spark.log_model(pipeline_model, "mlflow-log-model-$num")
             |mlflow_model = mlflow.pyfunc.load_model("mlflow-save-model-$num")
             |""".stripMargin
        case _ => ""
      }
      case _ => ""
    }

    s"""
       |def test_${stageName}_constructor_$num(self):
       |${indent(pyTestInstantiateModel(stage, num), 1)}
       |
       |    self.assert_correspondence(model, "py-constructor-model-$num.model", $num)
       |
       |${indent(fittingTest, 1)}
       |
       |${indent(mlflowTest, 1)}
       |
       |""".stripMargin
  }


  def makePyTestFile(conf: CodegenConfig): Unit = {
    spark
    savePyTestData(conf)
    val generatedTests = pyTestObjects().zipWithIndex.map { case (to, i) => makePyTests(to, i) }
    val stage = pyTestObjects().head.stage
    val stageName = stage.getClass.getName.split(".".toCharArray).last
    val importPath = stage.getClass.getName.split(".".toCharArray).dropRight(1)
    val importPathString = importPath.mkString(".").replaceAllLiterally("com.microsoft.azure.synapse.ml", "synapse.ml")
    val testClass =
      s"""import unittest
         |from synapsemltest.spark import *
         |from $importPathString import $stageName
         |from os.path import join
         |import json
         |import mlflow
         |from pyspark.ml import PipelineModel
         |
         |test_data_dir = "${pyTestDataDir(conf).toString.replaceAllLiterally("\\", "\\\\")}"
         |
         |
         |class $testClassName(unittest.TestCase):
         |    def assert_correspondence(self, model, name, num):
         |        model.write().overwrite().save(join(test_data_dir, name))
         |        sc._jvm.com.microsoft.azure.synapse.ml.core.utils.ModelEquality.assertEqual(
         |            "${stage.getClass.getName}",
         |            str(join(test_data_dir, name)),
         |            str(join(test_data_dir, "model-{}.model".format(num)))
         |        )
         |
         |${indent(generatedTests.mkString("\n\n"), 1)}
         |
         |if __name__ == "__main__":
         |    result = unittest.main()
         |
         |""".stripMargin

    val testFolders = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.azure.synapse.ml", "synapsemltest").split(".".toCharArray)
    val testDir = FileUtilities.join((Seq(conf.pyTestDir.toString) ++ testFolders.toSeq): _*)
    testDir.mkdirs()
    Files.write(
      FileUtilities.join(testDir, "test_" + camelToSnake(testClassName) + ".py").toPath,
      testClass.getBytes(StandardCharsets.UTF_8))
  }

}

trait ExperimentFuzzing[S <: PipelineStage] extends TestBase with DataFrameEquality {

  def experimentTestObjects(): Seq[TestObject[S]]

  def runExperiment(s: S, fittingDF: DataFrame, transformingDF: DataFrame): DataFrame = {
    s match {
      case t: Transformer =>
        t.transform(transformingDF)
      case e: Estimator[_] =>
        e.fit(fittingDF).transform(transformingDF)
      case _ => throw new MatchError(s"$s is not a Transformer or Estimator")
    }
  }

  def testExperiments(): Unit = {
    experimentTestObjects().foreach { req =>
      val res = runExperiment(req.stage, req.fitDF, req.transDF)
      req.validateDF match {
        case Some(vdf) => assertDFEq(res, vdf)
        case None => ()
      }
    }
  }

  test("Experiment Fuzzing") {
    testExperiments()
  }

}

trait SerializationFuzzing[S <: PipelineStage with MLWritable] extends TestBase with DataFrameEquality {
  def serializationTestObjects(): Seq[TestObject[S]]

  def reader: MLReadable[_]

  def modelReader: MLReadable[_]

  val useShm: Boolean = sys.env.getOrElse("MMLSPARK_TEST_SHM", "false").toBoolean

  lazy val savePath: String = {
    if (useShm) {
      val f = new File(s"/dev/shm/SavedModels-${System.currentTimeMillis()}")
      f.mkdir()
      f.toString
    } else {
      Files.createTempDirectory("SavedModels-").toString
    }
  }

  val ignoreEstimators: Boolean = false

  private def testSerializationHelper(path: String,
                                      stage: PipelineStage with MLWritable,
                                      reader: MLReadable[_],
                                      fitDF: DataFrame, transDF: DataFrame): Unit = {
    try {
      stage.write.overwrite().save(path)
      assert(new File(path).exists())
      val loadedStage = reader.load(path)
      (stage, loadedStage) match {
        case (e1: Estimator[_], e2: Estimator[_]) =>
          val df1 = e1.fit(fitDF).transform(transDF)
          val df2 = e2.fit(fitDF).transform(transDF)
          assertDFEq(df1, df2)
        case (t1: Transformer, t2: Transformer) =>
          val df1 = t1.transform(transDF)
          val df2 = t2.transform(transDF)
          assertDFEq(df1, df2)
        case _ => throw new IllegalArgumentException(s"$stage and $loadedStage do not have proper types")
      }
      ()
    } finally {
      if (new File(path).exists()) FileUtils.forceDelete(new File(path))
    }
  }

  def testSerialization(): Unit = {
    try {
      serializationTestObjects().foreach { req =>
        val fitStage = req.stage match {
          case stage: Estimator[_] =>
            if (!ignoreEstimators) {
              testSerializationHelper(savePath + "/stage", stage, reader, req.fitDF, req.transDF)
            }
            stage.fit(req.fitDF).asInstanceOf[PipelineStage with MLWritable]
          case stage: Transformer => stage
          case s => throw new IllegalArgumentException(s"$s does not have correct type")
        }
        testSerializationHelper(savePath + "/fitStage", fitStage, modelReader, req.transDF, req.transDF)

        val pipe = new Pipeline().setStages(Array(req.stage.asInstanceOf[PipelineStage]))
        if (!ignoreEstimators) {
          testSerializationHelper(savePath + "/pipe", pipe, Pipeline, req.fitDF, req.transDF)
        }
        val fitPipe = pipe.fit(req.fitDF)
        testSerializationHelper(savePath + "/fitPipe", fitPipe, PipelineModel, req.transDF, req.transDF)
      }
    } finally {
      if (new File(savePath).exists) FileUtils.forceDelete(new File(savePath))
    }
  }

  test("Serialization Fuzzing") {
    testSerialization()
  }

}

trait Fuzzing[S <: PipelineStage with MLWritable] extends SerializationFuzzing[S]
  with ExperimentFuzzing[S] with PyTestFuzzing[S] with DotnetTestFuzzing[S] {

  def testObjects(): Seq[TestObject[S]]

  def pyTestObjects(): Seq[TestObject[S]] = testObjects()

  def dotnetTestObjects(): Seq[TestObject[S]] = testObjects()

  def serializationTestObjects(): Seq[TestObject[S]] = testObjects()

  def experimentTestObjects(): Seq[TestObject[S]] = testObjects()

}

trait TransformerFuzzing[S <: Transformer with MLWritable] extends Fuzzing[S] {

  override val ignoreEstimators: Boolean = true

  override def modelReader: MLReadable[_] = reader

}

trait EstimatorFuzzing[S <: Estimator[_] with MLWritable] extends Fuzzing[S]
