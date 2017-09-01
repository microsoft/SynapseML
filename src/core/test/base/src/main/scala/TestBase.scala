// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.nio.file.Files

import org.apache.spark._
import org.apache.spark.ml._
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.{DataFrame, _}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.DenseVector
import org.scalactic.{Equality, TolerantNumerics}
import org.scalactic.source.Position
import org.scalatest._

import scala.reflect.ClassTag

// Common test tags
object TestBase {

  object Extended extends Tag("com.microsoft.ml.spark.test.tags.extended")

  object LinuxOnly extends Tag("com.microsoft.ml.spark.test.tags.linuxonly")

}

trait LinuxOnly extends TestBase {
  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit =
    super.test(testName, testTags.toList.::(TestBase.LinuxOnly): _*)(testFun)
}

abstract class TestBase extends FunSuite with BeforeAndAfterEachTestData with BeforeAndAfterAll {

  println(s"\n>>>-------------------- $this --------------------<<<")

  // "This Is A Bad Thing" according to my research. However, this is
  // just for tests so maybe ok. A better design would be to break the
  // session stuff into TestSparkSession as a trait and have test suites
  // that need it "with TestSparkSession" instead, but that's a lot of
  // changes right now and maybe not desired.
  private var sessionInitialized = false
  protected lazy val session: SparkSession = {
    info(s"Creating a spark session for suite $this")
    sessionInitialized = true
    SparkSessionFactory
      .getSession(s"$this", logLevel = "WARN")
  }

  protected lazy val sc: SparkContext = session.sparkContext
  protected lazy val dir = SparkSessionFactory.workingDir

  protected def normalizePath(path: String) = SparkSessionFactory.customNormalize(path)

  // Timing info
  var suiteElapsed: Long = 0
  var testStart: Long = 0
  var testElapsed: Long = 0

  // Test Fixture Overrides
  protected override def beforeEach(td: TestData): Unit = {
    testStart = System.currentTimeMillis
    testElapsed = 0
    super.beforeEach(td)
  }

  protected override def afterEach(td: TestData): Unit = {
    try {
      super.afterEach(td)
    }
    finally {
      testElapsed = System.currentTimeMillis - testStart
      logTime(s"Test ${td.name}", testElapsed, 3000)
      suiteElapsed += testElapsed
    }
  }

  protected override def beforeAll(): Unit = {
    if (sessionInitialized) {
      info(s"Parallelism: ${session.sparkContext.defaultParallelism.toString}")
    }
    suiteElapsed = 0
  }

  protected override def afterAll(): Unit = {
    logTime(s"Suite $this", suiteElapsed, 10000)
    if (sessionInitialized) {
      info("Shutting down spark session")
      session.stop()
    }
  }

  // Utilities

  def withoutLogging[T](e: => T): T = {
    // This should really keep the old level, but there is no sc.getLogLevel, so
    // take the cheap way out for now: just use "WARN", and do something proper
    // when/if needed
    sc.setLogLevel("OFF")
    try e finally sc.setLogLevel("WARN")
  }

  def interceptWithoutLogging[E <: Exception: ClassTag](e: => Any): Unit = {
    withoutLogging { intercept[E] { e }; () }
  }

  def assertSparkException[E <: Exception: ClassTag](stage: PipelineStage, data: DataFrame): Unit = {
    withoutLogging {
      intercept[E] {
        val transformer = stage match {
            case e: Estimator[_] => e.fit(data)
            case t: Transformer  => t
            case _ => sys.error(s"Unknown PipelineStage value: $stage")
          }
        // use .length to force the pipeline (.count might work, but maybe it's sometimes optimized)
        transformer.transform(data).foreach { r => r.length; () }
      }
      ()
    }
  }

  import session.implicits._

  def makeBasicDF(): DataFrame = {
    val df = Seq(
      (0, "guitars", "drums"),
      (1, "piano", "trumpet"),
      (2, "bass", "cymbals")).toDF("numbers", "words", "more")
    df
  }

  def makeBasicNullableDF(): DataFrame = {
    val df = Seq(
      (0, 2.5, "guitars", "drums"),
      (1, Double.NaN, "piano", "trumpet"),
      (2, 8.9, "bass", null)).toDF("indices", "numbers","words", "more")
    df
  }

  def verifyResult(expected: DataFrame, result: DataFrame): Boolean = {
    assert(expected.count == result.count)
    assert(expected.schema.length == result.schema.length)
    (expected.columns zip result.columns).forall{ case (x,y) => x == y }
  }

  def time[R](block: => R): R = {
    val t0     = System.nanoTime()
    val result = block
    val t1     = System.nanoTime()
    println(s"Elapsed time: ${(t1 - t0) / 1e9} sec")
    result
  }

  private def logTime(name: String, time: Long, threshold: Long) = {
    val msg = s"$name took ${time / 1000.0}s"
    if (time > threshold) {
      alert(msg)
    } else {
      info(msg)
    }
  }

}

trait RoundTripTestBase extends TestBase {

  val dfRoundTrip: DataFrame

  val reader: MLReadable[_]

  val modelReader: MLReadable[_]

  val stageRoundTrip: PipelineStage with MLWritable

  val savePath: String = Files.createTempDirectory("SavedModels-").toString

  val epsilon = 1e-4
  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)
  implicit val dvEq: Equality[DenseVector] = new Equality[DenseVector]{
    def areEqual(a: DenseVector, b: Any): Boolean = b match {
      case bArr:DenseVector =>
        a.values.zip(bArr.values).forall {case (x,y) => doubleEq.areEqual(x,y)}
    }
  }

  private def assertDataFrameEq(a: DataFrame, b: DataFrame): Unit ={
    assert(a.columns === b.columns)
    val aSort = a.collect()
    val bSort = b.collect()
    assert(aSort.length == bSort.length)
    aSort.zip(bSort).zipWithIndex.foreach {case ((rowA, rowB), i) =>
      a.columns.indices.foreach(j =>{
        rowA(j) match {
          case lhs: DenseVector =>
            assert(lhs === rowB(j), s"row $i column $j not equal")
          case lhs =>
            assert(lhs === rowB(j), s"row $i column $j not equal")
        }
      })
    }
  }

  private def testRoundTripHelper(path: String,
                                  stage: PipelineStage with MLWritable,
                                  reader: MLReadable[_], df: DataFrame): Unit = {
    stage.write.overwrite().save(path)
    val loadedStage = reader.load(path)
    (stage, loadedStage) match {
      case (e1: Estimator[_], e2: Estimator[_]) =>
        assertDataFrameEq(e1.fit(df).transform(df), e2.fit(df).transform(df))
      case (t1: Transformer, t2: Transformer) =>
        assertDataFrameEq(t1.transform(df), t2.transform(df))
      case _ => throw new IllegalArgumentException(s"$stage and $loadedStage do not have proper types")
    }
    ()
  }

  def testRoundTrip(ignoreEstimators: Boolean = false): Unit = {
    val fitStage = stageRoundTrip match {
      case stage: Estimator[_] =>
        if (!ignoreEstimators) {
          testRoundTripHelper(savePath + "/stage", stage, reader, dfRoundTrip)
        }
        stage.fit(dfRoundTrip).asInstanceOf[PipelineStage with MLWritable]
      case stage: Transformer => stage
      case s => throw new IllegalArgumentException(s"$s does not have correct type")
    }
    testRoundTripHelper(savePath + "/fitStage", fitStage, modelReader, dfRoundTrip)
    val pipe = new Pipeline().setStages(Array(stageRoundTrip))
    if (!ignoreEstimators) {
      testRoundTripHelper(savePath + "/pipe", pipe, Pipeline, dfRoundTrip)
    }
    val fitPipe = pipe.fit(dfRoundTrip)
    testRoundTripHelper(savePath + "/fitPipe", fitPipe, PipelineModel, dfRoundTrip)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new java.io.File(savePath))
    super.afterAll()
  }

}
