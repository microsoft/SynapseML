// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.base

import breeze.linalg.norm.Impl
import breeze.linalg.{*, norm, DenseMatrix => BDM, DenseVector => BDV}
import breeze.math.Field
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark._
import org.apache.spark.ml._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.streaming.{StreamingContext, Seconds => SparkSeconds}
import org.scalactic.Equality
import org.scalactic.source.Position
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.nio.file.{Files, Path}
import scala.concurrent._
import scala.reflect.ClassTag

trait SparkSessionManagement {

  def currentDir: String = System.getProperty("user.dir")

  // Default spark warehouse = ./spark-warehouse
  private val defaultWarehouseDirName = "spark-warehouse"
  private val testDir = System.currentTimeMillis.toString

  private lazy val localWarehousePath =
    "file:" + customNormalize(new File(currentDir, defaultWarehouseDirName).getAbsolutePath)
  lazy val workingDir: String = "file:" + customNormalize(new File(currentDir, testDir).getAbsolutePath)

  // On NTFS-like systems, normalize path
  //   (solves the problem of sending a path from spark to hdfs on Windows)
  def customNormalize(path: String): String = {
    if (File.separator != "\\") path
    else path.replaceFirst("[A-Z]:", "").replace("\\", "/")
  }

  def sparkConfiguration: SparkConf = {
    new SparkConf()
      .set("spark.logConf", "true")
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.driver.maxResultSize", "6g")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.warehouse.dir", localWarehousePath)
  }

  def getSession(name: String,
                 logLevel: String = "WARN",
                 numRetries: Int = 1,
                 numCores: Option[Int] = None): SparkSession = {
    val cores = numCores.map(_.toString).getOrElse("*")
    val master = if (numRetries == 1) {
      s"local[$cores]"
    } else {
      s"local[$cores, $numRetries]"
    }
    val sess = SparkSession.builder()
      .appName(name)
      .master(master)
      .config(sparkConfiguration)
      .getOrCreate()
    sess.sparkContext.setLogLevel(logLevel)
    sess.sparkContext.hadoopConfiguration
      .setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])
    sess
  }

  def sc: SparkContext = spark.sparkContext

  def ssc: StreamingContext = new StreamingContext(sc, SparkSeconds(1))

  private var sparkInternal: Option[SparkSession] = None

  def resetSparkSession(numRetries: Int = 1, logLevel: String="WARN", numCores: Option[Int] = None): Unit = {
    sparkInternal.foreach(_.close())
    sparkInternal = Some(getSession(s"$this", logLevel = logLevel, numRetries = numRetries, numCores = numCores)
    )
  }

  def stopSparkSession(): Unit = {
    sparkInternal.foreach { spark =>
      spark.close()
      Thread.sleep(1000) // TODO figure out if/why this is needed to give spark a chance to stop
    }
    sparkInternal = None
  }

  def spark: SparkSession = {
    if (sparkInternal.isEmpty) {
      resetSparkSession()
    }
    sparkInternal.get
  }

}

trait LinuxOnly extends TestBase {
  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit =
    super.test(testName, testTags.toList.::(TestBase.LinuxOnly): _*)(testFun)
}

trait Flaky extends TestBase {

  val retryMillis: Array[Int] = Array(0, 1000, 5000)

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      tryWithRetries(retryMillis)(testFun _)
    }
  }

}

trait TimeLimitedFlaky extends TestBase with TimeLimits {

  val timeoutInSeconds: Int = 5 * 60

  val retryMillis: Array[Int] = Array(0, 100, 100)

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      tryWithRetries(retryMillis) {
        failAfter(Span(timeoutInSeconds, Seconds)) {
          println("Executing time-limited flaky function")
          testFun _
        }
      }
    }
  }

}

// Common test tags
object TestBase extends SparkSessionManagement {

  // Run only on Linux
  object LinuxOnly extends Tag("com.microsoft.azure.synapse.ml.test.tags.linuxonly")

}

abstract class TestBase extends AnyFunSuite with BeforeAndAfterEachTestData with BeforeAndAfterAll {

  lazy val sparkProvider: SparkSessionManagement = TestBase

  lazy val spark: SparkSession = sparkProvider.spark
  lazy val sc: SparkContext = sparkProvider.sc
  lazy val ssc: StreamingContext = sparkProvider.ssc

  private var tmpDirCreated = false
  protected lazy val tmpDir: Path = {
    tmpDirCreated = true
    Files.createTempDirectory("MML-Test-")
  }

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
    System.setProperty("log4j1.compatibility", "true")
    suiteElapsed = 0
  }

  protected override def afterAll(): Unit = {
    logTime(s"Suite $this", suiteElapsed, 10000)
    if (tmpDirCreated && tmpDir.toFile.exists) {
        FileUtils.forceDelete(tmpDir.toFile)
    }
  }

  // Utilities

  def tryWithRetries[T](times: Array[Int] = Array(0, 100, 500, 1000, 3000, 5000))(block: () => T): T = {
    for ((t, i) <- times.zipWithIndex) {
      try {
        return block()  //scalastyle:ignore return
      } catch {
        case e: Exception if (i + 1) < times.length =>
          println(s"RETRYING after $t ms:  Caught error: $e ")
          blocking {
            Thread.sleep(t.toLong)
          }
      }
    }
    throw new RuntimeException("This error should not occur, bug has been introduced in tryWithRetries")
  }

  def withoutLogging[T](e: => T): T = {
    // This should really keep the old level, but there is no sc.getLogLevel, so
    // take the cheap way out for now: just use "WARN", and do something proper
    // when/if needed
    sc.setLogLevel("OFF")
    try e finally sc.setLogLevel("WARN")
  }

  def interceptWithoutLogging[E <: Throwable: ClassTag](e: => Any): Unit = {
    withoutLogging {
      intercept[E] {
        e
      }
      ()
    }
  }

  def assertSparkException[E <: Throwable: ClassTag](stage: PipelineStage, data: DataFrame): Unit = {
    withoutLogging {
      intercept[E] {
        val transformer = stage match {
          case e: Estimator[_] => e.fit(data)
          case t: Transformer => t
          case _ => sys.error(s"Unknown PipelineStage value: $stage")
        }
        transformer.transform(data).foreachPartition { it: Iterator[Row] => it.toList; () }
      }
      ()
    }
  }

  import spark.implicits._

  def makeBasicDF(): DataFrame = {
    Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans")
  }

  def makeBasicNullableDF(): DataFrame = {
    Seq(
      (0, 2.5, "guitars", Some("drums"), Some(2.toLong), None),
      (1, Double.NaN, "piano", Some("trumpet"), Some(1.toLong), Some(true)),
      (2, 8.9, "bass", None, None, Some(false)))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans")
  }

  def verifyResult(expected: DataFrame, result: DataFrame): Boolean = {
    assert(expected.count === result.count)
    assert(expected.schema.length == result.schema.length)
    (expected.columns zip result.columns).forall { case (x, y) => x == y }
  }

  def time[R](block: => R): R = {
    val (result, t) = getTime(block)
    println(s"Elapsed time: ${t / 1e9} sec")
    result
  }

  def getTime[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    (result, t1 - t0)
  }

  def getTime[R](n: Int)(block: => R): (Seq[R], Long) = {
    val t0 = System.nanoTime()
    val results = (1 to n).map(_ => block)
    val t1 = System.nanoTime()
    (results, (t1 - t0) / n)
  }

  private def logTime(name: String, time: Long, threshold: Long): Unit = {
    val msg = s"$name took ${time / 1000.0}s"
    if (time > threshold) {
      alert(msg)
    } else {
      info(msg)
    }
  }

  def breezeVectorEq[T: Field](tol: Double)(implicit normImpl: Impl[T, Double]): Equality[BDV[T]] =
    (a: BDV[T], b: Any) => {
      b match {
        case p: BDV[T@unchecked] =>
          a.length == p.length && norm(a - p) < tol
        case _ => false
      }
    }

  def breezeMatrixEq[T: Field](tol: Double)(implicit normImpl: Impl[T, Double]): Equality[BDM[T]] =
    (a: BDM[T], b: Any) => {
      b match {
        case p: BDM[T@unchecked] =>
          a.rows == p.rows && a.cols == p.cols && {
            ((a(*, ::).iterator) zip (p(*, ::).iterator)).forall {
              case (v1: BDV[T], v2: BDV[T]) =>
                // Row-wise comparison
                breezeVectorEq(tol).areEquivalent(v1, v2)
            }
          }
        case _ => false
      }
    }

  def mapEq[K, V: Equality]: Equality[Map[K, V]] = {
    (a: Map[K, V], b: Any) => {
      b match {
        case m: Map[K@unchecked, V@unchecked] => a.keySet == m.keySet && a.keySet.forall(key => a(key) === m(key))
        case _ => false
      }
    }
  }
}
