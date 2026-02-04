// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split2

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import com.microsoft.azure.synapse.ml.io.http.HTTPSchema.string_to_response
import com.microsoft.azure.synapse.ml.io.http.{RESTHelpers, SharedSingleton}
import com.microsoft.azure.synapse.ml.io.split1.WithFreeUrl
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{FileEntity, StringEntity}
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.DistributedHTTPSourceProvider
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.io.File
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

trait HTTPTestUtils extends TestBase with WithFreeUrl {

  def waitForServer(server: StreamingQuery, maxTimeWaited: Int = 20000, checkEvery: Int = 100): Unit = {
    var waited = 0
    while (waited < maxTimeWaited) { //scalastyle:ignore while
      if (!server.isActive) throw server.exception.get
      if (server.recentProgress.length > 1) return //scalastyle:ignore return
      Thread.sleep(checkEvery.toLong)
      waited += checkEvery
    }
    throw new TimeoutException(s"Server Did not start within $maxTimeWaited ms")
  }

  def sendStringRequest(url: String = url,
                        payload: String = "foo",
                        targetCode: Int = 200): (String, Double) = {
    val post = new HttpPost(url)
    val e = new StringEntity(payload)
    post.setEntity(e)
    //println("request sent")
    val t0 = System.nanoTime()
    val res = RESTHelpers.Client.execute(post)
    val t1 = System.nanoTime()

    assert(targetCode === res.getStatusLine.getStatusCode)
    val out = if (targetCode == res.getStatusLine.getStatusCode && !targetCode.toString.startsWith("2")) {
      null //scalastyle:ignore null
    } else {
      new BasicResponseHandler().handleResponse(res)
    }
    res.close()
    //println("request succeeded")
    (out, (t1 - t0).toDouble / 1e6)
  }

  def using(c: StreamingQuery)(t: => Unit): Unit = {
    try {
      t
    } finally {
      c.stop()
      Thread.sleep(1000)
    }
  }

  def sendStringRequestAsync(url: String = url): Future[(String, Double)] = {
    Future {
      sendStringRequest(url = url)
    }(ExecutionContext.global)
  }

  def sendJsonRequest(map: Map[String, Any], url: String): String = {
    val post = new HttpPost(url)
    implicit val defaultFormats: DefaultFormats = DefaultFormats
    val params = new StringEntity(Serialization.write(map))
    post.addHeader("content-type", "application/json")
    post.setEntity(params)
    val res = RESTHelpers.Client.execute(post)
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    out
  }

  def sendJsonRequest(payload: Int, url: String): (String, Long) = {
    val post = new HttpPost(url)
    val e = new StringEntity("{\"data\":" + s"$payload}")
    post.setEntity(e)
    val t0 = System.currentTimeMillis()
    val res = RESTHelpers.Client.execute(post)
    val t1 = System.currentTimeMillis()
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    (out, t1 - t0)
  }

  def sendJsonRequestAsync(map: Map[String, Any], url: String = url): Future[String] = {
    Future {
      sendJsonRequest(map, url = url)
    }(ExecutionContext.global)
  }

  def sendFileRequest(url: String = url, client: CloseableHttpClient = RESTHelpers.Client): (String, Double) = {
    val post = new HttpPost(url)
    val e = new FileEntity(FileUtilities.join(
      BuildInfo.datasetDir, "Images", "Grocery", "testImages", "WIN_20160803_11_28_42_Pro.jpg"))
    post.setEntity(e)
    val t0 = System.nanoTime()
    val res = client.execute(post)
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    val t1 = System.nanoTime()
    (out, (t1 - t0).toDouble / 1e6)
  }

  def mean(xs: List[Int]): Double = xs match {
    case Nil => 0.0
    case ys => ys.sum / ys.size.toDouble
  }

  def median(xs: List[Int]): Double = xs match {
    case Nil => 0.0
    case ys => ys.sorted.apply(ys.length / 2)
  }

  def stddev(xs: List[Int], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt(ys.foldLeft(0.0) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  lazy val requestDuration: FiniteDuration = Duration(10, TimeUnit.SECONDS)

  lazy implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def assertLatency(responsesWithLatencies: Seq[(String, Double)], cutoff: Double): Unit = {
    val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
    //responsesWithLatencies.foreach(r => println(r._1))
    val medianLatency = median(latencies)
    val meanLatency = mean(latencies)
    val stdLatency = stddev(latencies, meanLatency)
    println(s"Median Latency = $medianLatency")
    println(s"Latency = $meanLatency +/- $stdLatency")
    assert(medianLatency < cutoff)
    ()
  }

}

// TODO add tests for shuffles
class DistributedHTTPSuite extends TestBase with Flaky with HTTPTestUtils {
  // Logger.getRootLogger.setLevel(Level.WARN)
  // Logger.getLogger(classOf[DistributedHTTPSource]).setLevel(Level.INFO)
  // Logger.getLogger(classOf[JVMSharedServer]).setLevel(Level.INFO)

  override val host = "127.0.0.1"

  def baseReaderDist: DataStreamReader = {
    spark.readStream.distributedServer
      .address(host, port, "foo")
      .option("maxPartitions", 3)
      .option("maxPortAttempts", "1")
  }

  def baseReader: DataStreamReader = {
    spark.readStream.server
      .address(host, port, "foo")
      .option("maxPartitions", 3)
      .option("maxPortAttempts", "1")
  }

  def baseWriterDist(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      .distributedServer
      .option("name", "foo")
      .option("maxPortAttempts", "1")
      .queryName("foo")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
  }

  def baseWriter(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      .server
      .option("name", "foo")
      .option("maxPortAttempts", "1")
      .queryName("foo")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
  }

  def createServer(): DataStreamWriter[Row] = {
    println(classOf[DistributedHTTPSourceProvider].getName)
    baseWriterDist(baseReaderDist
      .load()
      .withColumn("contentLength", col("request.entity.contentLength"))
      .withColumn("reply", string_to_response(col("contentLength").cast(StringType))))
  }

  test("standard client") {
    val server = createServer().start()
    using(server) {
      waitForServer(server)
      val responses = List(
        sendJsonRequest(Map("foo" -> 1, "bar" -> "here"), url),
        sendJsonRequest(Map("foo" -> 2, "bar" -> "heree"), url),
        sendJsonRequest(Map("foo" -> 3, "bar" -> "hereee"), url),
        sendJsonRequest(Map("foo" -> 4, "bar" -> "hereeee"), url)
      )
      val correctResponses = List(22, 23, 24, 25).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(_ => sendJsonRequest(Map("foo" -> 1, "bar" -> "here"), url))
        .foreach(resp => assert(resp === "22"))
    }
  }

  test("test implicits") {
    val server = baseWriter(baseReader
      .load()
      .withColumn("contentLength", col("request.entity.contentLength"))
      .withColumn("reply", string_to_response(col("contentLength").cast(StringType))))
      .start()

    using(server) {
      waitForServer(server)
      val responses = List(
        sendJsonRequest(Map("foo" -> 1, "bar" -> "here"), url),
        sendJsonRequest(Map("foo" -> 2, "bar" -> "heree"), url),
        sendJsonRequest(Map("foo" -> 3, "bar" -> "hereee"), url),
        sendJsonRequest(Map("foo" -> 4, "bar" -> "hereeee"), url)
      )
      val correctResponses = List(22, 23, 24, 25).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(_ => sendJsonRequest(Map("foo" -> 1, "bar" -> "here"), url))
        .foreach(resp => assert(resp === "22"))
    }

  }

  test("test implicits 2") {

    val server = baseWriter(baseReader
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length"))
      .start()

    using(server) {
      waitForServer(server)
      val responsesWithLatencies = (1 to 10).map(_ => sendFileRequest())

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")

      responsesWithLatencies.foreach(s => assert(s._1 === "{\"length\":279186}"))
    }
  }

  test("test implicits 2 distributed") {

    val server = baseWriterDist(baseReaderDist
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length"))
      .start()

    using(server) {
      waitForServer(server)
      val responsesWithLatencies = (1 to 10).map(_ =>
        sendFileRequest()
      )

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")

      responsesWithLatencies.foreach(s => assert(s._1 === "{\"length\":279186}"))
    }

  }

  test("python client") {
    val testStartTime = System.currentTimeMillis()
    println(s"[DIAG] python client test starting at $testStartTime")
    println(s"[DIAG] target URL: $url (host=$host, port=$port)")

    val server = createServer().start()

    using(server) {
      waitForServer(server)
      println(s"[DIAG] server ready after ${System.currentTimeMillis() - testStartTime}ms")
      println(s"[DIAG] server recentProgress: ${server.recentProgress.length} entries")

      // Verify server is reachable with a quick Scala HTTP request before launching Python
      println(s"[DIAG] verifying server connectivity from Scala...")
      val connectivityCheckStart = System.currentTimeMillis()
      try {
        val (scalaResponse, scalaLatency) = sendStringRequest(url, "connectivity-check", 200)
        println(s"[DIAG] Scala connectivity check PASSED in ${scalaLatency}ms, " +
          s"response length=${scalaResponse.length}")
      } catch {
        case e: Exception =>
          val elapsed = System.currentTimeMillis() - connectivityCheckStart
          println(s"[DIAG] Scala connectivity check FAILED after ${elapsed}ms: ${e.getClass.getName}: ${e.getMessage}")
          println(s"[DIAG] This indicates the server is not reachable at $url")
          throw new RuntimeException(s"Server connectivity check failed for $url", e)
      }

      val pythonClientCode =
        s"""import requests
           |import threading
           |import time
           |import sys
           |import socket
           |
           |TARGET_URL = "$url"
           |print(f"[DIAG] Python client targeting URL: {TARGET_URL}", flush=True)
           |print(f"[DIAG] Python version: {sys.version}", flush=True)
           |
           |# Quick connectivity check
           |try:
           |    host, port_str = TARGET_URL.replace("http://", "").split("/")[0].split(":")
           |    port = int(port_str)
           |    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
           |    sock.settimeout(5)
           |    result = sock.connect_ex((host, port))
           |    sock.close()
           |    if result == 0:
           |        print(f"[DIAG] Socket connectivity check PASSED for {host}:{port}", flush=True)
           |    else:
           |        print(f"[DIAG] Socket connectivity check FAILED for {host}:{port}, error code={result}", flush=True)
           |except Exception as e:
           |    print(f"[DIAG] Socket connectivity check ERROR: {e}", flush=True)
           |
           |start_time = time.time()
           |s = requests.Session()
           |
           |class myThread(threading.Thread):
           |    def __init__(self, threadID):
           |        threading.Thread.__init__(self)
           |        self.threadID = threadID
           |        self.success = False
           |        self.error = None
           |
           |    def run(self):
           |        thread_start = time.time()
           |        print(f"[DIAG] Thread {self.threadID} starting at {thread_start - start_time:.2f}s", flush=True)
           |        try:
           |            r = s.post(TARGET_URL,
           |                       data={"number": 12524, "type": "issue", "action": "show"},
           |                       headers={"content-type": "application/json"},
           |                       timeout=120)
           |            elapsed = time.time() - thread_start
           |            print(f"[DIAG] Thread {self.threadID} done {elapsed:.2f}s status={r.status_code}", flush=True)
           |            assert r.status_code == 200
           |            self.success = True
           |        except requests.exceptions.Timeout as e:
           |            elapsed = time.time() - thread_start
           |            print(f"[DIAG] Thread {self.threadID} TIMEOUT after {elapsed:.2f}s: {e}", flush=True)
           |            self.error = f"TIMEOUT: {e}"
           |            raise
           |        except requests.exceptions.ConnectionError as e:
           |            elapsed = time.time() - thread_start
           |            print(f"[DIAG] Thread {self.threadID} CONN_ERR after {elapsed:.2f}s: {e}", flush=True)
           |            self.error = f"CONNECTION_ERROR: {e}"
           |            raise
           |        except Exception as e:
           |            elapsed = time.time() - thread_start
           |            print(f"[DIAG] Thread {self.threadID} FAILED {elapsed:.2f}s: {type(e).__name__}", flush=True)
           |            self.error = str(e)
           |            raise
           |
           |threads = []
           |for i in range(2):
           |    t = myThread(i)
           |    t.start()
           |    threads.append(t)
           |
           |# Wait for all threads to complete
           |for t in threads:
           |    t.join()
           |
           |total_time = time.time() - start_time
           |success_count = sum(1 for t in threads if t.success)
           |print(f"[DIAG] All threads completed in {total_time:.2f}s, {success_count}/2 succeeded", flush=True)
           |""".stripMargin

      val pythonFile = new File(tmpDir.toFile, "pythonClient.py")
      FileUtilities.writeFile(pythonFile, pythonClientCode)

      val pipInstall = Runtime.getRuntime.exec("pip install requests")
      pipInstall.waitFor()

      println(s"[DIAG] launching 10 Python processes at ${System.currentTimeMillis() - testStartTime}ms")

      val processes = (1 to 10).map { i =>
        val p = Runtime.getRuntime.exec(s"python ${pythonFile.getAbsolutePath}")
        println(s"[DIAG] launched process $i")
        p
      }

      var processIdx = 0
      processes.foreach { p =>
        processIdx += 1
        val procStartWait = System.currentTimeMillis()
        p.waitFor()
        val procWaitTime = System.currentTimeMillis() - procStartWait

        val stdout = IOUtils.toString(p.getInputStream, "UTF-8")
        val error = IOUtils.toString(p.getErrorStream, "UTF-8")

        println(s"[DIAG] process $processIdx completed in ${procWaitTime}ms, exitCode=${p.exitValue()}")
        if (stdout.nonEmpty) println(s"[DIAG] process $processIdx stdout:\n$stdout")
        if (error.nonEmpty) println(s"[DIAG] process $processIdx stderr:\n$error")

        if (error.nonEmpty) {
          val totalElapsed = System.currentTimeMillis() - testStartTime
          println(s"[DIAG] *** FAILURE DETECTED in process $processIdx ***")
          println(s"[DIAG] Total test time so far: ${totalElapsed}ms")
          println(s"[DIAG] Server state: isActive=${server.isActive}, " +
            s"recentProgress=${server.recentProgress.length}")
          if (server.recentProgress.nonEmpty) {
            val lastProgress = server.recentProgress.last
            println(s"[DIAG] Last progress: numInputRows=${lastProgress.numInputRows}, " +
              s"inputRowsPerSecond=${lastProgress.inputRowsPerSecond}, " +
              s"processedRowsPerSecond=${lastProgress.processedRowsPerSecond}")
          }
          if (server.exception.isDefined) {
            println(s"[DIAG] Server exception: ${server.exception.get}")
          }
          println(s"[DIAG] Full stderr from process $processIdx:")
          println(error)
          println(s"[DIAG] *** END FAILURE INFO ***")
        }
        assert(error === "", s"Process $processIdx failed with error: $error")
      }

      println(s"[DIAG] all processes completed after ${System.currentTimeMillis() - testStartTime}ms")
    }

  }

  test("async client") {
    val server = createServer().start()

    using(server) {
      waitForServer(server)

      val futures = List(
        sendJsonRequestAsync(Map("foo" -> 1, "bar" -> "here")),
        sendJsonRequestAsync(Map("foo" -> 2, "bar" -> "heree")),
        sendJsonRequestAsync(Map("foo" -> 3, "bar" -> "hereee")),
        sendJsonRequestAsync(Map("foo" -> 4, "bar" -> "hereeee"))
      )

      val responses = futures.map(f => Await.result(f, Duration.Inf))
      val correctResponses = List(22, 23, 24, 25).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(_ => sendJsonRequestAsync(Map("foo" -> 1, "bar" -> "here")))
        .foreach { f =>
          val resp = Await.result(f, Duration(10, TimeUnit.SECONDS))
          assert(resp === "22")
        }
    }
  }

  test("State can be saved in a Shared singleton") {
    object Holder extends Serializable {

      class FooHolder {
        var state = 0

        def increment(): Unit = synchronized {
          state += 1
        }
      }

      val Foo = SharedSingleton {
        new FooHolder
      }

      import spark.implicits._

      val DF: DataFrame = spark.sparkContext.parallelize(Seq(Tuple1("placeholder")))
        .toDF("plcaholder")
        .mapPartitions { _ =>
          Foo.get.increment()
          Iterator(Row(Foo.get.state))
        }(ExpressionEncoder(new StructType().add("state", IntegerType))).cache()
      val States1: Array[Row] = DF.collect()

      val DF2: DataFrame = DF.mapPartitions { _ =>
        Iterator(Row(Foo.get.state))
      }(ExpressionEncoder(new StructType().add("state", IntegerType)))
      val States2: Array[Row] = DF2.collect()
      assert(States2.forall(_.getInt(0) === States2.length))
    }
  }

}
