// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData}
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding
import org.apache.http.impl.client.CloseableHttpClient

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, ExecutionException, Executors, TimeUnit}

private[contentunderstanding] object ContentUnderstandingCancellationProbe {
  val Finished = new ConcurrentHashMap[String, CountDownLatch]()
}

private[contentunderstanding] class TestableContentUnderstandingCancellation(override val uid: String)
  extends ContentUnderstanding(uid) {

  override protected def handlingFunc(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData = {
    try {
      super.handlingFunc(client, request)
    } finally {
      ContentUnderstandingCancellationProbe.Finished.get(uid).countDown()
    }
  }
}

class ContentUnderstandingCancellationSuite extends TestBase {
  test("Spark cancellation and concurrentTimeout stop throttling waits in every HTTP worker") {
    Seq((1, false), (2, false), (2, true)).foreach { case (concurrency, expireTimeout) =>
      val entered = new CountDownLatch(concurrency)
      val finished = new CountDownLatch(concurrency)
      val id = "cu-cancel-" + UUID.randomUUID().toString
      val server = new ContentUnderstandingStub(_ => {
        entered.countDown()
        ContentUnderstandingStubReply(429, """{"error":{"code":"TooManyRequests"}}""",
          Map("Retry-After" -> "120"))
      })
      val executor = Executors.newSingleThreadExecutor()
      val context = spark.sparkContext
      val input = spark.range(concurrency).coalesce(1)
      val stage = new TestableContentUnderstandingCancellation(id).setEndpoint(server.endpoint)
        .setDocumentBytes(Array[Byte](1)).setConcurrency(concurrency).setOperationMode("submit")
      if (expireTimeout) stage.setConcurrentTimeout(5.0)
      ContentUnderstandingCancellationProbe.Finished.put(id, finished)
      val action = executor.submit(new Runnable {
        override def run(): Unit = {
          context.setJobGroup(id, "Cancel Content Understanding retries", interruptOnCancel = true)
          try {
            stage.transform(input).collect()
          } finally {
            context.clearJobGroup()
          }
        }
      })
      try {
        assert(entered.await(120, TimeUnit.SECONDS), "Expected all configured workers to reach the service")
        assert(!finished.await(200, TimeUnit.MILLISECONDS), "A 429 should wait rather than return Rejected")
        if (!expireTimeout) context.cancelJobGroup(id)
        assert(finished.await(30, TimeUnit.SECONDS), "Cancelled HTTP workers must not remain in retry loops")
        intercept[ExecutionException](action.get(30, TimeUnit.SECONDS))
        assert(server.requests.size == concurrency)
      } finally {
        context.cancelJobGroup(id)
        action.cancel(true)
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
        server.close()
        ContentUnderstandingCancellationProbe.Finished.remove(id)
      }
    }
  }

  test("Retry-After values longer than a minute are not shortened") {
    val protocolClass = Class.forName(
      "com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstandingProtocol$")
    val protocol = protocolClass.getField("MODULE$").get(None.orNull)
    val retryDelay = protocolClass.getMethod("retryDelay", classOf[Option[_]], java.lang.Integer.TYPE)
    assert(retryDelay.invoke(protocol, Some("120"), Int.box(1)) == Long.box(120000L))
    val future = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).plusMinutes(3)
      .format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
    val actual = retryDelay.invoke(protocol, Some(future), Int.box(1)).asInstanceOf[Long]
    assert(actual > 120000L && actual <= 180000L)
    assert(retryDelay.invoke(protocol, Some(Long.MaxValue.toString), Int.box(1)).asInstanceOf[Long] > 0L)
  }
}
