// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class VerifyClients extends TestBase {

  // Test SingleThreadedClient
  private class TestSingleThreadedClient extends SingleThreadedClient {
    override protected type Client = Unit
    override protected type ResponseType = String
    override protected type RequestType = String
    override protected val internalClient: Client = ()

    override protected def sendRequestWithContext(
        request: RequestWithContext): ResponseWithContext = {
      request.request match {
        case Some(req) => ResponseWithContext(Some(s"response-$req"), request.context)
        case None => ResponseWithContext(None, request.context)
      }
    }
  }

  test("SingleThreadedClient processes requests sequentially") {
    val client = new TestSingleThreadedClient
    val requests = Iterator(
      client.RequestWithContext(Some("a"), Some("ctx-a")),
      client.RequestWithContext(Some("b"), Some("ctx-b")),
      client.RequestWithContext(Some("c"), Some("ctx-c"))
    )

    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.length === 3)
    assert(responses(0).response === Some("response-a"))
    assert(responses(0).context === Some("ctx-a"))
    assert(responses(1).response === Some("response-b"))
    assert(responses(2).response === Some("response-c"))
  }

  test("SingleThreadedClient handles empty requests") {
    val client = new TestSingleThreadedClient
    val requests = Iterator(
      client.RequestWithContext(None, Some("ctx"))
    )

    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.length === 1)
    assert(responses.head.response === None)
    assert(responses.head.context === Some("ctx"))
  }

  test("SingleThreadedClient handles empty iterator") {
    val client = new TestSingleThreadedClient
    val requests = Iterator.empty.asInstanceOf[Iterator[client.RequestWithContext]]
    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.isEmpty)
  }

  // Test AsyncClient
  private class TestAsyncClient(conc: Int, to: Duration)
                               (implicit ec: ExecutionContext)
    extends AsyncClient(conc, to) {

    override protected type Client = Unit
    override protected type ResponseType = String
    override protected type RequestType = String
    override protected val internalClient: Client = ()

    override protected def sendRequestWithContext(
        request: RequestWithContext): ResponseWithContext = {
      request.request match {
        case Some(req) =>
          // Simulate some work
          Thread.sleep(10)
          ResponseWithContext(Some(s"async-$req"), request.context)
        case None =>
          ResponseWithContext(None, request.context)
      }
    }
  }

  test("AsyncClient processes requests concurrently") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new TestAsyncClient(4, 30.seconds)

    val requests = Iterator(
      client.RequestWithContext(Some("1"), None),
      client.RequestWithContext(Some("2"), None),
      client.RequestWithContext(Some("3"), None),
      client.RequestWithContext(Some("4"), None)
    )

    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.length === 4)
    assert(responses.map(_.response).forall(_.isDefined))
    assert(responses.flatMap(_.response).toSet === Set("async-1", "async-2", "async-3", "async-4"))
  }

  test("AsyncClient preserves context") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new TestAsyncClient(2, 30.seconds)

    val requests = Iterator(
      client.RequestWithContext(Some("a"), Some("context-a")),
      client.RequestWithContext(Some("b"), Some("context-b"))
    )

    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.length === 2)
    // Note: order may vary due to concurrency, but contexts should match requests
    responses.foreach { resp =>
      resp.response match {
        case Some("async-a") => assert(resp.context === Some("context-a"))
        case Some("async-b") => assert(resp.context === Some("context-b"))
        case _ => fail("Unexpected response")
      }
    }
  }

  test("AsyncClient handles empty requests") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new TestAsyncClient(2, 30.seconds)

    val requests = Iterator(
      client.RequestWithContext(None, Some("ctx"))
    )

    val responses = client.sendRequestsWithContext(requests).toList
    assert(responses.length === 1)
    assert(responses.head.response === None)
  }

  test("AsyncClient respects concurrency parameter") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new TestAsyncClient(2, 30.seconds)
    assert(client.concurrency === 2)
  }

  test("AsyncClient respects timeout parameter") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new TestAsyncClient(2, 45.seconds)
    assert(client.timeout === 45.seconds)
  }

  // Test RequestWithContext and ResponseWithContext
  test("RequestWithContext can be created with request only") {
    val client = new TestSingleThreadedClient
    val req = new client.RequestWithContext(Some("test"))
    assert(req.request === Some("test"))
    assert(req.context === None)
  }

  test("RequestWithContext can be created with request and context") {
    val client = new TestSingleThreadedClient
    val req = client.RequestWithContext(Some("test"), Some("ctx"))
    assert(req.request === Some("test"))
    assert(req.context === Some("ctx"))
  }

  test("ResponseWithContext can be created with response only") {
    val client = new TestSingleThreadedClient
    val resp = new client.ResponseWithContext(Some("test"))
    assert(resp.response === Some("test"))
    assert(resp.context === None)
  }

  test("ResponseWithContext can be created with response and context") {
    val client = new TestSingleThreadedClient
    val resp = client.ResponseWithContext(Some("test"), Some("ctx"))
    assert(resp.response === Some("test"))
    assert(resp.context === Some("ctx"))
  }
}
