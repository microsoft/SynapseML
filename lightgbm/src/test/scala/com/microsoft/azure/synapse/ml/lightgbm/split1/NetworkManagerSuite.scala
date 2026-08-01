// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{NetworkManager, NetworkTopologyInfo}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.io.IOException
import java.net.{BindException, InetSocketAddress, ServerSocket, Socket, SocketAddress, SocketException}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class NetworkManagerSuite extends AnyFunSuite {

  private val log = LoggerFactory.getLogger(classOf[NetworkManagerSuite])

  private class FailingCloseSocket(closeFailures: Seq[IOException]) extends Socket {
    var closeAttempts = 0

    override def close(): Unit = {
      val attempt = closeAttempts
      closeAttempts += 1
      if (attempt < closeFailures.length) throw closeFailures(attempt)
      super.close()
    }
  }

  private def bindEphemeralSocket(): Socket = {
    val socket = new Socket()
    socket.bind(new InetSocketAddress(0))
    socket
  }

  private def assertPortAvailable(port: Int): Unit = {
    val socket = new ServerSocket()
    try {
      socket.bind(new InetSocketAddress(port))
      assert(socket.isBound)
    } finally {
      socket.close()
    }
  }

  test("Port reservation skips a competing socket and holds its port until network initialization") {
    val competitor = bindEphemeralSocket()
    val reservation = NetworkManager.reserveOpenPort(competitor.getLocalPort, log)
    val reservedPort = reservation.getLocalPort

    try {
      assert(reservedPort != competitor.getLocalPort)
      val topology = NetworkManager.withPortReservation(reservation, shouldExecuteTraining = true) { port =>
        NetworkTopologyInfo(s"localhost:$port", Array(0), port)
      }

      assert(!reservation.isClosed)
      val challenger = new ServerSocket()
      try {
        assertThrows[BindException] {
          challenger.bind(new InetSocketAddress(reservedPort))
        }
      } finally {
        challenger.close()
      }

      topology.releasePortReservation()
      topology.releasePortReservation()
      assert(reservation.isClosed)
    } finally {
      if (!reservation.isClosed) reservation.close()
      competitor.close()
    }

    assertPortAvailable(reservedPort)
  }

  test("Helper and failed topology paths release their port reservations") {
    val helperReservation = bindEphemeralSocket()
    val helperPort = helperReservation.getLocalPort
    try {
      NetworkManager.withPortReservation(helperReservation, shouldExecuteTraining = false) { port =>
        NetworkTopologyInfo("", Array.empty[Int], port)
      }
      assert(helperReservation.isClosed)
    } finally {
      if (!helperReservation.isClosed) helperReservation.close()
    }
    assertPortAvailable(helperPort)

    val failedReservation = bindEphemeralSocket()
    val failedPort = failedReservation.getLocalPort
    val expected = new IOException("topology lookup failed")
    try {
      val actual = intercept[IOException] {
        NetworkManager.withPortReservation(failedReservation, shouldExecuteTraining = true) { _ =>
          throw expected
        }
      }
      assert(actual eq expected)
      assert(failedReservation.isClosed)
    } finally {
      if (!failedReservation.isClosed) failedReservation.close()
    }
    assertPortAvailable(failedPort)
  }

  test("Non-contention bind failures propagate after closing the candidate socket") {
    val expected = new SocketException("network configuration failure")
    val candidate = new Socket() {
      override def bind(bindpoint: SocketAddress): Unit = throw expected
    }

    try {
      val actual = intercept[SocketException] {
        NetworkManager.reserveOpenPort(12345, log, () => candidate)
      }
      assert(actual eq expected)
      assert(candidate.isClosed)
    } finally {
      if (!candidate.isClosed) candidate.close()
    }
  }

  test("Bind failures stay primary when candidate cleanup initially fails") {
    val bindFailure = new SocketException("network configuration failure")
    val cleanupFailure = new IOException("candidate close failed")
    val candidate = new FailingCloseSocket(Seq(cleanupFailure)) {
      override def bind(bindpoint: SocketAddress): Unit = throw bindFailure
    }

    try {
      val actual = intercept[SocketException] {
        NetworkManager.reserveOpenPort(12345, log, () => candidate)
      }

      assert(actual eq bindFailure)
      assert(actual.getSuppressed.toSeq == Seq(cleanupFailure))
      assert(candidate.closeAttempts == 2)
      assert(candidate.isClosed)
    } finally {
      if (!candidate.isClosed) candidate.close()
    }
  }

  test("A failed reservation close retries before retaining the socket for final cleanup") {
    val firstFailure = new IOException("first close failure")
    val secondFailure = new IOException("second close failure")
    val reservation = new FailingCloseSocket(Seq(firstFailure, secondFailure))
    reservation.bind(new InetSocketAddress(0))
    val topology = NetworkTopologyInfo("", Array.empty[Int], reservation.getLocalPort)
      .retainPortReservation(reservation)

    try {
      val actual = intercept[IOException] {
        topology.releasePortReservation()
      }
      assert(actual eq firstFailure)
      assert(actual.getSuppressed.toSeq == Seq(secondFailure))
      assert(!reservation.isClosed)
      assert(reservation.closeAttempts == 2)

      topology.releasePortReservation()
      assert(reservation.isClosed)
      assert(reservation.closeAttempts == 3)
    } finally {
      if (!reservation.isClosed) reservation.close()
    }
  }

  test("Repeated cleanup failures are suppressed without replacing the primary failure") {
    val primaryFailure = new IllegalStateException("partition failed")
    val firstCleanupFailure = new IOException("first reservation close failed")
    val secondCleanupFailure = new IOException("second reservation close failed")
    val reservation = new FailingCloseSocket(Seq(firstCleanupFailure, secondCleanupFailure))
    reservation.bind(new InetSocketAddress(0))
    val topology = NetworkTopologyInfo("", Array.empty[Int], reservation.getLocalPort)
      .retainPortReservation(reservation)

    try {
      val actual = intercept[IllegalStateException] {
        NetworkManager.withCleanupPreservingPrimary(topology.releasePortReservation()) {
          throw primaryFailure
        }
      }

      assert(actual eq primaryFailure)
      assert(actual.getSuppressed.toSeq == Seq(firstCleanupFailure))
      assert(firstCleanupFailure.getSuppressed.toSeq == Seq(secondCleanupFailure))
      assert(reservation.closeAttempts == 2)
      assert(!reservation.isClosed)

      topology.releasePortReservation()
      assert(reservation.closeAttempts == 3)
      assert(reservation.isClosed)
    } finally {
      if (!reservation.isClosed) reservation.close()
    }
  }

  test("Topology lookup failure stays primary when direct reservation cleanup fails") {
    val primaryFailure = new IllegalArgumentException("topology failed")
    val cleanupFailure = new IOException("reservation close failed")
    val reservation = new FailingCloseSocket(Seq(cleanupFailure))
    reservation.bind(new InetSocketAddress(0))

    try {
      val actual = intercept[IllegalArgumentException] {
        NetworkManager.withPortReservation(reservation, shouldExecuteTraining = true) { _ =>
          throw primaryFailure
        }
      }

      assert(actual eq primaryFailure)
      assert(actual.getSuppressed.toSeq == Seq(cleanupFailure))
      assert(reservation.closeAttempts == 2)
      assert(reservation.isClosed)
    } finally {
      if (!reservation.isClosed) reservation.close()
    }
  }

  test("Native-init retry holds the advertised port throughout backoff") {
    val initialReservation = bindEphemeralSocket()
    val port = initialReservation.getLocalPort
    val topology = NetworkTopologyInfo(s"localhost:$port", Array(0), port)
      .retainPortReservation(initialReservation)
    val firstFailure = new Exception("first native init failed")
    var initCalls = 0
    var retryReservation: Option[Socket] = None
    var observedReservedBackoff = false

    try {
      NetworkManager.initLightGBMNetworkWithRetry(
        topology,
        log,
        retry = 1,
        delay = 1L,
        networkInit = () => {
          initCalls += 1
          if (initCalls == 1) throw firstFailure
          assert(retryReservation.exists(_.isClosed))
        },
        reservePort = retryPort => {
          val reservation = NetworkManager.reserveExactPort(retryPort, log)
          retryReservation = Option(reservation)
          reservation
        },
        sleep = _ => {
          assert(retryReservation.exists(reservation => !reservation.isClosed))
          val challenger = new ServerSocket()
          try {
            assertThrows[BindException] {
              challenger.bind(new InetSocketAddress(port))
            }
          } finally {
            challenger.close()
          }
          observedReservedBackoff = true
        })

      assert(initCalls == 2)
      assert(observedReservedBackoff)
      assert(retryReservation.exists(_.isClosed))
      assertPortAvailable(port)
    } finally {
      topology.releasePortReservation()
      retryReservation.foreach(reservation => if (!reservation.isClosed) reservation.close())
      if (!initialReservation.isClosed) initialReservation.close()
    }
  }

  test("Native-init retry preserves its failure when a competitor takes the advertised port") {
    val initialReservation = bindEphemeralSocket()
    val port = initialReservation.getLocalPort
    val topology = NetworkTopologyInfo(s"localhost:$port", Array(0), port)
      .retainPortReservation(initialReservation)
    val nativeFailure = new Exception("native init failed")
    var initCalls = 0
    var competitor: Option[ServerSocket] = None

    try {
      val actual = intercept[Exception] {
        NetworkManager.initLightGBMNetworkWithRetry(
          topology,
          log,
          retry = 1,
          delay = 1L,
          networkInit = () => {
            initCalls += 1
            val competingSocket = new ServerSocket()
            competitor = Option(competingSocket)
            competingSocket.bind(new InetSocketAddress(port))
            throw nativeFailure
          },
          reservePort = retryPort => NetworkManager.reserveExactPort(retryPort, log),
          sleep = _ => fail("Retry backoff must not start without an exact-port reservation"))
      }

      assert(actual eq nativeFailure)
      assert(initCalls == 1)
      assert(actual.getSuppressed.exists(_.isInstanceOf[BindException]))
    } finally {
      topology.releasePortReservation()
      competitor.foreach(socket => if (!socket.isClosed) socket.close())
      if (!initialReservation.isClosed) initialReservation.close()
    }

    assertPortAvailable(port)
  }

  test("Concurrent port reservations remain unique and are all reusable after cleanup") {
    val workerCount = 6
    val competitor = bindEphemeralSocket()
    val start = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(workerCount)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val allocated = new ConcurrentLinkedQueue[Socket]()

    try {
      val attempts = (1 to workerCount).map { _ =>
        Future {
          start.await()
          Try {
            val reservation = NetworkManager.reserveOpenPort(competitor.getLocalPort, log)
            allocated.add(reservation)
            reservation
          }
        }
      }
      start.countDown()

      val outcomes = Await.result(Future.sequence(attempts), 30.seconds)
      val failures = outcomes.collect { case Failure(error) => error }
      assert(failures.isEmpty, failures.mkString(", "))
      val reservations = outcomes.collect { case Success(reservation) => reservation }
      val reservedPorts = reservations.map(_.getLocalPort)
      assert(reservations.size == workerCount)
      assert(reservedPorts.distinct.size == workerCount)

      reservations.foreach { reservation =>
        val challenger = new ServerSocket()
        try {
          assertThrows[BindException] {
            challenger.bind(new InetSocketAddress(reservation.getLocalPort))
          }
        } finally {
          challenger.close()
        }
      }
    } finally {
      try {
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
      } finally {
        allocated.asScala.foreach(reservation => if (!reservation.isClosed) reservation.close())
        competitor.close()
      }
    }

    allocated.asScala.foreach(reservation => assertPortAvailable(reservation.getLocalPort))
  }

}
