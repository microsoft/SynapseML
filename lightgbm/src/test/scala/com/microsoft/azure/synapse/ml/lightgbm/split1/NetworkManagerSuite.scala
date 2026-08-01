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

  test("A failed reservation close remains retryable during final cleanup") {
    val expected = new IOException("temporary close failure")
    var closeAttempts = 0
    val reservation = new Socket() {
      override def close(): Unit = {
        closeAttempts += 1
        if (closeAttempts == 1) throw expected
        super.close()
      }
    }
    reservation.bind(new InetSocketAddress(0))
    val topology = NetworkTopologyInfo("", Array.empty[Int], reservation.getLocalPort)
      .retainPortReservation(reservation)

    try {
      val actual = intercept[IOException] {
        topology.releasePortReservation()
      }
      assert(actual eq expected)
      assert(!reservation.isClosed)

      topology.releasePortReservation()
      assert(reservation.isClosed)
      assert(closeAttempts == 2)
    } finally {
      if (!reservation.isClosed) reservation.close()
    }
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
