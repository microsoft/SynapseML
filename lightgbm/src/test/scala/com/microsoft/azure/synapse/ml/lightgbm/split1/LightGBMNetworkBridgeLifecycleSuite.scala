// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMNetworkBridge, LightGBMNetworkRelay, NetworkManager,
  NetworkTopologyInfo, WorkerEndpoint}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.io.{DataInputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.channels.ServerSocketChannel
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Executors, ThreadFactory, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

/** Covers what the IPv6 bridge does over the life of a task: admission, concurrency, and cleanup. */
class LightGBMNetworkBridgeLifecycleSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val log = LoggerFactory.getLogger(classOf[LightGBMNetworkBridgeLifecycleSuite])
  private val ipv6Loopback = "::1"
  private val ipv4Loopback = LightGBMNetworkBridge.LoopbackHost
  private val socketTimeoutMillis = 30000
  private val smallPayloadSize = 64 * 1024
  private val concurrentPeers = 8
  private val sequentialPeers = 8
  private val largeTopologySize = 33
  private val stallWriteBytes = 64L * 1024 * 1024
  private val stallObservationMillis = 2000L
  private val maxBufferedBytes = 8L * 1024 * 1024
  private val threadShutdownMillis = 10000L
  private val unsolicitedConnections = 6
  private val shortHandshakeMillis = 500L
  private val closeables = new ConcurrentLinkedQueue[AutoCloseable]()
  private val pool = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, "lightgbm-network-bridge-lifecycle-test")
      thread.setDaemon(true)
      thread
    }
  })

  override def afterEach(): Unit = {
    closeables.asScala.foreach(resource => Try(resource.close()))
    closeables.clear()
    super.afterEach()
  }

  private def register[T <: AutoCloseable](resource: T): T = {
    closeables.add(resource)
    resource
  }

  private def ipv6LoopbackAvailable: Boolean = Try {
    val probe = new ServerSocket()
    try {
      probe.bind(new InetSocketAddress(InetAddress.getByName(ipv6Loopback), 0))
      true
    } finally {
      probe.close()
    }
  }.getOrElse(false)

  private def freePort(host: String): Int = {
    val probe = new ServerSocket()
    try {
      probe.bind(new InetSocketAddress(InetAddress.getByName(host), 0))
      probe.getLocalPort
    } finally {
      probe.close()
    }
  }

  private def listenOnLoopback(port: Int): ServerSocket = {
    val listener = new ServerSocket()
    listener.bind(new InetSocketAddress(InetAddress.getByName(ipv4Loopback), port))
    listener.setSoTimeout(socketTimeoutMillis)
    register(listener)
  }

  private def connect(host: String, port: Int): Socket = {
    val socket = new Socket()
    socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), socketTimeoutMillis)
    socket.setSoTimeout(socketTimeoutMillis)
    register(socket)
  }

  private def dialAsRank(port: Int, rank: Int): Socket = {
    val socket = connect(ipv6Loopback, port)
    socket.getOutputStream.write(LightGBMNetworkRelay.rankBuffer(rank).array())
    socket.getOutputStream.flush()
    socket
  }

  private def acceptNativeSlot(listener: ServerSocket): (Int, Socket) = {
    val socket = register(listener.accept())
    val bytes = new Array[Byte](LightGBMNetworkRelay.RankBytes)
    new DataInputStream(socket.getInputStream).readFully(bytes)
    (ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).getInt, socket)
  }

  /** A machine list where this task is the last entry, so every other machine is a lower rank. */
  private def machineListWithSelfLast(peerCount: Int, advertisedPort: Int): String =
    ((1 to peerCount).map(_ => s"[$ipv6Loopback]:${freePort(ipv6Loopback)}") :+
      s"[$ipv6Loopback]:$advertisedPort").mkString(",")

  private def twoMachineList(selfPort: Int, peerPort: Int): String =
    s"[$ipv6Loopback]:$selfPort,[$ipv6Loopback]:$peerPort"

  private def payload(seed: Int, size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new Random(seed.toLong).nextBytes(bytes)
    bytes
  }

  private def assertPayloadCrosses(sender: Socket, receiver: Socket, bytes: Array[Byte]): Unit = {
    val received = pool.submit(new Callable[Array[Byte]] {
      override def call(): Array[Byte] = {
        val buffer = new Array[Byte](bytes.length)
        new DataInputStream(receiver.getInputStream).readFully(buffer)
        buffer
      }
    })
    sender.getOutputStream.write(bytes)
    sender.getOutputStream.flush()
    assert(received.get(socketTimeoutMillis.toLong, TimeUnit.MILLISECONDS).sameElements(bytes),
      "A relayed payload did not arrive intact")
  }

  private def assertEndOfStream(socket: Socket, clue: String): Unit = {
    val ended = try {
      socket.setSoTimeout(socketTimeoutMillis)
      socket.getInputStream.read() == -1
    } catch {
      case _: IOException => true
    }
    assert(ended, clue)
  }

  private def relayThreads(bridge: LightGBMNetworkBridge): Seq[Thread] =
    Thread.getAllStackTraces.keySet.asScala
      .filter(thread => thread.isAlive && thread.getName.startsWith(bridge.threadNamePrefix)).toSeq

  private def awaitNoRelayThreads(bridge: LightGBMNetworkBridge): Unit = {
    val deadline = System.currentTimeMillis() + threadShutdownMillis

    @tailrec
    def poll(): Seq[Thread] = {
      val alive = relayThreads(bridge)
      if (alive.isEmpty || System.currentTimeMillis() > deadline) {
        alive
      } else {
        Thread.sleep(50L)
        poll()
      }
    }

    assert(poll().isEmpty, s"Relay threads of ${bridge.threadNamePrefix} outlived the bridge")
  }

  private def awaitLinkCount(bridge: LightGBMNetworkBridge, expected: Int, clue: String): Unit = {
    val deadline = System.currentTimeMillis() + threadShutdownMillis

    @tailrec
    def poll(): Int = {
      val count = bridge.relayLinkCount
      if (count == expected || System.currentTimeMillis() > deadline) count else {
        Thread.sleep(25L)
        poll()
      }
    }

    assert(poll() == expected, clue)
  }

  private def assertPortIsFree(port: Int): Unit = {
    val rebound = new ServerSocket()
    try {
      rebound.bind(new InetSocketAddress(port))
      assert(rebound.isBound)
    } finally {
      rebound.close()
    }
  }

  test("A connection that never sends its rank is dropped instead of stalling the native listener") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log, handshakeTimeoutMillis = shortHandshakeMillis))
    listenOnLoopback(bridge.bridgedNetwork.localListenPort)

    // The native accept loop has no timeout of its own, so a silent caller has to be dropped here.
    val silent = connect(ipv6Loopback, advertisedPort)
    assertEndOfStream(silent, "A connection that never sent a rank was left open")
    awaitLinkCount(bridge, 0, "A dropped handshake still holds a link slot")
  }

  test("A connection claiming a rank the topology does not have is refused") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    // Rank 1 of 2, so rank 0 is the only rank allowed to open a link here.
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    listenOnLoopback(bridge.bridgedNetwork.localListenPort)

    Seq(1, 7, -1).foreach { forged =>
      val stray = dialAsRank(advertisedPort, forged)
      assertEndOfStream(stray, s"A connection claiming rank $forged was relayed to the native library")
    }
    awaitLinkCount(bridge, 0, "A refused handshake still holds a link slot")
  }

  test("A second connection claiming an already linked rank is refused") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val (claimedRank, nativeSide) = acceptNativeSlot(nativeListener)
    assert(claimedRank == 0)

    val first = dialAsRank(advertisedPort, 0)
    assertPayloadCrosses(first, nativeSide, payload(1, 64))

    val impostor = dialAsRank(advertisedPort, 0)
    assertEndOfStream(impostor, "A duplicate rank was allowed to take over an established link")
    assertPayloadCrosses(first, nativeSide, payload(2, 64))
  }

  test("A mixed IPv4 and IPv6 topology relays only its IPv6 peer and still accepts both families") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val ipv4PeerPort = freePort(ipv4Loopback)
    val ipv6PeerPort = freePort(ipv6Loopback)
    val machineList = s"$ipv4Loopback:$ipv4PeerPort,[$ipv6Loopback]:$ipv6PeerPort,[$ipv6Loopback]:$advertisedPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val bridged = bridge.bridgedNetwork
    val entries = bridged.machineList.split(",").toSeq

    // The IPv4 peer keeps its own entry, so the native library dials it with no relay in between.
    assert(entries(1) == s"$ipv4Loopback:$ipv4PeerPort")
    assert(WorkerEndpoint.parse(entries(2)).host == ipv4Loopback)

    val nativeListener = listenOnLoopback(bridged.localListenPort)
    val slots = (1 to 2).map(_ => acceptNativeSlot(nativeListener)).toMap
    assert(slots.keySet == Set(0, 1))

    // The advertised port is a dual stack listener, so peers of either family land on the native side.
    val ipv4Peer = connect(ipv4Loopback, advertisedPort)
    ipv4Peer.getOutputStream.write(LightGBMNetworkRelay.rankBuffer(0).array())
    ipv4Peer.getOutputStream.flush()
    assertPayloadCrosses(ipv4Peer, slots(0), payload(1, smallPayloadSize))

    val ipv6Peer = dialAsRank(advertisedPort, 1)
    assertPayloadCrosses(ipv6Peer, slots(1), payload(2, smallPayloadSize))
    assertPayloadCrosses(slots(1), ipv6Peer, payload(3, smallPayloadSize))
  }

  test("Sequential links from every lower rank are served without accumulating threads") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(sequentialPeers, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val slots = (1 to sequentialPeers).map(_ => acceptNativeSlot(nativeListener)).toMap
    val threadsAfterStart = relayThreads(bridge).size

    (0 until sequentialPeers).foreach { rank =>
      val peer = dialAsRank(advertisedPort, rank)
      assertPayloadCrosses(peer, slots(rank), payload(rank, 4096))
      peer.close()
      slots(rank).close()
    }

    assert(relayThreads(bridge).size == threadsAfterStart,
      s"The relay thread count changed while serving $sequentialPeers links")
  }

  test("Concurrent relayed links keep their streams separate") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(concurrentPeers, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val slots = (1 to concurrentPeers).map(_ => acceptNativeSlot(nativeListener)).toMap

    val peers = (0 until concurrentPeers).map(rank => rank -> dialAsRank(advertisedPort, rank))
    val transfers = peers.map { case (rank, peer) =>
      pool.submit(new Runnable {
        override def run(): Unit = assertPayloadCrosses(peer, slots(rank), payload(rank, smallPayloadSize))
      })
    }
    transfers.foreach(_.get(socketTimeoutMillis.toLong, TimeUnit.MILLISECONDS))
  }

  test("One relay thread serves a bridge whatever the machine count is") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val smallPort = freePort(ipv6Loopback)
    val small = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, smallPort),
      ipv6Loopback, smallPort, log))
    val largePort = freePort(ipv6Loopback)
    val large = register(LightGBMNetworkBridge.open(machineListWithSelfLast(largeTopologySize - 1, largePort),
      ipv6Loopback, largePort, log))
    val nativeListener = listenOnLoopback(large.bridgedNetwork.localListenPort)
    val slots = (1 until largeTopologySize).map(_ => acceptNativeSlot(nativeListener)).toMap

    val peers = (0 until largeTopologySize - 1).map(rank => rank -> dialAsRank(largePort, rank))
    peers.foreach { case (rank, peer) => assertPayloadCrosses(peer, slots(rank), payload(rank, 1024)) }

    assert(relayThreads(small).size == 1, "A two machine bridge should run exactly one relay thread")
    assert(relayThreads(large).size == 1,
      s"A $largeTopologySize machine bridge with ${peers.size} live links should still run one relay thread")
  }

  test("An abrupt failure on one relay direction closes the other one too") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val (_, nativeSide) = acceptNativeSlot(nativeListener)
    val peer = dialAsRank(advertisedPort, 0)
    assertPayloadCrosses(peer, nativeSide, payload(1, 64))
    awaitLinkCount(bridge, 1, "The established link was not counted")

    // A reset, which is what a killed executor or a dropped route looks like to the other side.
    nativeSide.setSoLinger(true, 0)
    nativeSide.close()

    assertEndOfStream(peer, "The peer was left waiting on a link whose other half had failed")
    awaitLinkCount(bridge, 0, "An aborted link never released its slot")
  }

  test("A stalled reader stops the sender instead of buffering inside the bridge") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val (_, nativeSide) = acceptNativeSlot(nativeListener)  // deliberately never read from again
    val peer = dialAsRank(advertisedPort, 0)

    val accepted = new AtomicLong(0)
    pool.submit(new Runnable {
      override def run(): Unit = {
        val chunk = new Array[Byte](64 * 1024)

        @tailrec
        def writeChunk(): Unit = {
          if (accepted.get() < stallWriteBytes) {
            peer.getOutputStream.write(chunk)
            accepted.addAndGet(chunk.length.toLong)
            writeChunk()
          }
        }

        Try(writeChunk())
      }
    })
    Thread.sleep(stallObservationMillis)

    val buffered = accepted.get()
    assert(buffered < maxBufferedBytes,
      s"The bridge absorbed $buffered bytes for a reader that never read, so backpressure is not reaching the peer")
    assert(buffered > 0, "The relay never forwarded anything to the stalled reader")
    assert(nativeSide.isConnected)
  }

  test("Closing the bridge ends its relay thread, which is a daemon") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log)
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    acceptNativeSlot(nativeListener)
    val peer = dialAsRank(advertisedPort, 0)

    val running = relayThreads(bridge)
    assert(running.size == 1, "The bridge should run exactly one relay thread")
    assert(running.forall(_.isDaemon), "A relay thread would keep the executor JVM alive after training")

    bridge.close()
    awaitNoRelayThreads(bridge)
    assertPortIsFree(advertisedPort)
    Try(peer.close())
  }

  test("Cleanup after a cancelled task closes the bridge and preserves the interrupt") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val topology = NetworkTopologyInfo(twoMachineList(advertisedPort, freePort(ipv6Loopback)),
      Array(0), advertisedPort).withAdvertisedHost(ipv6Loopback)
    NetworkManager.initNativeNetwork(topology, 2, log, (_, _, _) => ())
    assert(topology.hasNetworkBridge)

    // Spark cancels a task by interrupting its thread, and cleanup still has to finish.
    Thread.currentThread().interrupt()
    try {
      topology.releaseNetworkResources()
      assert(Thread.currentThread().isInterrupted,
        "Bridge cleanup swallowed the interrupt that tells Spark the task was cancelled")
    } finally {
      Thread.interrupted()
    }
    assert(!topology.hasNetworkBridge)
    assertPortIsFree(advertisedPort)
  }

  test("A failed native init closes the bridge so the advertised port is free for the retry") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val topology = NetworkTopologyInfo(twoMachineList(advertisedPort, freePort(ipv6Loopback)),
      Array(0), advertisedPort).withAdvertisedHost(ipv6Loopback)

    val failure = intercept[RuntimeException] {
      NetworkManager.initNativeNetwork(topology, 2, log,
        (_, _, _) => throw new RuntimeException("native init failed"))
    }

    assert(failure.getMessage.contains("native init failed"))
    assert(!topology.hasNetworkBridge, "A bridge outlived the native failure it was opened for")
    assertPortIsFree(advertisedPort)
    val reservation = NetworkManager.reserveExactPort(advertisedPort, log)
    try {
      assert(reservation.getLocalPort == advertisedPort)
    } finally {
      reservation.close()
    }
  }

  test("The native init retry rebinds the advertised port and rebuilds the bridge on every attempt") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val topology = NetworkTopologyInfo(twoMachineList(advertisedPort, freePort(ipv6Loopback)),
      Array(0), advertisedPort).withAdvertisedHost(ipv6Loopback)
    val attempts = new AtomicInteger(0)
    val nativePorts = new ConcurrentLinkedQueue[Int]()
    val machineLists = new ConcurrentLinkedQueue[String]()

    NetworkManager.initLightGBMNetworkWithRetry(
      topology,
      log,
      retry = 2,
      delay = 1L,
      networkInit = () => NetworkManager.initNativeNetwork(topology, 2, log, (machines, port, _) => {
        machineLists.add(machines)
        nativePorts.add(port)
        if (attempts.incrementAndGet() < 3) throw new RuntimeException("native init failed")
      }),
      reservePort = port => NetworkManager.reserveExactPort(port, log),
      sleep = _ => ())

    assert(attempts.get() == 3, "The retry did not reach the attempt that succeeds")
    assert(machineLists.asScala.forall(_.startsWith(s"${LightGBMNetworkBridge.RankPrefix}0,")),
      "Every attempt has to pin the rank for the native library")
    assert(nativePorts.asScala.forall(_ != advertisedPort),
      "The native listener must never be given the port the bridge owns")
    assert(topology.hasNetworkBridge, "The successful attempt did not keep its bridge")

    topology.releaseNetworkResources()
    assertPortIsFree(advertisedPort)
  }

  /** A connect that fails the way an address with no route does: synchronously, on the caller. */
  private def failingConnect(failuresPerAddress: Int,
                             matches: InetSocketAddress => Boolean,
                             attempts: AtomicInteger): LightGBMNetworkRelay.ConnectAttempt =
    (channel, address) => {
      if (matches(address) && attempts.incrementAndGet() <= failuresPerAddress) {
        throw new java.net.SocketException("Network is unreachable")
      }
      LightGBMNetworkRelay.DefaultConnect(channel, address)
    }

  private def isLoopbackAddress(address: InetSocketAddress): Boolean =
    address.getAddress.isLoopbackAddress

  test("A dial that fails synchronously is retried on the timer until it connects") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val attempts = new AtomicInteger(0)
    // The native link slot is claimed over loopback, so this fails that dial three times first.
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log, connectAttempt = failingConnect(3, isLoopbackAddress, attempts)))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)

    val (claimedRank, nativeSide) = acceptNativeSlot(nativeListener)
    assert(claimedRank == 0, "The retried dial did not claim the slot for the lower rank")
    assert(attempts.get() > 3, "The connect hook was not exercised")
    assert(bridge.terminalFailure.isEmpty, "A retried failure was recorded as terminal")

    val peer = dialAsRank(advertisedPort, 0)
    assertPayloadCrosses(peer, nativeSide, payload(1, smallPayloadSize))
  }

  test("A peer that can never be reached releases its slot, keeps the listener, and is recorded") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val peerPort = freePort(ipv6Loopback)
    val advertisedPort = freePort(ipv6Loopback)
    val attempts = new AtomicInteger(0)
    // Every dial to the peer fails synchronously, which is what ENETUNREACH looks like.
    val bridge = register(LightGBMNetworkBridge.open(
      s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:$peerPort", ipv6Loopback, advertisedPort, log,
      connectTimeoutMillis = 300L,
      connectAttempt = failingConnect(Int.MaxValue, address => address.getPort == peerPort, attempts)))
    val relayPort = WorkerEndpoint.parse(bridge.bridgedNetwork.machineList.split(",")(2)).port

    val nativeSide = connect(ipv4Loopback, relayPort)
    assertEndOfStream(nativeSide, "The native link was left open although its peer was unreachable")
    awaitLinkCount(bridge, 0, "An unreachable peer never released its link slot")
    assert(attempts.get() > 1, "The dial was not retried before giving up")

    // The listener has to survive the failure of the connection it accepted.
    val second = connect(ipv4Loopback, relayPort)
    assertEndOfStream(second, "The outbound listener stopped serving after a failed dial")
    awaitLinkCount(bridge, 0, "The second attempt never released its link slot")
    assert(bridge.terminalFailure.isDefined, "An unreachable peer was not recorded as a terminal failure")
  }

  test("A terminal relay failure fails the native init path instead of leaving the task waiting") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val peerPort = freePort(ipv6Loopback)
    val advertisedPort = freePort(ipv6Loopback)
    val attempts = new AtomicInteger(0)
    val bridge = register(LightGBMNetworkBridge.open(
      s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:$peerPort", ipv6Loopback, advertisedPort, log,
      connectTimeoutMillis = 300L,
      connectAttempt = failingConnect(Int.MaxValue, address => address.getPort == peerPort, attempts)))
    val relayPort = WorkerEndpoint.parse(bridge.bridgedNetwork.machineList.split(",")(2)).port
    val nativeSide = connect(ipv4Loopback, relayPort)
    assertEndOfStream(nativeSide, "The native link was left open although its peer was unreachable")

    val failure = intercept[Exception](NetworkManager.failIfBridgeIsBroken(bridge))
    assert(failure.getMessage.contains("network bridge for this task failed"))
    assert(failure.getMessage.contains("could not reach"))
  }

  test("A peer that disconnects mid handshake leaves the advertised listener serving") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freePort(ipv6Loopback)
    val bridge = register(LightGBMNetworkBridge.open(machineListWithSelfLast(1, advertisedPort),
      ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)
    val (_, nativeSide) = acceptNativeSlot(nativeListener)

    // A half sent rank followed by a reset, which is what a killed peer looks like.
    val aborted = connect(ipv6Loopback, advertisedPort)
    aborted.getOutputStream.write(Array[Byte](0, 0))
    aborted.getOutputStream.flush()
    aborted.setSoLinger(true, 0)
    aborted.close()
    awaitLinkCount(bridge, 0, "An aborted handshake never released its link slot")

    val peer = dialAsRank(advertisedPort, 0)
    assertPayloadCrosses(peer, nativeSide, payload(2, smallPayloadSize))
  }

  test("Unsolicited connections to the lowest rank never consume the links its peers need") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    // Rank 0 is the only rank no machine opens a link to, so every inbound connection is unsolicited.
    val advertisedPort = freePort(ipv6Loopback)
    val peerListener = ServerSocketChannel.open()
    peerListener.bind(new InetSocketAddress(InetAddress.getByName(ipv6Loopback), 0))
    register(peerListener)
    val peerPort = peerListener.socket().getLocalPort
    val machineList = s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:$peerPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    assert(bridge.bridgedNetwork.rank == 0)

    // More than the two machine cap allows, so a slot leaked per refusal would exhaust it.
    (1 to unsolicitedConnections).foreach { attempt =>
      val stray = connect(ipv6Loopback, advertisedPort)
      assertEndOfStream(stray, s"Unsolicited connection $attempt was not refused")
    }
    awaitLinkCount(bridge, 0, "Refusing an unsolicited connection consumed a link slot")

    // The native library can still open its own outbound link, which draws on the same cap.
    val relayPort = WorkerEndpoint.parse(bridge.bridgedNetwork.machineList.split(",")(2)).port
    val nativeSide = connect(ipv4Loopback, relayPort)
    val peerSide = register(peerListener.accept().socket())
    peerSide.setSoTimeout(socketTimeoutMillis)
    assertPayloadCrosses(nativeSide, peerSide, payload(7, smallPayloadSize))
    awaitLinkCount(bridge, 1, "The native outbound link was not admitted after the refusals")
  }

  test("NetworkTopologyInfo keeps the three field shape callers and serialized forms depend on") {
    val partitions = Array(0, 1)
    val topology = NetworkTopologyInfo("10.0.0.4:12400", partitions, 12400)
    assert(topology.productArity == 3)
    assert(NetworkTopologyInfo.unapply(topology).map { case (machines, ids, port) =>
      (machines, ids.toSeq, port)
    }.contains(("10.0.0.4:12400", Seq(0, 1), 12400)))
    assert(topology.copy(localListenPort = 12401).localListenPort == 12401)

    // The advertised host is task local state, so it changes neither the shape nor equality.
    val withHost = topology.withAdvertisedHost("2001:db8::1")
    assert(withHost.taskHost == "2001:db8::1")
    assert(withHost == NetworkTopologyInfo("10.0.0.4:12400", partitions, 12400).withAdvertisedHost("other"))
    assert(NetworkTopologyInfo("10.0.0.4:12400", partitions, 12400).taskHost.isEmpty)
  }
}
