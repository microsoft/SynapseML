// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMNetworkBridge, LightGBMNetworkRelay, WorkerEndpoint}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.io.{DataInputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, NetworkInterface, ServerSocket, Socket}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Executors, ThreadFactory, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.Try

/** Covers the IPv6 transport bridge that carries the LightGBM traffic the native library cannot. */
class LightGBMNetworkBridgeSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val log = LoggerFactory.getLogger(classOf[LightGBMNetworkBridgeSuite])
  private val socketTimeoutMillis = 30000
  private val ipv6Loopback = "::1"
  private val payloadSize = 1 << 20
  private val closeables = new ConcurrentLinkedQueue[AutoCloseable]()
  private val pool = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, "lightgbm-network-bridge-test")
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

  private def listenOnIpv6(): ServerSocket = {
    val listener = new ServerSocket()
    listener.bind(new InetSocketAddress(InetAddress.getByName(ipv6Loopback), 0))
    listener.setSoTimeout(socketTimeoutMillis)
    register(listener)
  }

  private def listenOnLoopback(port: Int): ServerSocket = {
    val listener = new ServerSocket()
    listener.bind(new InetSocketAddress(InetAddress.getByName(LightGBMNetworkBridge.LoopbackHost), port))
    listener.setSoTimeout(socketTimeoutMillis)
    register(listener)
  }

  private def freeIpv6Port(): Int = {
    val probe = new ServerSocket()
    try {
      probe.bind(new InetSocketAddress(InetAddress.getByName(ipv6Loopback), 0))
      probe.getLocalPort
    } finally {
      probe.close()
    }
  }

  private def connect(host: String, port: Int): Socket = {
    val socket = new Socket()
    socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), socketTimeoutMillis)
    socket.setSoTimeout(socketTimeoutMillis)
    register(socket)
  }

  /** Stand in for a peer opening a LightGBM link, which starts by sending its own rank. */
  private def dialAsRank(port: Int, rank: Int, host: String = "::1"): Socket = {
    val socket = connect(host, port)
    socket.getOutputStream.write(LightGBMNetworkRelay.rankBuffer(rank).array())
    socket.getOutputStream.flush()
    socket
  }

  /** Stand in for the native listener, which the bridge claims a link slot on for every lower rank. */
  private def acceptNativeSlot(listener: ServerSocket): (Int, Socket) = {
    val socket = register(listener.accept())
    val bytes = new Array[Byte](LightGBMNetworkRelay.RankBytes)
    new DataInputStream(socket.getInputStream).readFully(bytes)
    (ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).getInt, socket)
  }

  private def endpoint(entry: String): WorkerEndpoint = WorkerEndpoint.parse(entry)

  private def bridgedEntries(machineList: String): Seq[String] = machineList.split(",").toSeq

  private def randomPayload(size: Int): Array[Byte] = {
    val payload = new Array[Byte](size)
    new Random(size.toLong).nextBytes(payload)
    payload
  }

  private def assertPayloadCrosses(sender: Socket, receiver: Socket, payload: Array[Byte]): Unit = {
    val received = pool.submit(new Callable[Array[Byte]] {
      override def call(): Array[Byte] = {
        val buffer = new Array[Byte](payload.length)
        new DataInputStream(receiver.getInputStream).readFully(buffer)
        buffer
      }
    })
    sender.getOutputStream.write(payload)
    sender.getOutputStream.flush()
    assert(received.get(socketTimeoutMillis.toLong, TimeUnit.MILLISECONDS).sameElements(payload),
      "The relayed payload did not arrive intact")
  }

  private def assertEndOfStream(socket: Socket, clue: String): Unit = {
    val ended = try {
      socket.getInputStream.read() == -1
    } catch {
      // A reset is the other legitimate way for the far side to report that it is gone.
      case _: IOException => true
    }
    assert(ended, clue)
  }

  test("An IPv4 machine list never needs the bridge") {
    assert(!LightGBMNetworkBridge.requiresBridge("127.0.0.1:12400,10.0.0.4:12400"))
    assert(!LightGBMNetworkBridge.requiresBridge("worker-1:12400,worker-2:12401"))
    assert(!LightGBMNetworkBridge.requiresBridge(""))
    assert(!LightGBMNetworkBridge.requiresBridge(None.orNull))
  }

  test("Every IPv6 machine list form needs the bridge") {
    assert(LightGBMNetworkBridge.requiresBridge("[2001:db8::1]:12400,[2001:db8::2]:12400"))
    assert(LightGBMNetworkBridge.requiresBridge("2001:db8::1:12400"))
    assert(LightGBMNetworkBridge.requiresBridge("[fe80::1%eth0]:12400"))
    // A single IPv6 machine in an otherwise IPv4 list is still unreachable for the native library.
    assert(LightGBMNetworkBridge.requiresBridge("10.0.0.4:12400,[2001:db8::2]:12400"))
  }

  test("Machine list parsing keeps entry order and rejects malformed entries") {
    val parsed = LightGBMNetworkBridge.parseMachineList(" [2001:db8::2]:12401 ,10.0.0.4:12400 ")
    assert(parsed.map(_.wireString) == Seq("[2001:db8::2]:12401", "10.0.0.4:12400"))
    assert(intercept[IllegalArgumentException](LightGBMNetworkBridge.parseMachineList("")).getMessage
      .contains("does not contain any endpoint"))
    assert(intercept[IllegalArgumentException](LightGBMNetworkBridge.parseMachineList("[2001:db8::2]"))
      .getMessage.contains("missing its port"))
  }

  test("A task finds its own machine list entry even when peers share its host or port") {
    val entries = Seq("[2001:db8::1]:12400", "[2001:db8::2]:12400", "[2001:db8::2]:12401").map(endpoint)
    assert(LightGBMNetworkBridge.findSelf(entries, "2001:db8::2", 12400) == 1)
    assert(LightGBMNetworkBridge.findSelf(entries, "2001:db8::1", 12400) == 0)
    assert(LightGBMNetworkBridge.findSelf(entries, "2001:db8::2", 12401) == 2)
    // A host written in a different but equivalent form still resolves to the same entry.
    assert(LightGBMNetworkBridge.findSelf(entries, "2001:0db8:0000:0000:0000:0000:0000:0002", 12401) == 2)
    // A unique port identifies the entry even when the reported host was not preserved.
    assert(LightGBMNetworkBridge.findSelf(entries, "", 12401) == 2)
  }

  test("A machine list without this task's endpoint fails with an actionable error") {
    val entries = Seq("[2001:db8::1]:12400", "[2001:db8::2]:12400").map(endpoint)
    val failure = intercept[IllegalArgumentException](LightGBMNetworkBridge.findSelf(entries, "2001:db8::9", 12999))
    assert(failure.getMessage.contains("does not contain this task's own endpoint"))
    assert(failure.getMessage.contains("[2001:db8::9]:12999"))
  }

  test("The bridged machine list keeps entry order, pins the rank, and only relays IPv6 peers") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freeIpv6Port()
    val peerPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$peerPort,10.0.0.4:12400,[$ipv6Loopback]:$advertisedPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))

    val bridged = bridge.bridgedNetwork
    val entries = bridgedEntries(bridged.machineList)
    assert(entries.head == s"${LightGBMNetworkBridge.RankPrefix}2", "The rank has to be pinned for the native library")
    assert(entries.length == 4)
    assert(bridged.machineCount == 3, "The bridge must not change the number of machines")
    assert(bridged.rank == 2)
    // The IPv4 peer stays a direct native connection; the IPv6 peer and this task are relayed.
    assert(endpoint(entries(1)).host == LightGBMNetworkBridge.LoopbackHost)
    assert(entries(2) == "10.0.0.4:12400")
    assert(endpoint(entries(3)) ==
      WorkerEndpoint.parse(s"${LightGBMNetworkBridge.LoopbackHost}:${bridged.localListenPort}"))
    assert(bridged.localListenPort != advertisedPort,
      "The native listener needs its own port, because the bridge owns the advertised one")
  }

  test("The bridge claims a native link slot for every lower rank as soon as the port is bound") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    // Rank 2 of 3, so the native listener would accept two links before closing itself.
    val advertisedPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:${freeIpv6Port()},[$ipv6Loopback]:${freeIpv6Port()}," +
      s"[$ipv6Loopback]:$advertisedPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val nativeListener = listenOnLoopback(bridge.bridgedNetwork.localListenPort)

    // No peer has connected yet, so these can only be the bridge claiming the slots from loopback.
    val claimed = (1 to 2).map(_ => acceptNativeSlot(nativeListener))
    assert(claimed.map { case (rank, _) => rank }.toSet == Set(0, 1),
      "The bridge has to identify each claimed slot with the rank that will use it")
    claimed.foreach { case (_, socket) =>
      assert(socket.getInetAddress.isLoopbackAddress, "A native link slot was claimed from off the machine")
    }
  }

  test("Peer traffic arriving over IPv6 reaches the native listener and flows both ways") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freeIpv6Port()
    val peerPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$peerPort,[$ipv6Loopback]:$advertisedPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val bridged = bridge.bridgedNetwork

    // Stand in for the native LightGBM listener, which only ever binds an IPv4 socket.
    val nativeListener = listenOnLoopback(bridged.localListenPort)
    val (claimedRank, nativeConnection) = acceptNativeSlot(nativeListener)
    assert(claimedRank == 0)

    val peerConnection = dialAsRank(advertisedPort, 0)
    assertPayloadCrosses(peerConnection, nativeConnection, randomPayload(payloadSize))
    assertPayloadCrosses(nativeConnection, peerConnection, randomPayload(payloadSize))

    // LightGBM ends a link by closing it, so the half close has to reach the peer.
    nativeConnection.shutdownOutput()
    assertEndOfStream(peerConnection, "The peer never saw the native end of stream")
  }

  test("Native traffic to an IPv6 peer is relayed to that peer's real address") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val peerListener = listenOnIpv6()
    val advertisedPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:${peerListener.getLocalPort}"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val bridged = bridge.bridgedNetwork

    // The native library dials the loopback entry the bridge published for machine 1.
    val relayPort = endpoint(bridgedEntries(bridged.machineList)(2)).port
    val nativeConnection = connect(LightGBMNetworkBridge.LoopbackHost, relayPort)
    val peerConnection = register(peerListener.accept())

    assertPayloadCrosses(nativeConnection, peerConnection, randomPayload(payloadSize))
    assertPayloadCrosses(peerConnection, nativeConnection, randomPayload(payloadSize))
  }

  test("A rank handshake in the LightGBM wire form survives the bridge") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    // An outbound link is forwarded verbatim, so the peer reads the rank the native library sent.
    val peerListener = listenOnIpv6()
    val advertisedPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:${peerListener.getLocalPort}"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val relayPort = endpoint(bridgedEntries(bridge.bridgedNetwork.machineList)(2)).port

    val nativeConnection = connect(LightGBMNetworkBridge.LoopbackHost, relayPort)
    nativeConnection.getOutputStream.write(LightGBMNetworkRelay.rankBuffer(0).array())
    nativeConnection.getOutputStream.flush()

    val peerConnection = register(peerListener.accept())
    val rankBytes = new Array[Byte](LightGBMNetworkRelay.RankBytes)
    new DataInputStream(peerConnection.getInputStream).readFully(rankBytes)
    assert(ByteBuffer.wrap(rankBytes).order(ByteOrder.nativeOrder()).getInt == 0)
  }

  test("A relayed connection whose far side never answers is closed instead of stalling LightGBM") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val deadPeerPort = freeIpv6Port()
    val advertisedPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:$deadPeerPort"
    val bridge = register(
      LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log, connectTimeoutMillis = 200L))
    val relayPort = endpoint(bridgedEntries(bridge.bridgedNetwork.machineList)(2)).port

    val nativeConnection = connect(LightGBMNetworkBridge.LoopbackHost, relayPort)
    // Nothing is listening on the peer port, so the relay gives up and closes the native side.
    assertEndOfStream(nativeConnection,
      "The bridge kept a relayed connection open even though its far side was unreachable")
  }

  test("An IPv6 link-local peer is only accepted with a zone this machine can resolve") {
    val unscoped = intercept[IllegalArgumentException](
      LightGBMNetworkBridge.resolvePeer(endpoint("[fe80::1]:12400"), log))
    assert(unscoped.getMessage.contains("has to advertise a zone identifier"))

    val unknownZone = intercept[IllegalArgumentException](
      LightGBMNetworkBridge.resolvePeer(endpoint("[fe80::1%no-such-interface]:12400"), log))
    assert(unknownZone.getMessage.contains("does not name an interface on this machine"))

    // A zone this machine does know keeps its scope all the way through resolution.
    val scopedZone = NetworkInterface.getNetworkInterfaces.asScala.map(_.getName)
      .find(name => Try(InetAddress.getByName(s"fe80::1%$name")).isSuccess)
    scopedZone.foreach { zone =>
      val resolved = LightGBMNetworkBridge.resolvePeer(endpoint(s"[fe80::1%$zone]:12400"), log)
      assert(resolved.isLinkLocalAddress)
      assert(resolved.getHostAddress.contains("%"), "The resolved link-local address lost its zone")
    }
  }

  test("A numeric IPv6 scope is normalized before it is published and rejected when a peer sends one") {
    // An interface index only means something on the machine that produced it.
    val named = NetworkInterface.getNetworkInterfaces.asScala.find(_.getIndex > 0)
    named.foreach { candidate =>
      val normalized = WorkerEndpoint.normalizeHost(s"fe80::1%${candidate.getIndex}")
      assert(normalized == s"fe80::1%${candidate.getName}",
        s"A numeric scope was published as is instead of as an interface name")
    }
    assert(WorkerEndpoint.normalizeHost("10.0.0.4") == "10.0.0.4")
    assert(WorkerEndpoint.normalizeHost("fe80::1%eth0") == "fe80::1%eth0")

    val numericPeer = intercept[IllegalArgumentException](
      LightGBMNetworkBridge.resolvePeer(endpoint("[fe80::1%3]:12400"), log))
    assert(numericPeer.getMessage.contains("numeric interface index"))
  }

  test("A JVM pinned to the IPv4 stack is told why it cannot join an IPv6 network") {
    val property = "java.net.preferIPv4Stack"
    val previous = Option(System.getProperty(property))
    try {
      System.setProperty(property, "true")
      val failure = intercept[IllegalStateException](
        LightGBMNetworkBridge.open("[2001:db8::1]:12400,[2001:db8::2]:12400", "2001:db8::1", 12400, log))
      assert(failure.getMessage.contains("-Djava.net.preferIPv4Stack=true"))
    } finally {
      previous.map(value => System.setProperty(property, value)).getOrElse(System.clearProperty(property))
    }
  }

  test("Closing the bridge releases the advertised port and its relay ports") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val advertisedPort = freeIpv6Port()
    val peerPort = freeIpv6Port()
    val machineList = s"[$ipv6Loopback]:$advertisedPort,[$ipv6Loopback]:$peerPort"
    val bridge = LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log)
    val relayPort = endpoint(bridgedEntries(bridge.bridgedNetwork.machineList)(2)).port

    bridge.close()
    bridge.close()  // The training path can close a bridge more than once.

    Seq(advertisedPort, relayPort).foreach { port =>
      val rebound = new ServerSocket()
      try {
        rebound.bind(new InetSocketAddress(port))
        assert(rebound.isBound)
      } finally {
        rebound.close()
      }
    }
  }
}
