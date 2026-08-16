// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.slf4j.Logger

import java.io.Closeable
import java.net.{InetAddress, InetSocketAddress, NetworkInterface, SocketException, UnknownHostException}
import java.nio.channels.ServerSocketChannel
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** The machine list, listen port, and machine count to hand to the native LGBM_NetworkInit call. */
private[lightgbm] final case class BridgedNetwork(machineList: String,
                                                  localListenPort: Int,
                                                  machineCount: Int,
                                                  rank: Int)

/** Bridges an IPv6 LightGBM training network onto the IPv4-only native transport.
  *
  * The native LightGBM socket layer is IPv4-only in every released version, including the
  * lightgbmlib artifact this project depends on. `Linkers::ParseMachineList` splits each machine
  * entry on ':' and keeps it only when it has exactly two parts, so `[2001:db8::1]:12400` is
  * discarded and `2001:db8::1:12400` is misread as the host `1`; `TcpSocket` then builds every
  * address with `socket(AF_INET, ...)`, `sockaddr_in`, and `inet_pton(AF_INET, ...)`, so even a
  * parsed IPv6 literal could neither be dialed nor accepted.
  *
  * Rather than fail, this bridge keeps the native library on the transport it understands and
  * carries the traffic itself:
  *
  *  - an inbound relay owns the port this task advertised to its peers, accepts their connections
  *    on either address family, checks the LightGBM rank handshake, and forwards each link to the
  *    native listener over IPv4 loopback;
  *  - one outbound relay per IPv6 peer listens on IPv4 loopback and forwards whatever the native
  *    library sends to that peer's real IPv6 endpoint;
  *  - the machine list handed to the native library is rewritten so every bridged endpoint becomes
  *    a `127.0.0.1:port` entry, prefixed with an explicit `rank=` so the native library never has
  *    to infer its own position from a loopback address.
  *
  * Entry order is preserved because a LightGBM rank is an index into the machine list, and an IPv4
  * peer keeps its original entry so it stays a direct native connection. A machine list without any
  * IPv6 entry never reaches this class: `requiresBridge` is false and the native call is made
  * exactly as it always was.
  *
  * The native listener is the one thing this library cannot rebind: `TcpSocket::Bind` hardcodes
  * `0.0.0.0`, so it is reachable on every IPv4 interface for as long as it is open, and the native
  * accept loop has no timeout and trusts the first four bytes of every connection as a rank. The
  * bridge therefore claims each of that listener's link slots itself, from loopback, as soon as the
  * port is bound, which closes the listener within milliseconds of `LGBM_NetworkInit` binding it,
  * and it performs the rank handshake with peers on its own port instead, where a stalled or
  * invalid handshake is rejected rather than left to block the native accept loop. Removing the
  * remaining window needs an upstream change: `TcpSocket::Bind` taking a bind address so that
  * `Linkers::TryBind` can pass a loopback address.
  */
private[lightgbm] object LightGBMNetworkBridge {
  /** Native LightGBM resolves this with inet_pton(AF_INET), so it has to stay a numeric IPv4 literal. */
  private[lightgbm] val LoopbackHost: String = "127.0.0.1"

  /** LightGBM reads this prefix from the machine list and skips its own local-address lookup. */
  private[lightgbm] val RankPrefix: String = "rank="

  /** Covers the native connect-retry budget (20 attempts with a 1.3x backoff from 200ms). */
  private[lightgbm] val DefaultConnectTimeoutMillis: Long = 180000L

  /** A peer that has opened a connection has to identify itself well within the native timeout. */
  private[lightgbm] val DefaultHandshakeTimeoutMillis: Long = 30000L

  /** Headroom over the links a topology can actually need, which is one per other machine. */
  private val RelayedConnectionsPerMachine: Int = 2

  private val BridgeCount = new AtomicInteger(0)

  private def nextBridgeId(): Int = BridgeCount.incrementAndGet()

  /** Parse a LightGBM machine list into its endpoints, preserving order. */
  def parseMachineList(machineList: String): Seq[WorkerEndpoint] = {
    val entries = splitEntries(machineList)
    if (entries.isEmpty) {
      throw new IllegalArgumentException(
        s"LightGBM machine list ${WorkerEndpoint.preview(machineList)} does not contain any endpoint")
    }
    entries.map(WorkerEndpoint.parse)
  }

  /** Whether the native library would have to speak IPv6 to establish this network.
    *
    * This deliberately classifies without parsing, so an IPv4 machine list keeps reaching the
    * native call unchanged even if it holds an entry this library would reject.
    */
  def requiresBridge(machineList: String): Boolean = splitEntries(machineList).exists(isIpv6Entry)

  private def splitEntries(machineList: String): Seq[String] =
    Option(machineList).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty).toSeq

  /** An IPv6 entry is either bracketed or has more colons than the single host:port separator. */
  private def isIpv6Entry(entry: String): Boolean = entry.startsWith("[") || entry.count(_ == ':') > 1

  /** Build the machine list handed to the native library. */
  private[lightgbm] def formatMachineList(rank: Int, entries: Seq[String]): String =
    (Seq(s"$RankPrefix$rank") ++ entries).mkString(",")

  /** Locate this task's own entry, which is also its LightGBM rank.
    *
    * The driver echoes back the exact host string the task reported, so an exact match is the
    * normal path. The fallbacks only matter when a host string is rewritten on the way (a
    * differently compressed IPv6 literal, for example), and each one is anchored on the advertised
    * port so it can never select another machine's entry.
    */
  private[lightgbm] def findSelf(entries: Seq[WorkerEndpoint], advertisedHost: String, advertisedPort: Int): Int = {
    val candidates = entries.zipWithIndex.filter { case (entry, _) => entry.port == advertisedPort }
    val self = candidates.find { case (entry, _) => entry.host == advertisedHost }
      .orElse(candidates.find { case (entry, _) => sameAddress(entry.host, advertisedHost) })
      .orElse(if (candidates.lengthCompare(1) == 0) candidates.headOption else None)
      .orElse(candidates.find { case (entry, _) => isLocalAddress(entry.host) })
    self.map { case (_, index) => index }.getOrElse {
      throw new IllegalArgumentException(
        s"LightGBM machine list ${WorkerEndpoint.preview(entries.map(_.wireString).mkString(","))} does not " +
          "contain this task's own endpoint " +
          s"${WorkerEndpoint.preview(s"${WorkerEndpoint.wireHost(advertisedHost)}:$advertisedPort")}. " +
          "The endpoint a task advertises to the driver has to appear in the machine list the driver sends back.")
    }
  }

  /** Resolve a peer host, failing with an actionable message instead of a bare UnknownHostException. */
  private[lightgbm] def resolvePeer(entry: WorkerEndpoint, log: Logger): InetAddress = {
    if (entry.hasNumericZone) {
      throw new IllegalArgumentException(s"Cannot reach LightGBM peer ${entry.wireString}: its IPv6 zone " +
        s"identifier '${entry.zoneId.getOrElse("")}' is a numeric interface index, which only means " +
        "anything on the machine that produced it. A peer has to advertise an interface name, such as " +
        "fe80::1%eth0.")
    }
    val address = try {
      InetAddress.getByName(entry.address)
    } catch {
      case failure: UnknownHostException =>
        throw new IllegalArgumentException(s"Cannot reach LightGBM peer ${entry.wireString}: " +
          "the host could not be resolved on this machine.", failure)
    }
    if (address.isLinkLocalAddress) scopedLinkLocalAddress(entry, log) else address
  }

  /** Attach a link-local peer's zone to a locally meaningful interface, or explain why it cannot be. */
  private def scopedLinkLocalAddress(entry: WorkerEndpoint, log: Logger): InetAddress = {
    log.warn(s"LightGBM peer ${entry.wireString} is an IPv6 link-local address. A link-local address is only " +
      "meaningful within one interface's scope, so its zone identifier is resolved against this machine's " +
      "interfaces. Prefer a globally routable or unique-local IPv6 address for distributed training.")
    entry.zoneId match {
      case None =>
        throw new IllegalArgumentException(s"Cannot reach LightGBM peer ${entry.wireString}: an IPv6 " +
          "link-local peer has to advertise a zone identifier (for example fe80::1%eth0), because a " +
          "link-local address alone does not say which interface to send from.")
      case Some(zone) =>
        try {
          InetAddress.getByName(entry.host)
        } catch {
          case failure: UnknownHostException =>
            throw new IllegalArgumentException(s"Cannot reach LightGBM peer ${entry.wireString}: its IPv6 " +
              s"zone identifier '$zone' does not name an interface on this machine, so the link-local " +
              "address cannot be reached from here.", failure)
        }
    }
  }

  private def sameAddress(host: String, otherHost: String): Boolean = {
    if (host.isEmpty || otherHost.isEmpty) {
      false
    } else {
      try {
        InetAddress.getByName(host) == InetAddress.getByName(otherHost)
      } catch {
        case _: UnknownHostException => false
      }
    }
  }

  private def isLocalAddress(host: String): Boolean = {
    try {
      val address = InetAddress.getByName(host)
      address.isAnyLocalAddress || address.isLoopbackAddress ||
        NetworkInterface.getNetworkInterfaces.asScala.exists(_.getInetAddresses.asScala.contains(address))
    } catch {
      case _: UnknownHostException => false
      case _: SocketException => false
    }
  }

  /** Start the relays for a machine list and return a running bridge.
    *
    * The caller owns the returned bridge and has to close it once the native network is done with
    * it, including when the native initialization it wraps fails.
    */
  def open(machineList: String,
           advertisedHost: String,
           advertisedPort: Int,
           log: Logger,
           connectTimeoutMillis: Long = DefaultConnectTimeoutMillis,
           handshakeTimeoutMillis: Long = DefaultHandshakeTimeoutMillis,
           connectAttempt: LightGBMNetworkRelay.ConnectAttempt =
             LightGBMNetworkRelay.DefaultConnect): LightGBMNetworkBridge = {
    requireIpv6CapableJvm()
    val entries = parseMachineList(machineList)
    val rank = findSelf(entries, advertisedHost, advertisedPort)
    val bridge = new LightGBMNetworkBridge(entries, rank, advertisedPort, log, connectTimeoutMillis,
      handshakeTimeoutMillis, connectAttempt)
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(bridge.close())(bridge.start())
    bridge
  }

  /** A JVM forced onto the IPv4 stack cannot open an IPv6 socket at all, whatever the bridge does. */
  private def requireIpv6CapableJvm(): Unit = {
    if (java.lang.Boolean.getBoolean("java.net.preferIPv4Stack")) {
      throw new IllegalStateException("This LightGBM training network has IPv6 endpoints, but this JVM was " +
        "started with -Djava.net.preferIPv4Stack=true, which prevents it from opening any IPv6 socket. " +
        "Remove that option from the Spark executor JVM options, or give the executors IPv4 addresses.")
    }
  }

  private[lightgbm] def maxLinksFor(machineCount: Int): Int = machineCount * RelayedConnectionsPerMachine
}

/** Owns the sockets and the single relay loop for one task. Created through its companion's `open`. */
private[lightgbm] final class LightGBMNetworkBridge private(entries: Seq[WorkerEndpoint],
                                                            rank: Int,
                                                            advertisedPort: Int,
                                                            log: Logger,
                                                            connectTimeoutMillis: Long,
                                                            handshakeTimeoutMillis: Long,
                                                            connectAttempt: LightGBMNetworkRelay.ConnectAttempt)
  extends Closeable {
  import LightGBMNetworkBridge._

  /** The name the relay thread of this bridge carries, so a thread dump attributes it. */
  private[lightgbm] val threadNamePrefix: String = s"lightgbm-network-bridge-${nextBridgeId()}-rank$rank"

  private val relay = new LightGBMNetworkRelay(threadNamePrefix, log, maxLinksFor(entries.length),
    connectTimeoutMillis, handshakeTimeoutMillis, connectAttempt)
  private val listeners = mutable.ListBuffer.empty[ServerSocketChannel]
  private var nativeListenPort: Int = -1
  private var bridgedMachineList: String = ""

  /** The values to pass to the native LGBM_NetworkInit call. */
  def bridgedNetwork: BridgedNetwork = synchronized {
    require(nativeListenPort > 0, "The LightGBM network bridge has not been started")
    BridgedNetwork(bridgedMachineList, nativeListenPort, entries.length, rank)
  }

  /** A failure the relay could not retry away, which leaves this task's transport unusable. */
  def terminalFailure: Option[Throwable] = relay.failure

  private[lightgbm] def relayLinkCount: Int = relay.linkCount

  private[lightgbm] def isRunning: Boolean = relay.isLoopAlive

  private[lightgbm] def start(): Unit = synchronized {
    // Resolve every peer before binding anything, so an unreachable address fails immediately.
    val peerAddresses = entries.zipWithIndex.map { case (entry, index) =>
      if (index == rank || !entry.isIpv6Literal) None else Some(resolvePeer(entry, log))
    }

    // Own the advertised port first: it is the only port peers know, and taking it before the
    // native port is chosen guarantees the two cannot collide.
    val inbound = bindWildcard(advertisedPort)
    nativeListenPort = findFreePort()
    relay.expectInboundRanks((0 until rank).toSet)
    relay.start()

    val bridgedEntries = entries.zipWithIndex.map { case (entry, index) =>
      if (index == rank) {
        WorkerEndpoint.wireString(LoopbackHost, nativeListenPort)
      } else {
        peerAddresses(index)
          .map(address => WorkerEndpoint.wireString(LoopbackHost, startOutboundRelay(index, entry, address)))
          .getOrElse(entry.wireString)
      }
    }
    bridgedMachineList = formatMachineList(rank, bridgedEntries)

    relay.addInboundListener(inbound)
    // Claim every slot of the native listener from loopback, so it closes as soon as it opens.
    val nativeAddress = new InetSocketAddress(InetAddress.getByName(LoopbackHost), nativeListenPort)
    (0 until rank).foreach(peerRank => relay.primeNativeLink(nativeAddress, peerRank))

    log.info(s"LightGBM IPv6 network bridge is rank $rank of ${entries.length}, relaying peer traffic from " +
      s"advertised port $advertisedPort to native listen port $nativeListenPort with machine list " +
      s"$bridgedMachineList")
  }

  private def startOutboundRelay(index: Int, entry: WorkerEndpoint, address: InetAddress): Int = {
    val listener = bindLoopback()
    relay.addOutboundListener(listener, new InetSocketAddress(address, entry.port),
      s"LightGBM machine $index at ${entry.wireString}")
    listener.socket().getLocalPort
  }

  private def bindWildcard(port: Int): ServerSocketChannel = {
    val listener = ServerSocketChannel.open()
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(closeQuietly(listener)) {
      // A wildcard bind is dual stack, so peers reach this port over either address family.
      listener.bind(new InetSocketAddress(port))
      listeners += listener
      listener
    }
  }

  private def bindLoopback(): ServerSocketChannel = {
    val listener = ServerSocketChannel.open()
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(closeQuietly(listener)) {
      listener.bind(new InetSocketAddress(InetAddress.getByName(LoopbackHost), 0))
      listeners += listener
      listener
    }
  }

  /** Pick a port for the native listener. Only this bridge ever dials it, over loopback. */
  private def findFreePort(): Int = {
    val probe = ServerSocketChannel.open()
    try {
      probe.bind(new InetSocketAddress(0))
      probe.socket().getLocalPort
    } finally {
      closeQuietly(probe)
    }
  }

  private def closeQuietly(resource: Closeable): Unit = {
    try {
      resource.close()
    } catch {
      case NonFatal(failure) => log.debug("LightGBM network bridge could not close a channel", failure)
    }
  }

  /** Release every relay socket and the loop thread. Safe to call more than once and from any thread. */
  override def close(): Unit = {
    relay.close()
    val current = synchronized {
      val snapshot = listeners.toSeq
      listeners.clear()
      snapshot
    }
    current.foreach(closeQuietly)
  }
}
