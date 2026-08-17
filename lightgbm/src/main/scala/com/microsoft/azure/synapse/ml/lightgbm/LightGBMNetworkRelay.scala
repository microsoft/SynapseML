// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.slf4j.Logger

import java.io.{Closeable, IOException}
import java.net.{InetSocketAddress, StandardSocketOptions}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

private[lightgbm] object LightGBMNetworkRelay {
  /** LightGBM opens every link by sending its own rank as a host endian int. */
  val RankBytes: Int = 4

  /** How a dial reaches its address. Injected so tests can fail a connect the way a route can. */
  type ConnectAttempt = (SocketChannel, InetSocketAddress) => Boolean

  val DefaultConnect: ConnectAttempt = (channel, address) => channel.connect(address)

  private val BufferSize: Int = 65536
  private val SocketBufferSize: Int = 100000
  private val SelectTimeoutMillis: Long = 100L
  private val ShutdownWaitMillis: Long = 2000L
  private val RetryIntervalMillis: Long = 5L
  private val MaxRetryIntervalMillis: Long = 200L

  private final case class Timer(deadline: Long, action: () => Unit)

  private val TimerOrder: Ordering[Timer] = Ordering.by[Timer, Long](-_.deadline)

  private[lightgbm] def rankBuffer(rank: Int): ByteBuffer =
    ByteBuffer.allocate(RankBytes).order(ByteOrder.nativeOrder()).putInt(rank)
}

/** The event loop that moves LightGBM traffic between peers and the native library.
  *
  * One selector thread serves every listener and every relayed link, so the thread count of a
  * bridge is one no matter how many machines the training network has. Memory is bounded the same
  * way: two fixed buffers per relayed link, and links are capped, so nothing scales with peers
  * except the sockets LightGBM itself would have opened.
  *
  * The loop also owns the LightGBM link handshake. A machine opens a link by sending its rank, so
  * an inbound connection has to produce a valid, unused, lower rank within a deadline before any of
  * its bytes reach the native library. Connections that stall, repeat a rank, or claim a rank the
  * topology does not have are closed here instead of stalling the native listener thread, which has
  * no timeout of its own.
  *
  * Every dial goes through one retry policy, whether the failure arrives synchronously (a connect
  * to an address with no route fails on the calling thread) or asynchronously, and a failure while
  * handling an accepted connection never reaches the listener that accepted it. What cannot be
  * retried is recorded as a terminal failure, which the caller surfaces instead of leaving a task
  * waiting on a transport that will not recover.
  */
private[lightgbm] final class LightGBMNetworkRelay(threadName: String,
                                                   log: Logger,
                                                   maxLinks: Int,
                                                   connectTimeoutMillis: Long,
                                                   handshakeTimeoutMillis: Long,
                                                   connectAttempt: LightGBMNetworkRelay.ConnectAttempt =
                                                     LightGBMNetworkRelay.DefaultConnect) extends Closeable {
  import LightGBMNetworkRelay._

  private val selector: Selector = Selector.open()
  private val tasks = new ConcurrentLinkedQueue[() => Unit]()
  private val timers = mutable.PriorityQueue.empty[Timer](TimerOrder)
  private val closed = new AtomicBoolean(false)
  private val started = new AtomicBoolean(false)
  private val liveLinks = new AtomicInteger(0)
  private val terminalFailure = new AtomicReference[Option[Throwable]](None)
  private val resources = mutable.Set.empty[Closeable]

  // Only the loop thread touches these.
  private var expectedInboundRanks: Set[Int] = Set.empty
  private val primedNativeLinks = mutable.Map.empty[Int, SocketChannel]
  private val peersAwaitingNativeLink = mutable.Map.empty[Int, (SocketChannel, LinkSlot)]
  private val linkedRanks = mutable.Set.empty[Int]

  private val loopThread: Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = runLoop()
    }, threadName)
    thread.setDaemon(true)
    thread
  }

  /** The ranks allowed to open a link to this task, which LightGBM defines as the lower ranks. */
  def expectInboundRanks(ranks: Set[Int]): Unit = submit(() => expectedInboundRanks = ranks)

  def start(): Unit = if (started.compareAndSet(false, true)) loopThread.start()

  def linkCount: Int = liveLinks.get()

  /** A failure the relay cannot retry away, which leaves this task's transport unusable. */
  def failure: Option[Throwable] = terminalFailure.get()

  private[lightgbm] def isLoopAlive: Boolean = loopThread.isAlive

  /** Accept peer connections, which must pass the rank handshake before they are relayed. */
  def addInboundListener(listener: ServerSocketChannel): Unit =
    register(listener, SelectionKey.OP_ACCEPT, new InboundAcceptor)

  /** Accept the native library's link to one peer and forward it to that peer's real address. */
  def addOutboundListener(listener: ServerSocketChannel,
                          target: InetSocketAddress,
                          description: String): Unit =
    register(listener, SelectionKey.OP_ACCEPT, new OutboundAcceptor(target, description))

  /** Take one of the native listener's link slots before anyone else can.
    *
    * The native listener accepts exactly one link per lower rank and then closes, so filling those
    * slots from loopback as soon as the port is bound keeps every other caller out of a listener
    * that the native library binds on all interfaces and that this library cannot rebind.
    */
  def primeNativeLink(nativeAddress: InetSocketAddress, peerRank: Int): Unit =
    submit(() => dialNative(nativeAddress, peerRank, System.currentTimeMillis() + connectTimeoutMillis,
      RetryIntervalMillis))

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      selector.wakeup()
      // Cleanup runs on a cancelled Spark task too, where the thread is already interrupted, so the
      // wait for the loop has to survive that and hand the interrupt back to the caller.
      if (started.get()) {
        try {
          loopThread.join(ShutdownWaitMillis)
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
        }
      }
      closeEverything()
      closeQuietly(selector)
      log.info(s"$threadName closed")
    }
  }

  private def recordTerminalFailure(reason: String, failure: Throwable): Unit = {
    log.error(s"$threadName $reason", failure)
    terminalFailure.compareAndSet(None, Some(new IOException(s"$threadName $reason", failure)))
  }

  private def submit(task: () => Unit): Unit = {
    tasks.add(task)
    selector.wakeup()
  }

  private def register(channel: java.nio.channels.SelectableChannel,
                       ops: Int,
                       handler: Handler): Unit = {
    track(channel)
    submit(() => {
      channel.configureBlocking(false)
      channel.register(selector, ops, handler)
      ()
    })
  }

  private def track(resource: Closeable): Unit = synchronized {
    if (closed.get()) closeQuietly(resource) else resources += resource
  }

  private def closeEverything(): Unit = {
    val snapshot = synchronized {
      val current = resources.toSeq
      resources.clear()
      current
    }
    snapshot.foreach(closeQuietly)
  }

  private def closeQuietly(resource: Closeable): Unit = {
    try {
      resource.close()
    } catch {
      case NonFatal(failure) => log.debug(s"$threadName could not close a channel", failure)
    }
  }

  @tailrec
  private def runLoop(): Unit = {
    if (!closed.get()) {
      step()
      runLoop()
    }
  }

  private def step(): Unit = {
    try {
      runTasks()
      selector.select(SelectTimeoutMillis)
      processSelectedKeys()
      runTimers()
    } catch {
      case NonFatal(failure) => if (!closed.get()) log.warn(s"$threadName event loop iteration failed", failure)
    }
  }

  @tailrec
  private def runTasks(): Unit = {
    val task = tasks.poll()
    if (task != None.orNull) {
      try {
        task()
      } catch {
        case NonFatal(failure) => recordTerminalFailure("could not apply a registration", failure)
      }
      runTasks()
    }
  }

  private def processSelectedKeys(): Unit = {
    val selected = selector.selectedKeys()
    val snapshot = selected.asScala.toSeq
    selected.clear()
    snapshot.foreach(handleKey)
  }

  private def handleKey(key: SelectionKey): Unit = {
    val handler = key.attachment().asInstanceOf[Handler]
    try {
      if (key.isValid && key.isAcceptable) handler.onAcceptable(key)
      if (key.isValid && key.isConnectable) handler.onConnectable(key)
      if (key.isValid && (key.isReadable || key.isWritable)) handler.onReadWrite(key)
    } catch {
      case NonFatal(failure) => handler.onFailure(key, failure)
    }
  }

  private def runTimers(): Unit = {
    val now = System.currentTimeMillis()
    val due = mutable.ListBuffer.empty[Timer]

    @tailrec
    def collect(): Unit = {
      if (timers.nonEmpty && timers.head.deadline <= now) {
        due += timers.dequeue()
        collect()
      }
    }

    collect()
    due.foreach { timer =>
      try {
        timer.action()
      } catch {
        // A retry that cannot even be started is terminal: nothing else will drive it.
        case NonFatal(failure) => recordTerminalFailure("could not run a scheduled retry", failure)
      }
    }
  }

  private def scheduleAt(delayMillis: Long)(action: => Unit): Unit =
    timers.enqueue(Timer(System.currentTimeMillis() + delayMillis, () => action))

  private def openChannel(): SocketChannel = {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    configure(channel)
    track(channel)
    channel
  }

  private def configure(channel: SocketChannel): Unit = {
    try {
      channel.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, true)
      channel.setOption[Integer](StandardSocketOptions.SO_RCVBUF, SocketBufferSize)
      channel.setOption[Integer](StandardSocketOptions.SO_SNDBUF, SocketBufferSize)
    } catch {
      case NonFatal(failure) => log.debug(s"$threadName could not configure a channel", failure)
    }
  }

  /** One admitted link, released exactly once however its life ends. */
  private final class LinkSlot {
    private val released = new AtomicBoolean(false)

    def release(): Unit = if (released.compareAndSet(false, true)) liveLinks.decrementAndGet()
  }

  /** Admit a link only while the topology could still need one. */
  private def admit(): Option[LinkSlot] = {
    if (liveLinks.get() >= maxLinks) {
      None
    } else {
      liveLinks.incrementAndGet()
      Some(new LinkSlot)
    }
  }

  private def splice(first: SocketChannel, second: SocketChannel, slot: LinkSlot): Unit =
    new RelayLink(first, second, slot).register()

  /** Open, register, and start a dial, so a synchronous failure follows the same policy as a late one. */
  private def beginDial(address: InetSocketAddress,
                        onConnected: SocketChannel => Unit,
                        onFailed: Throwable => Unit): Unit = {
    val opened = try {
      Some(openChannel())
    } catch {
      case NonFatal(failure) =>
        onFailed(failure)
        None
    }
    opened.foreach { channel =>
      try {
        val key = channel.register(selector, SelectionKey.OP_CONNECT, new Dialer(channel, onConnected, onFailed))
        // A connect to an address with no route fails here rather than through the selector.
        if (connectAttempt(channel, address)) {
          key.interestOps(0)
          onConnected(channel)
        }
      } catch {
        case NonFatal(failure) =>
          closeQuietly(channel)
          onFailed(failure)
      }
    }
  }

  private def retryOrGiveUp(deadline: Long,
                            interval: Long,
                            failure: Throwable,
                            retry: Long => Unit,
                            giveUp: Throwable => Unit): Unit = {
    if (!closed.get()) {
      if (System.currentTimeMillis() >= deadline) {
        giveUp(failure)
      } else {
        scheduleAt(interval)(retry(math.min(interval * 2, MaxRetryIntervalMillis)))
      }
    }
  }

  private def dialNative(address: InetSocketAddress, peerRank: Int, deadline: Long, interval: Long): Unit = {
    if (!closed.get()) {
      beginDial(address,
        channel => {
          sendRank(channel, peerRank)
          log.info(s"$threadName claimed the native link slot for rank $peerRank")
          nativeLinkReady(channel, peerRank)
        },
        failure => retryOrGiveUp(deadline, interval, failure,
          next => dialNative(address, peerRank, deadline, next),
          giveUp => nativeLinkFailed(peerRank, giveUp)))
    }
  }

  private def dialPeer(native: SocketChannel,
                       slot: LinkSlot,
                       address: InetSocketAddress,
                       description: String,
                       deadline: Long,
                       interval: Long): Unit = {
    if (!closed.get()) {
      beginDial(address,
        channel => splice(native, channel, slot),
        failure => retryOrGiveUp(deadline, interval, failure,
          next => dialPeer(native, slot, address, description, deadline, next),
          giveUp => {
            recordTerminalFailure(s"could not reach $description, so its LightGBM link cannot be relayed", giveUp)
            slot.release()
            closeQuietly(native)
          }))
    }
  }

  private def sendRank(channel: SocketChannel, rank: Int): Unit = {
    val buffer = rankBuffer(rank)
    buffer.flip()

    @tailrec
    def write(attemptsLeft: Int): Unit = {
      if (buffer.hasRemaining && attemptsLeft > 0) {
        channel.write(buffer)
        write(attemptsLeft - 1)
      }
    }

    write(RankBytes)
    if (buffer.hasRemaining) throw new IOException(s"$threadName could not send a rank in one write")
  }

  /** Hand a peer's connection to the native link slot already claimed for its rank. */
  private def linkPeerToNative(peer: SocketChannel, rank: Int, slot: LinkSlot): Unit = {
    primedNativeLinks.remove(rank) match {
      case Some(nativeChannel) => splice(peer, nativeChannel, slot)
      case None => peersAwaitingNativeLink.put(rank, (peer, slot)).foreach { case (previous, previousSlot) =>
        previousSlot.release()
        closeQuietly(previous)
      }
    }
  }

  private def nativeLinkReady(nativeChannel: SocketChannel, rank: Int): Unit = {
    peersAwaitingNativeLink.remove(rank) match {
      case Some((peer, slot)) => splice(peer, nativeChannel, slot)
      case None => primedNativeLinks.put(rank, nativeChannel).foreach(closeQuietly)
    }
  }

  private def nativeLinkFailed(rank: Int, failure: Throwable): Unit = {
    recordTerminalFailure(s"could not claim the native link slot for rank $rank, so the native listener " +
      "may keep waiting for a link that will never arrive", failure)
    peersAwaitingNativeLink.remove(rank).foreach { case (peer, slot) =>
      slot.release()
      closeQuietly(peer)
    }
  }

  private trait Handler {
    def onAcceptable(key: SelectionKey): Unit = ()
    def onConnectable(key: SelectionKey): Unit = ()
    def onReadWrite(key: SelectionKey): Unit = ()
    def onFailure(key: SelectionKey, failure: Throwable): Unit = {
      log.debug(s"$threadName closing a channel after a failure", failure)
      key.cancel()
      closeQuietly(key.channel())
    }
  }

  /** A listener outlives the connections it accepts, so their failures never close it. */
  private trait ListenerHandler extends Handler {
    override def onFailure(key: SelectionKey, failure: Throwable): Unit = {
      if (!key.channel().isOpen) {
        key.cancel()
        if (!closed.get()) recordTerminalFailure("lost a listening socket", failure)
      } else if (!closed.get()) {
        log.warn(s"$threadName ignored a failure while accepting; the listener stays open", failure)
      }
    }

    /** Handle one accepted connection without ever letting its failure reach the listener. */
    protected def guard(accepted: SocketChannel, slot: Option[LinkSlot])(work: => Unit): Unit = {
      try {
        work
      } catch {
        case NonFatal(failure) =>
          log.warn(s"$threadName dropped an accepted connection", failure)
          slot.foreach(_.release())
          closeQuietly(accepted)
      }
    }
  }

  /** Accepts peer connections, which have to pass the rank handshake before they are relayed. */
  private final class InboundAcceptor extends ListenerHandler {
    override def onAcceptable(key: SelectionKey): Unit = {
      val listener = key.channel().asInstanceOf[ServerSocketChannel]
      Option(listener.accept()).foreach { peer =>
        // Whether a link is wanted at all is decided before admission, so refusing one here can
        // never consume a slot that the outbound links to peers also draw on.
        if (expectedInboundRanks.isEmpty) {
          log.warn(s"$threadName refused a connection from ${remoteAddress(peer)}: this task is the lowest " +
            "rank, so no machine opens a link to it")
          closeQuietly(peer)
        } else {
          admitPeer(peer)
        }
      }
    }

    private def admitPeer(peer: SocketChannel): Unit = {
      admit() match {
        case None =>
          log.warn(s"$threadName refused a connection from ${remoteAddress(peer)}: this task expects " +
            s"${expectedInboundRanks.size} inbound links and already holds ${liveLinks.get()}")
          closeQuietly(peer)
        case Some(slot) => guard(peer, Some(slot))(beginHandshake(peer, slot))
      }
    }

    private def beginHandshake(peer: SocketChannel, slot: LinkSlot): Unit = {
      peer.configureBlocking(false)
      configure(peer)
      track(peer)
      val handshake = new RankHandshake(peer, slot)
      peer.register(selector, SelectionKey.OP_READ, handshake)
      scheduleAt(handshakeTimeoutMillis)(handshake.onDeadline())
    }
  }

  /** Reads and validates the rank a peer sends before any of its bytes reach the native library. */
  private final class RankHandshake(peer: SocketChannel, slot: LinkSlot) extends Handler {
    private val buffer = ByteBuffer.allocate(RankBytes)
    private var settled = false

    override def onReadWrite(key: SelectionKey): Unit = {
      val count = peer.read(buffer)
      if (count < 0) {
        reject(key, "closed the connection before sending its rank")
      } else if (!buffer.hasRemaining) {
        complete(key)
      }
    }

    def onDeadline(): Unit = {
      if (!settled) {
        settled = true
        slot.release()
        log.warn(s"$threadName closed a connection from ${remoteAddress(peer)} that did not send a " +
          s"LightGBM rank within ${handshakeTimeoutMillis}ms")
        closeQuietly(peer)
      }
    }

    override def onFailure(key: SelectionKey, failure: Throwable): Unit = reject(key, s"failed: $failure")

    private def complete(key: SelectionKey): Unit = {
      buffer.flip()
      val rank = buffer.order(ByteOrder.nativeOrder()).getInt
      if (!expectedInboundRanks.contains(rank)) {
        reject(key, s"claimed rank $rank, which is not one of the lower ranks this task expects")
      } else if (!linkedRanks.add(rank)) {
        reject(key, s"claimed rank $rank, which is already linked")
      } else {
        settled = true
        // The key is reused by the relayed link, so it must not be cancelled here.
        key.interestOps(0)
        log.info(s"$threadName accepted the link from rank $rank at ${remoteAddress(peer)}")
        linkPeerToNative(peer, rank, slot)
      }
    }

    private def reject(key: SelectionKey, reason: String): Unit = {
      if (!settled) {
        settled = true
        slot.release()
        log.warn(s"$threadName refused a connection from ${remoteAddress(peer)}: it $reason")
      }
      key.cancel()
      closeQuietly(peer)
    }
  }

  /** Accepts the native library's link to one peer and forwards it to that peer's real address. */
  private final class OutboundAcceptor(target: InetSocketAddress, description: String) extends ListenerHandler {
    override def onAcceptable(key: SelectionKey): Unit = {
      val listener = key.channel().asInstanceOf[ServerSocketChannel]
      Option(listener.accept()).foreach { native =>
        admit() match {
          case None =>
            log.warn(s"$threadName refused a native link to $description beyond the $maxLinks links " +
              "this topology can need")
            closeQuietly(native)
          case Some(slot) => guard(native, Some(slot)) {
            native.configureBlocking(false)
            configure(native)
            track(native)
            native.register(selector, 0, new Handler {})
            dialPeer(native, slot, target, description,
              System.currentTimeMillis() + connectTimeoutMillis, RetryIntervalMillis)
          }
        }
      }
    }
  }

  /** A non blocking connect whose outcome, early or late, is handed to the same policy. */
  private final class Dialer(channel: SocketChannel,
                             onConnected: SocketChannel => Unit,
                             onFailed: Throwable => Unit) extends Handler {
    override def onConnectable(key: SelectionKey): Unit = {
      if (channel.finishConnect()) {
        key.interestOps(0)
        onConnected(channel)
      }
    }

    override def onFailure(key: SelectionKey, failure: Throwable): Unit = {
      key.cancel()
      closeQuietly(channel)
      onFailed(failure)
    }
  }

  private def remoteAddress(channel: SocketChannel): String =
    try {
      Option(channel.getRemoteAddress).map(_.toString).getOrElse("an unknown address")
    } catch {
      case NonFatal(_) => "an unknown address"
    }

  /** Two channels relayed in both directions, closed together. */
  private final class RelayLink(first: SocketChannel, second: SocketChannel, slot: LinkSlot) {
    private val forward = new Direction(first, second)
    private val backward = new Direction(second, first)
    private val ended = new AtomicBoolean(false)

    def register(): Unit = {
      first.register(selector, SelectionKey.OP_READ, new LinkEnd(this))
      second.register(selector, SelectionKey.OP_READ, new LinkEnd(this))
      pump()
    }

    def pump(): Unit = {
      forward.pump()
      backward.pump()
      if (forward.finished && backward.finished) {
        finish()
      } else {
        updateInterest(first)
        updateInterest(second)
      }
    }

    /** Any I/O failure ends both directions, so the other one can never wait on a dead channel. */
    def abort(failure: Throwable): Unit = {
      log.debug(s"$threadName aborting a relayed link", failure)
      finish()
    }

    private def finish(): Unit = {
      if (ended.compareAndSet(false, true)) {
        slot.release()
        closeQuietly(first)
        closeQuietly(second)
      }
    }

    private def updateInterest(channel: SocketChannel): Unit = {
      val key = channel.keyFor(selector)
      if (key != None.orNull && key.isValid) {
        val reading = Seq(forward, backward).find(_.source eq channel).exists(_.wantsRead)
        val writing = Seq(forward, backward).find(_.target eq channel).exists(_.wantsWrite)
        val ops = (if (reading) SelectionKey.OP_READ else 0) | (if (writing) SelectionKey.OP_WRITE else 0)
        key.interestOps(ops)
      }
    }
  }

  private final class LinkEnd(link: RelayLink) extends Handler {
    override def onReadWrite(key: SelectionKey): Unit = link.pump()

    override def onFailure(key: SelectionKey, failure: Throwable): Unit = link.abort(failure)
  }

  /** One half of a relayed link: read into a fixed buffer, write it out, then propagate the close. */
  private final class Direction(val source: SocketChannel, val target: SocketChannel) {
    private val buffer = ByteBuffer.allocate(BufferSize)
    private var sourceEnded = false
    private var targetShutdown = false

    def wantsRead: Boolean = !sourceEnded && buffer.hasRemaining

    def wantsWrite: Boolean = buffer.position() > 0

    def finished: Boolean = sourceEnded && buffer.position() == 0 && targetShutdown

    def pump(): Unit = {
      if (wantsRead && source.read(buffer) < 0) sourceEnded = true
      buffer.flip()
      if (buffer.hasRemaining) target.write(buffer)
      buffer.compact()
      // A clean end of stream is passed on as a half close, which is what LightGBM sends.
      if (sourceEnded && buffer.position() == 0 && !targetShutdown) {
        target.shutdownOutput()
        targetShutdown = true
      }
    }
  }
}
