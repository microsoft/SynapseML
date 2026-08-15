// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMNetworkBridge, LightGBMUtils,
  NetworkManager, NetworkTopologyInfo}
import com.microsoft.ml.lightgbm.{lightgbmlib, lightgbmlibConstants}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.io.DataInputStream
import java.net.{InetAddress, InetSocketAddress, NetworkInterface, ServerSocket, Socket}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, ThreadFactory, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

/** Drives the real native LightGBM network over IPv6.
  *
  * The native network state is thread local, so one JVM can hold several LightGBM ranks as long as
  * each one stays on its own thread. These tests use that to run a real two worker training round
  * over IPv6, and to check the single rank link behavior against simulated peers.
  */
class LightGBMIpv6NetworkE2ESuite extends AnyFunSuite with BeforeAndAfterEach {

  private val log = LoggerFactory.getLogger(classOf[LightGBMIpv6NetworkE2ESuite])
  private val ipv6Loopback = "::1"
  private val ipv4Loopback = "127.0.0.1"
  private val socketTimeoutMillis = 30000L
  private val connectAttemptTimeoutMillis = 1000
  private val nativeWorkTimeoutMillis = 180000L
  private val retryIntervalMillis = 10L
  private val rankSize = 4
  private val trainingRows = 64
  private val trainingCols = 2
  private val trainingIterations = 5
  private val peerLabelOffset = 100.0f
  private val modelBufferLength = 1L << 20
  private val closeables = new ConcurrentLinkedQueue[AutoCloseable]()

  private val peerExecutor = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, "lightgbm-ipv6-e2e-peer")
      thread.setDaemon(true)
      thread
    }
  })

  override def beforeEach(): Unit = {
    super.beforeEach()
    LightGBMUtils.initializeNativeLibrary()
  }

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

  /** An address the native library also finds in its own local address list, on every platform. */
  private def localIpv4Host: String = {
    Try(NetworkInterface.getNetworkInterfaces.asScala
      .filter(candidate => Try(candidate.isUp).getOrElse(false))
      .flatMap(_.getInetAddresses.asScala)
      .find(address => address.isSiteLocalAddress && address.getAddress.length == 4)
      .map(_.getHostAddress)).toOption.flatten.getOrElse(ipv4Loopback)
  }

  private def freePort(host: String): Int = {
    val probe = new ServerSocket()
    try {
      probe.bind(new InetSocketAddress(InetAddress.getByName(host), 0))
      probe.getLocalPort
    } finally {
      probe.close()
    }
  }

  private def listenOn(host: String): ServerSocket = {
    val listener = new ServerSocket()
    listener.bind(new InetSocketAddress(InetAddress.getByName(host), 0))
    listener.setSoTimeout(socketTimeoutMillis.toInt)
    register(listener)
  }

  private def rankBytes(rank: Int): Array[Byte] =
    ByteBuffer.allocate(rankSize).order(ByteOrder.nativeOrder()).putInt(rank).array()

  private def readRank(socket: Socket): Int = {
    val buffer = new Array[Byte](rankSize)
    new DataInputStream(socket.getInputStream).readFully(buffer)
    ByteBuffer.wrap(buffer).order(ByteOrder.nativeOrder()).getInt
  }

  /** Stand in for a machine LightGBM expects to dial this task, which every lower rank does. */
  private def startDialingPeer(host: String, port: Int, rank: Int): CountDownLatch = {
    val linked = new CountDownLatch(1)
    peerExecutor.execute(new Runnable {
      override def run(): Unit = {
        val deadline = System.currentTimeMillis() + socketTimeoutMillis

        @tailrec
        def dial(): Unit = {
          val socket = new Socket()
          val connected = try {
            socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), connectAttemptTimeoutMillis)
            register(socket)
            socket.getOutputStream.write(rankBytes(rank))
            socket.getOutputStream.flush()
            true
          } catch {
            case _: Exception =>
              Try(socket.close())
              false
          }
          if (connected) {
            linked.countDown()
            // Hold the link open: LightGBM keeps every peer socket for the whole training run.
            Thread.sleep(socketTimeoutMillis)
          } else if (System.currentTimeMillis() < deadline) {
            Thread.sleep(retryIntervalMillis)
            dial()
          }
        }

        dial()
      }
    })
    linked
  }

  /** Stand in for a machine LightGBM dials, which every higher rank is. */
  private def startAcceptingPeer(listener: ServerSocket, receivedRank: AtomicInteger): CountDownLatch = {
    val linked = new CountDownLatch(1)
    peerExecutor.execute(new Runnable {
      override def run(): Unit = {
        val socket = register(listener.accept())
        receivedRank.set(readRank(socket))
        linked.countDown()
        Thread.sleep(socketTimeoutMillis)
      }
    })
    linked
  }

  private def nativeNetworkInit(machineList: String, localListenPort: Int, machineCount: Int): Int =
    lightgbmlib.LGBM_NetworkInit(machineList, localListenPort, LightGBMConstants.DefaultListenTimeout, machineCount)

  /** Run native work on its own thread, because LightGBM keeps its network state thread local. */
  private def onNativeThread[T](name: String)(work: => T): () => T = {
    val outcome = new AtomicReference[Either[Throwable, T]]()
    val done = new CountDownLatch(1)
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          outcome.set(Right(work))
        } catch {
          case failure: Throwable => outcome.set(Left(failure))
        } finally {
          done.countDown()
        }
      }
    }, name)
    thread.setDaemon(true)
    thread.start()
    () => {
      assert(done.await(nativeWorkTimeoutMillis, TimeUnit.MILLISECONDS), s"$name never finished")
      outcome.get() match {
        case Right(result) => result
        case Left(failure) => throw failure
      }
    }
  }

  private def await[T](pending: () => T): T = pending()

  /** Initialize the production network path, run the body, and always release the native network. */
  private def withNativeNetwork[T](topology: NetworkTopologyInfo, machineCount: Int)(body: => T): T = {
    var initialized = false
    try {
      NetworkManager.initNativeNetwork(topology, machineCount, log)
      initialized = true
      body
    } finally {
      if (initialized) lightgbmlib.LGBM_NetworkFree()
      topology.releaseNetworkResources()
    }
  }

  /** Train a tiny model on this rank's shard, which allreduces with every peer on every iteration. */
  private def trainShard(rank: Int, numMachines: Int): String = {
    val features = lightgbmlib.new_doubleArray((trainingRows * trainingCols).toLong)
    val labels = lightgbmlib.new_floatArray(trainingRows.toLong)
    (0 until trainingRows).foreach { row =>
      lightgbmlib.doubleArray_setitem(features, (row * trainingCols).toLong, row.toDouble)
      lightgbmlib.doubleArray_setitem(features, (row * trainingCols + 1).toLong, (row % 8).toDouble)
      // Each rank holds a clearly different slice, so a model built from both is easy to tell apart.
      lightgbmlib.floatArray_setitem(labels, row.toLong, row.toFloat + rank * peerLabelOffset)
    }

    val datasetParams = "max_bin=15 min_data_in_bin=1 min_data_in_leaf=1 verbosity=-1"
    val datasetOut = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(
      lightgbmlib.double_to_voidp_ptr(features),
      lightgbmlibConstants.C_API_DTYPE_FLOAT64,
      trainingRows,
      trainingCols,
      1,
      datasetParams,
      None.orNull,
      datasetOut), "Dataset create")
    val dataset = lightgbmlib.voidpp_value(datasetOut)
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSetField(dataset, "label",
      lightgbmlib.float_to_voidp_ptr(labels), trainingRows, lightgbmlibConstants.C_API_DTYPE_FLOAT32),
      "Dataset set label")

    val boosterOut = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterCreate(dataset,
      s"objective=regression tree_learner=data num_machines=$numMachines num_leaves=4 learning_rate=0.5 " +
        s"$datasetParams num_threads=1", boosterOut), "Booster create")
    val booster = lightgbmlib.voidpp_value(boosterOut)
    val isFinished = lightgbmlib.new_intp()
    val modelLength = lightgbmlib.new_int64_tp()
    try {
      (0 until trainingIterations).foreach(_ =>
        LightGBMUtils.validate(lightgbmlib.LGBM_BoosterUpdateOneIter(booster, isFinished), "Update one iteration"))
      lightgbmlib.LGBM_BoosterSaveModelToStringSWIG(booster, 0, -1, 0, modelBufferLength, modelLength)
    } finally {
      lightgbmlib.delete_intp(isFinished)
      lightgbmlib.delete_int64_tp(modelLength)
      lightgbmlib.LGBM_BoosterFree(booster)
      lightgbmlib.LGBM_DatasetFree(dataset)
      lightgbmlib.delete_doubleArray(features)
      lightgbmlib.delete_floatArray(labels)
    }
  }

  private def leafValues(model: String): Seq[Double] = {
    model.split("\n").filter(_.startsWith("leaf_value=")).flatMap(line =>
      line.stripPrefix("leaf_value=").trim.split("\\s+").filter(_.nonEmpty).map(_.toDouble)).toSeq
  }

  test("Without a bridge the native library cannot form a network from any IPv6 machine list") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val selfPort = freePort(ipv6Loopback)
    val peerPort = freePort(ipv6Loopback)
    // The bracketed form is what an IPv6 aware driver publishes.
    val bracketed = nativeNetworkInit(s"[$ipv6Loopback]:$selfPort,[$ipv6Loopback]:$peerPort", selfPort, 2)
    assert(bracketed == -1, "The native library unexpectedly accepted a bracketed IPv6 machine list")
    assert(lightgbmlib.LGBM_GetLastError().contains("Cannot find any ip and port"))

    // The bare form is what the driver published before this change: the native parser splits it on
    // ':' and keeps a meaningless host instead of rejecting the entry.
    val bare = nativeNetworkInit(s"$ipv6Loopback:$selfPort,$ipv6Loopback:$peerPort", selfPort, 2)
    assert(bare == -1, "The native library unexpectedly accepted a bare IPv6 machine list")
    assert(lightgbmlib.LGBM_GetLastError().contains("doesn't contain the local machine"))

    // A routable IPv6 address fares no better, so this is not a loopback quirk.
    val routable = nativeNetworkInit(s"[2001:db8::1]:$selfPort,[2001:db8::2]:$peerPort", selfPort, 2)
    assert(routable == -1, "The native library unexpectedly accepted a routable IPv6 machine list")
  }

  test("An IPv4 topology still initializes natively with no bridge and no rewriting") {
    val host = localIpv4Host
    val selfPort = freePort(ipv4Loopback)
    val peerListener = listenOn(ipv4Loopback)
    val receivedRank = new AtomicInteger(-1)
    val peerLinked = startAcceptingPeer(peerListener, receivedRank)

    val machineList = s"$host:$selfPort,$ipv4Loopback:${peerListener.getLocalPort}"
    val topology = NetworkTopologyInfo(machineList, Array(0), selfPort).withAdvertisedHost(host)
    await(onNativeThread("lightgbm-ipv4-rank") {
      withNativeNetwork(topology, 2) {
        assert(!topology.hasNetworkBridge, "An IPv4 topology has to reach the native library untouched")
        assert(peerLinked.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
          "The native library never linked to its IPv4 peer")
        assert(receivedRank.get() == 0, "The native library announced the wrong rank to its peer")
      }
    })
  }

  test("A LightGBM worker links to IPv6 peers in both directions through the bridge") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    // Rank 1 of 3 is the only rank that both accepts a link (from rank 0) and dials one (to rank 2).
    val advertisedPort = freePort(ipv6Loopback)
    val lowerRankPort = freePort(ipv6Loopback)
    val higherRankListener = listenOn(ipv6Loopback)
    val receivedRank = new AtomicInteger(-1)
    val higherRankLinked = startAcceptingPeer(higherRankListener, receivedRank)
    val lowerRankLinked = startDialingPeer(ipv6Loopback, advertisedPort, 0)

    val machineList = s"[$ipv6Loopback]:$lowerRankPort,[$ipv6Loopback]:$advertisedPort," +
      s"[$ipv6Loopback]:${higherRankListener.getLocalPort}"
    val topology = NetworkTopologyInfo(machineList, Array(0), advertisedPort).withAdvertisedHost(ipv6Loopback)
    await(onNativeThread("lightgbm-ipv6-rank") {
      withNativeNetwork(topology, 3) {
        assert(topology.hasNetworkBridge, "An IPv6 topology has to be bridged onto the native transport")
        assert(lowerRankLinked.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
          "The lower rank never linked to this worker over IPv6")
        assert(higherRankLinked.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
          "This worker never linked to the higher rank over IPv6")
        assert(receivedRank.get() == 1,
          s"The higher rank received rank ${receivedRank.get()} instead of the bridged worker's rank 1")
      }
    })
  }

  test("The native listener stops accepting once the bridge has claimed its link slots") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    // Rank 1 of 2, so the native listener has exactly one slot, which the bridge claims from loopback.
    val advertisedPort = freePort(ipv6Loopback)
    val machineList = s"[$ipv6Loopback]:${freePort(ipv6Loopback)},[$ipv6Loopback]:$advertisedPort"
    val bridge = register(LightGBMNetworkBridge.open(machineList, ipv6Loopback, advertisedPort, log))
    val bridged = bridge.bridgedNetwork
    val lowerRankLinked = startDialingPeer(ipv6Loopback, advertisedPort, 0)

    await(onNativeThread("lightgbm-native-listener") {
      val result = nativeNetworkInit(bridged.machineList, bridged.localListenPort, bridged.machineCount)
      try {
        assert(result == 0, s"Native init failed: ${lightgbmlib.LGBM_GetLastError()}")
        assert(lowerRankLinked.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
          "The lower rank never linked through the bridge")
      } finally {
        if (result == 0) lightgbmlib.LGBM_NetworkFree()
      }
    })

    // The native library closes its listener as soon as its slots are filled, and only the bridge
    // ever filled them, so nothing external can still reach that port.
    val probe = new Socket()
    val refused = try {
      probe.connect(new InetSocketAddress(InetAddress.getByName(ipv4Loopback), bridged.localListenPort),
        connectAttemptTimeoutMillis)
      false
    } catch {
      case _: java.io.IOException => true
    } finally {
      Try(probe.close())
    }
    assert(refused, s"The native listener on port ${bridged.localListenPort} is still accepting connections")
  }

  test("Two LightGBM workers train one distributed model over IPv6") {
    assume(ipv6LoopbackAvailable, "IPv6 loopback is not available on this machine")
    val firstPort = freePort(ipv6Loopback)
    val secondPort = freePort(ipv6Loopback)
    val machineList = s"[$ipv6Loopback]:$firstPort,[$ipv6Loopback]:$secondPort"

    def worker(rank: Int, advertisedPort: Int): () => String = {
      val topology = NetworkTopologyInfo(machineList, Array(rank), advertisedPort).withAdvertisedHost(ipv6Loopback)
      onNativeThread(s"lightgbm-ipv6-worker-$rank") {
        withNativeNetwork(topology, 2) {
          assert(topology.hasNetworkBridge, s"Worker $rank did not bridge its IPv6 topology")
          trainShard(rank, 2)
        }
      }
    }

    // Both workers have to be running before either can link, exactly as two Spark tasks would be.
    val pendingFirst = worker(0, firstPort)
    val pendingSecond = worker(1, secondPort)
    val firstModel = await(pendingFirst)
    val secondModel = await(pendingSecond)

    assert(firstModel.contains("Tree=0"), "The distributed training produced no trees")
    assert(firstModel.contains("split_feature="), "The distributed training produced only empty trees")
    assert(firstModel == secondModel,
      "Data parallel LightGBM builds the same model on every rank, so the two IPv6 workers disagreeing " +
        "means their allreduce traffic did not cross the bridge")

    // A model built from only one shard cannot see the peer's labels, so its leaves sit far lower.
    val localModel = await(onNativeThread("lightgbm-single-machine")(trainShard(0, 1)))
    val distributedMean = leafValues(firstModel).sum / leafValues(firstModel).length
    val localMean = leafValues(localModel).sum / leafValues(localModel).length
    assert(math.abs(distributedMean - localMean) > 1.0,
      s"The distributed leaves ($distributedMean) match a single machine model ($localMean), so the " +
        "peer's data never reached this worker")
  }
}
