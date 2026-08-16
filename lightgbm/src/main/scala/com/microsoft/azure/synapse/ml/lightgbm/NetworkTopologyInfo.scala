// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.net.Socket

case class TaskMessageInfo(status: String,
                           taskHost: String,
                           localListenPort: Int,
                           partitionId: Int,
                           executorId: String) {
  def this(status: String) = this(status, "", -1, -1, "") // Constructor for general messages, not Task-connected

  val isForTraining: Boolean = status == LightGBMConstants.EnabledTask
  val isForLoadOnly: Boolean = status == LightGBMConstants.IgnoreStatus
  val isFinished: Boolean = status == LightGBMConstants.FinishedStatus

  // Format all the information as a delimited string to send to driver
  override def toString: String = s"$status:$taskHost:$localListenPort:$partitionId:$executorId"
}

case class NetworkTopologyInfo(lightgbmNetworkString: String,
                               executorPartitionIdList: Array[Int],
                               localListenPort: Int) {
  @transient private var portReservation: Option[Socket] = None
  @transient private var networkBridge: Option[LightGBMNetworkBridge] = None
  @transient private var advertisedHost: String = ""

  private def currentPortReservation: Option[Socket] = Option(portReservation).flatten

  private def currentNetworkBridge: Option[LightGBMNetworkBridge] = Option(networkBridge).flatten

  /** The endpoint this task advertised to the driver, which is also its entry in the machine list.
    *
    * This is task local state rather than a constructor field, so the case class keeps the shape
    * every existing caller, extractor, and serialized form depends on.
    */
  private[lightgbm] def taskHost: String = Option(advertisedHost).getOrElse("")

  private[lightgbm] def withAdvertisedHost(host: String): NetworkTopologyInfo = synchronized {
    advertisedHost = Option(host).getOrElse("")
    this
  }

  private[lightgbm] def hasPortReservation: Boolean = synchronized {
    currentPortReservation.nonEmpty
  }

  private[lightgbm] def retainPortReservation(reservation: Socket): NetworkTopologyInfo = synchronized {
    require(!reservation.isClosed, "Cannot retain a closed port reservation")
    require(reservation.isBound, "Cannot retain an unbound port reservation")
    require(reservation.getLocalPort == localListenPort,
      s"Port reservation ${reservation.getLocalPort} does not match topology port $localListenPort")
    require(currentPortReservation.isEmpty, s"Port $localListenPort already has a reservation")
    portReservation = Option(reservation)
    this
  }

  /** Keep an IPv6 transport bridge alive for as long as the native network uses it. */
  private[lightgbm] def retainNetworkBridge(bridge: LightGBMNetworkBridge): NetworkTopologyInfo = synchronized {
    require(currentNetworkBridge.isEmpty, "This task already has a LightGBM network bridge")
    networkBridge = Option(bridge)
    this
  }

  private[lightgbm] def hasNetworkBridge: Boolean = synchronized {
    currentNetworkBridge.nonEmpty
  }

  /** Release the temporary JVM reservation immediately before LightGBM binds the same port.
    *
    * The operation is idempotent so final cleanup can safely call it after any success or failure path.
    */
  private[lightgbm] def releasePortReservation(): Unit = synchronized {
    currentPortReservation.foreach { reservation =>
      try {
        NetworkManager.closeSocketWithRetry(reservation)
      } finally {
        // Keep an open socket reachable for a later final-cleanup attempt.
        if (reservation.isClosed) portReservation = None
      }
    }
  }

  /** Tear down the IPv6 transport bridge, if this task needed one. Idempotent. */
  private[lightgbm] def releaseNetworkBridge(): Unit = synchronized {
    currentNetworkBridge.foreach { bridge =>
      try {
        bridge.close()
      } finally {
        networkBridge = None
      }
    }
  }

  /** Release every network resource this task owns, whichever transport it ended up using. */
  private[lightgbm] def releaseNetworkResources(): Unit = {
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(releaseNetworkBridge())(releasePortReservation())
  }
}

