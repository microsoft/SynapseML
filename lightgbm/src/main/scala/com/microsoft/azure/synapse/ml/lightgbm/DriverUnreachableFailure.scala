// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.slf4j.Logger

import java.net.ConnectException

private[lightgbm] object DriverUnreachableFailure {
  /** Explain why a task could not reach the driver's network topology endpoint.
    *
    * The driver serves the topology exchange exactly once per training round and then closes its
    * server socket. A task that Spark retries after that point can therefore only ever see
    * "connection refused", which silently replaces the failure that caused the retry in the first
    * place. Naming that explicitly keeps the original failure discoverable.
    */
  def apply(networkParams: NetworkParams,
            partitionId: Int,
            taskIdentity: WorkerTaskIdentity,
            log: Logger,
            cause: ConnectException): Exception = {
    val attemptNumber = taskIdentity.attemptNumber.getOrElse(0)
    val endpoint = s"${networkParams.ipAddress}:${networkParams.port}"
    val identity = NetworkManager.workerIdentitySummary(taskIdentity, partitionId)
    val message = if (attemptNumber > 0) {
      s"LightGBM task ($identity) could not reach the driver network topology endpoint " +
        s"$endpoint on retry attempt $attemptNumber. The driver serves the topology exchange once per training " +
        "round and has already closed it, so a retried task can never rejoin the LightGBM network. This error " +
        s"is therefore a consequence of an earlier failure: inspect the logs of the first failed attempt of " +
        s"partition $partitionId to find the real cause. Distributed LightGBM training cannot recover from a " +
        "partial task retry."
    } else {
      s"LightGBM task ($identity) could not reach the driver network topology endpoint " +
        s"$endpoint on its first attempt. Either executors cannot open connections to the driver on that " +
        "port, or the driver stopped waiting before this task reported, for example because numTasks is " +
        "larger than the number of tasks Spark can run at once. Check the driver log for a LightGBM " +
        "missing-tasks error."
    }
    log.error(message, cause)
    new Exception(message, cause)
  }
}
