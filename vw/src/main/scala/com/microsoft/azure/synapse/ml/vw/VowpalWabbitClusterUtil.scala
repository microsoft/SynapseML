// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.{ClusterUtil, ParamsStringBuilder}
import org.apache.spark.sql.SparkSession
import org.vowpalwabbit.spark.ClusterSpanningTree

import java.util.UUID

/**
  * Helper to spin up spanning tree coordinator for AllReduce.
  */
class VowpalWabbitClusterUtil(quiet: Boolean) {
  val spanningTree = new ClusterSpanningTree(0, quiet)

  spanningTree.start()

  private val driverHostAddress = ClusterUtil.getDriverHost(SparkSession.active)
  private val port = spanningTree.getPort

  def augmentVowpalWabbitArguments(vwArgs: ParamsStringBuilder,
                                   numTasks: Int,
                                   jobUniqueId: String =
                                      Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt).toString): Unit = {
    /*
    --span_server specifies the network address of a little server that sets up spanning trees over the nodes.
    --unique_id should be a number that is the same for all nodes executing a particular job and
      different for all others.
    --total is the total number of nodes.
    --node should be unique for each node and range from {0,total-1}.
    --holdout_off should be included for distributed training
    */
    vwArgs.appendParamFlagIfNotThere("holdout_off")
      .appendParamValueIfNotThere("span_server", Option(driverHostAddress))
      .appendParamValueIfNotThere("span_server_port", Option(port))
      .appendParamValueIfNotThere("unique_id", Option(jobUniqueId))
      .appendParamValueIfNotThere("total", Option(numTasks))
  }

  def stop(): Unit = spanningTree.stop()
}

object VowpalWabbitClusterUtil {
  lazy val Instance = new VowpalWabbitClusterUtil(false)
}
