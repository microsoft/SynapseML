// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import org.apache.spark.sql.SparkSession

object SynapsePlatformUtils {
  val PlatformSynapse = "synapse"
  val PlatformSynapseInternal = "synapse_internal"

  def getClusterType: String = {
    sys.env.getOrElse("AZURE_SERVICE", None).toString match {
      case "Microsoft.ProjectArcadia" =>
        val spark = SparkSession.getActiveSession match {
          case s: Option[SparkSession] => s.get
          case _ => throw new Exception("SparkSession not existing!")
        }
        val sc = spark.sparkContext
        sc.getConf.get("spark.cluster.type")
      case _ => throw new Exception("Not inside Synapse")
    }
  }

  def currentPlatform: String = {
    getClusterType match {
      case PlatformSynapse => PlatformSynapse
      case _ => PlatformSynapseInternal
    }
  }

  def isSynapse: Boolean = currentPlatform == PlatformSynapse

  def isSynapseInternal: Boolean = currentPlatform == PlatformSynapseInternal

  def getAccessTokenInternal(resource: String = "pbi"): String = {
    val clusterType = getClusterType
    clusterType match {
      case PlatformSynapse => throw new Exception(s"Doesn't support getAccessTokenInternal on $PlatformSynapse yet!")
      case _ =>
        val clazz = java.lang.Class.forName(s"com.microsoft.azure.$clusterType.tokenlibrary.TokenLibrary$$")
        val module = clazz.getDeclaredField("MODULE$")
        val tokenLib = module.get(None)
        val getAccessTokenMethod = tokenLib.getClass.getMethod("getAccessToken", resource.getClass)
        getAccessTokenMethod.invoke(tokenLib, resource).toString
    }
  }

  def getTokenExpiryTime(token: String): java.util.Date = {
    val clusterType = getClusterType
    clusterType match {
      case PlatformSynapse => throw new Exception(s"Doesn't support getTokenExpiryTime on $PlatformSynapse yet!")
      case _ =>
        val clazz = java.lang.Class.forName(s"com.microsoft.azure.$clusterType.tokenlibrary.TokenLibrary$$")
        val module = clazz.getDeclaredField("MODULE$")
        val tokenLib = module.get(None)
        val getExpiryTimeMethod = tokenLib.getClass.getMethod("getExpiryTime", token.getClass)
        getExpiryTimeMethod.invoke(tokenLib, token).asInstanceOf[java.util.Date]
    }
  }

}
