// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object FaultToleranceUtils {
  def retryWithTimeout[T](times: Int, timeout: Duration)(f: => T): T ={
    try {
      Await.result(Future(f)(ExecutionContext.global), timeout)
    } catch {
      case e: Exception if times >= 1 =>
        print(s"Received exception on call, retrying: $e")
        retryWithTimeout(times-1, timeout)(f)
    }
  }

  val Backoffs: Seq[Int] = Seq(0, 100, 200, 500)  //scalastyle:ignore magic.number

  def retryWithTimeout[T](times: Seq[Int] = Backoffs)(f: => T): T ={
    try {
      f
    } catch {
      case e: Exception if times.nonEmpty =>
        println(s"Received exception on call, retrying: $e")
        Thread.sleep(times.head)
        retryWithTimeout(times.tail)(f)
    }
  }

}
