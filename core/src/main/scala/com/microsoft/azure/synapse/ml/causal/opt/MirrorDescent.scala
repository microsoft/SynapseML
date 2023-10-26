// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.opt

import breeze.optimize.DiffFunction
import com.microsoft.azure.synapse.ml.causal.CacheOps
import com.microsoft.azure.synapse.ml.causal.linalg.VectorOps
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

/**
  * Solver for optimization problem with constraint 1^T^x = 1, x > 0.
  *  is provided by vectorOps.
  * @param vectorOps Provides implementation for vector operations
  * @param cacheOps Provides optimization for Spark
  */
private[opt] class MirrorDescent[TVec](private val func: DiffFunction[TVec],
                          step: Double,
                          maxIter: Int,
                          numIterNoChange: Option[Int] = None,
                          tol: Double = 1E-4
                         )(implicit vectorOps: VectorOps[TVec], cacheOps: CacheOps[TVec]) {

  if (step <= 0) throw new IllegalArgumentException("step must be positive")
  if (maxIter <= 0) throw new IllegalArgumentException("maxIter must be positive")
  if (tol <= 0) throw new IllegalArgumentException("tol must be positive")
  if (!numIterNoChange.forall(_ > 0)) {
    throw new IllegalArgumentException("numIterNoChange must be positive if defined.")
  }

  private[causal] case class State(x: TVec, valueAt: Double, gradientAt: TVec)

  private object State{
    def apply(x: TVec): State = {
      val (valueAt, gradientAt) = func.calculate(x)
      State(x, valueAt, gradientAt)
    }
  }

  @transient private[causal] val history: ArrayBuffer[State] = ArrayBuffer.empty
  @transient private lazy val logger = LogManager.getLogger(getClass.getName)

  private def solveSingleIteration(curr: State, i: Int): State = {
    time {
      logger.info(s"iteration: $i, loss: ${curr.valueAt}")

      val currStep = step / math.sqrt(i)

      val gradCheckpoint = checkpoint(curr.gradientAt)

      // t = (grad /:/ max(abs(grad)) * (-currStep)).map(math.exp) *:* x
      val t = cache {
        // normalize the gradients by max(abs(gradients)) for numerical stability
        val normalized = vectorOps.axpy(gradCheckpoint, None, -currStep / vectorOps.maxAbs(gradCheckpoint))
        vectorOps.elementwiseProduct(vectorOps.exp(normalized), curr.x)
      }

      // Renormalization to enforce constraint.
      // t /:/ sum(t)
      val x = checkpoint {
        vectorOps.axpy(t, None, 1 / vectorOps.sum(t))
      }

      val state = State(x)
      history.append(state)
      state
    }
  }

  def solve(init: TVec): TVec = {
    val initState = State(init)
    history.append(initState)

    val (result, _) = (1 to maxIter).foldLeft((initState, false)) {
      case ((curr, true), _) =>
        (curr, true) // terminated

      case ((curr, false), i) =>
        val result = solveSingleIteration(curr, i)

        val terminate = numIterNoChange.map(shouldTerminateEarly(_, tol))
        if (terminate.exists(_._1)) {
          // numIterNoChange is defined, and should terminate
          // return the best state so far and terminate
          (terminate.get._2, true)
        } else {
          // next iteration
          (result, false)
        }
    }

    result.x
  }

  private def shouldTerminateEarly(numIterNoChange: Int, tol: Double): (Boolean, State) = {
    assert(history.nonEmpty)

    if (history.size < numIterNoChange) {
      // If history buffer does not have enough iterations, return false
      (false, history.minBy(_.valueAt))
    } else {
      val lastIterations = history.takeRight(numIterNoChange)
      val minLastValueAt = lastIterations.map(_.valueAt).min

      val firstValueAtInLastIterations = lastIterations.head.valueAt

      // Check if the minimal loss is better (less) than the first iteration in the last numIterNoChange
      // iterations by at least tol. Terminate if it's not improving.
      if (minLastValueAt < firstValueAtInLastIterations - tol)
        (false, lastIterations.minBy(_.valueAt))
      else
        (true, lastIterations.minBy(_.valueAt))
    }
  }


  private def cache[R: CacheOps](block: => R): R = {
    val result = block
    val cacheOps = implicitly[CacheOps[R]]
    cacheOps.cache(result)
  }

  private def checkpoint[R: CacheOps](block: => R): R = {
    val result = block
    val cacheOps = implicitly[CacheOps[R]]
    cacheOps.checkpoint(result)
  }

  private def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()

    logger.debug("Elapsed time: " + (t1 - t0) / 1E9 + "s")
    result
  }
}
