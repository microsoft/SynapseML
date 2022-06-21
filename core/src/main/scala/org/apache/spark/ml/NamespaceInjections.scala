// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.param.shared.HasParallelism
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.util.ThreadUtils

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

object NamespaceInjections {

  def pipelineModel[T <: Transformer](stages: Array[T]): PipelineModel = {
    new PipelineModel(Identifiable.randomUID("PipelineModel"), stages.asInstanceOf[Array[Transformer]])
  }

}

object ImageInjections {

  def decode(origin: String, bytes: Array[Byte]): Option[Row] = ImageSchema.decode(origin, bytes)

}

object ParamInjections {
  trait HasParallelismInjected extends HasParallelism {

    def getExecutionContextProxy: ExecutionContext = {
      getExecutionContext
    }

    protected def awaitFutures[T](futures: Array[Future[T]]): Seq[T] = {
      futures.map(ThreadUtils.awaitResult(_, Duration.Inf))
    }
  }
}
