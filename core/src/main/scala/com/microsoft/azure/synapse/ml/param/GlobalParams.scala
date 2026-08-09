// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}

import scala.collection.mutable

trait GlobalKey[T]

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[Any]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalKey[Any], Any] = mutable.Map.empty

  private def untypedKey[T](key: GlobalKey[T]): GlobalKey[Any] = {
    key.asInstanceOf[GlobalKey[Any]]
  }

  def setGlobalParam[T](key: GlobalKey[T], value: T): Unit = {
    GlobalParams(untypedKey(key)) = value
  }

  def getGlobalParam[T](key: GlobalKey[T]): Option[T] = {
    GlobalParams.get(untypedKey(key)).map(_.asInstanceOf[T])
  }

  def resetGlobalParam[T](key: GlobalKey[T]): Unit = {
    GlobalParams -= untypedKey(key)
  }

  def getParam[T](p: Param[T]): Option[T] = {
    ParamToKeyMap.get(p).flatMap(GlobalParams.get).map(_.asInstanceOf[T])
  }

  def registerParam[T](p: Param[T], key: GlobalKey[T]): Unit = {
    ParamToKeyMap(p) = untypedKey(key)
  }
}

trait HasGlobalParams extends Params{

  private[ml] def transferGlobalParamsToParamMap(): Unit = {
    // check for empty params. Fill em with GlobalParams
    this.params
      .filter(p => !this.isSet(p))
      .foreach { p =>
        GlobalParams.getParam(p).foreach { v =>
          set(p.asInstanceOf[Param[Any]], v)
        }
      }
  }
}
