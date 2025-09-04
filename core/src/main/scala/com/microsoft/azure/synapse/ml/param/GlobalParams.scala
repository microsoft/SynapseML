// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}

import scala.collection.mutable

trait GlobalKey[T]

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalKey[_], Any] = mutable.Map.empty


  def setGlobalParam[T](key: GlobalKey[T], value: T): Unit = {
    GlobalParams(key) = value
  }

  def getGlobalParam[T](key: GlobalKey[T]): Option[T] = {
    GlobalParams.get(key.asInstanceOf[GlobalKey[Any]]).map(_.asInstanceOf[T])
  }

  def resetGlobalParam[T](key: GlobalKey[T]): Unit = {
    GlobalParams -= key
  }

  def getParam[T](p: Param[T]): Option[T] = {
    ParamToKeyMap.get(p).flatMap { key =>
      key match {
        case k: GlobalKey[T] =>
          getGlobalParam(k)
        case _ => None
      }
    }
  }

  def registerParam[T](p: Param[T], key: GlobalKey[T]): Unit = {
    ParamToKeyMap(p) = key
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
