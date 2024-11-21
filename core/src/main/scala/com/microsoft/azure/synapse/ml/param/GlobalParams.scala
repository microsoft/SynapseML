package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.Row

import scala.collection.mutable
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils

trait GlobalKey[T]

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalKey[_], Any] = mutable.Map.empty


  def setGlobalParam[T](key: GlobalKey[T], value: T): Unit = {
    GlobalParams(key) = value
  }

  private def getGlobalParam[T](key: GlobalKey[T]): Option[T] = {
    GlobalParams.get(key.asInstanceOf[GlobalKey[Any]]).map(_.asInstanceOf[T])
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
      .filter(p => !this.isSet(p) && !this.hasDefault(p))
      .foreach { p =>
        GlobalParams.getParam(p).foreach { v =>
          set(p.asInstanceOf[Param[Any]], v)
        }
      }
  }
}