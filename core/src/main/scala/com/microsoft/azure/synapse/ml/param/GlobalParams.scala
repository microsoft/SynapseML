package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.Row

import scala.collection.mutable
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils

trait GlobalKey[T] {
  val name: String
}

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalKey[_], Any] = mutable.Map.empty

  private val StringtoKeyMap: mutable.Map[String, GlobalKey[_]] = {
    val strToKeyMap = mutable.Map[String, GlobalKey[_]]()
    JarLoadingUtils.instantiateObjects[GlobalKey[_]]().foreach { key: GlobalKey[_] =>
      strToKeyMap += (key.name -> key)
    }
    strToKeyMap
  }

  private def findGlobalKeyByName(keyName: String): Option[GlobalKey[_]] = {
    StringtoKeyMap.get(keyName)
  }

  def setGlobalParam[T](key: GlobalKey[T], value: T): Unit = {
    GlobalParams(key) = value
  }

  def setGlobalParam[T](keyName: String, value: T): Unit = {
    val key = findGlobalKeyByName(keyName)
    key match {
      case Some(k) =>
        GlobalParams(k) = value
      case None => throw new IllegalArgumentException("${keyName} is not a valid key in GlobalParams")
    }
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

  def registerParam[T](sp: ServiceParam[T], key: GlobalKey[T]): Unit = {
    ParamToKeyMap(sp) = key
  }
}

trait HasGlobalParams extends Params{

  def getGlobalParam[T](p: Param[T]): T = {
    try {
      this.getOrDefault(p)
    }
    catch {
      case e: Exception =>
        GlobalParams.getParam(p) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }
}