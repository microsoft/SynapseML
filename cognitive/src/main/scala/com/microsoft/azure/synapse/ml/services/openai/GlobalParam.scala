package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.HasServiceParams
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Row

import scala.collection.mutable

sealed trait GlobalParamKey[T]
case object OpenAIDeploymentNameKey extends GlobalParamKey[String]

object GlobalParamObject {
  private val ParamToKeyMap: mutable.Map[Any, GlobalParamKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalParamKey[Any], Any] = mutable.Map.empty

  def setGlobalParamKey[T](key: GlobalParamKey[T], value: T): Unit = {
    GlobalParams(key.asInstanceOf[GlobalParamKey[Any]]) = value
  }

  def getGlobalParamKey[T](key: GlobalParamKey[T]): Option[T] = {
    GlobalParams.get(key.asInstanceOf[GlobalParamKey[Any]]).map(_.asInstanceOf[T])
  }

  def getParam[T](p: Param[T]): Option[T] = {
    ParamToKeyMap.get(p).flatMap { key =>
      key match {
        case k: GlobalParamKey[T] => getGlobalParamKey(k)
        case _ => None
      }
    }
  }

  def getServiceParam[T](sp: ServiceParam[T]): Option[T] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalParamKey[T] => getGlobalParamKey(k)
        case _ => None
      }
    }
  }

  def registerParam[T](p: Param[T], key: GlobalParamKey[T]): Unit = {
    ParamToKeyMap(p) = key
  }

  def registerServiceParam[T](sp: ServiceParam[T], key: GlobalParamKey[T]): Unit = {
    ParamToKeyMap(sp) = key
  }
}



trait HasGlobalParams extends HasServiceParams {

  def getGlobalParam[T](p: Param[T]): T = {
    try {
      this.getOrDefault(p)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getParam(p) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalParam[T](name: String): T = {
    val param = this.getParam(name).asInstanceOf[Param[T]]
    try {
      this.getOrDefault(param)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getParam(param) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamScalar[T](p: ServiceParam[T]): T = {
    try {
      this.getScalarParam(p)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getServiceParam(p) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamVector[T](p: ServiceParam[T]): T= { //TODO: should return a string
    try {
      this.getScalarParam(p)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getServiceParam(p)match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamScalar[T](name: String): T = {
    val serviceParam = this.getParam(name).asInstanceOf[ServiceParam[T]]
    try {
      this.getScalarParam(serviceParam)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getServiceParam(serviceParam) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamVector[T](name: String): T = { //TODO: should return a string
    val serviceParam = this.getParam(name).asInstanceOf[ServiceParam[T]]
    try {
      this.getScalarParam(serviceParam)
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getServiceParam(serviceParam) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  protected def getGlobalServiceParamValueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = {
    //TODO: Confirm with Mark what this colName business is about
    try {
      get(p).orElse(getDefault(p)).flatMap {
        case Right(colName) => Option(row.getAs[T](colName))
        case Left(value) => Some(value)
      }
      match {
        case some @ Some(_) => some
        case None => GlobalParamObject.getServiceParam(p)
      }
    }
    catch {
      case e: Exception =>
        GlobalParamObject.getServiceParam(p) match {
          case Some(v) => Some(v)
          case None => throw e
        }
    }
  }


  protected def getGlobalServiceParamValue[T](row: Row, p: ServiceParam[T]): T =
    getGlobalServiceParamValueOpt(row, p).get
}