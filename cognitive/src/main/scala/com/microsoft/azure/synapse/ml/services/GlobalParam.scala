package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Row

import scala.collection.mutable

sealed trait GlobalKey[T]
sealed trait GlobalParamKey[T] extends GlobalKey[T]
sealed trait GlobalServiceParamKey[T] extends GlobalKey[Either[T, String]] //left is Scalar, right is Vector

case object OpenAIDeploymentNameKey extends GlobalServiceParamKey[String]

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalParamKey[Any], Any] = mutable.Map.empty
  private val GlobalServiceParams: mutable.Map[GlobalServiceParamKey[Any], Either[Any, String]] = mutable.Map.empty

  private def setGlobalParamKey[T](key: GlobalParamKey[T], value: T): Unit = {
    GlobalParams(key.asInstanceOf[GlobalParamKey[Any]]) = value
  }

  private def setGlobalServiceParamKey[T](key: GlobalServiceParamKey[T], value: T): Unit = {
    GlobalServiceParams(key.asInstanceOf[GlobalServiceParamKey[Any]]) = Left(value)
  }

  private def setGlobalServiceParamKeyCol[T](key: GlobalServiceParamKey[T], value: String): Unit = {
    GlobalServiceParams(key.asInstanceOf[GlobalServiceParamKey[Any]]) = Right(value)
  }

  private def getGlobalParam[T](key: GlobalParamKey[T]): Option[T] = {
    GlobalParams.get(key.asInstanceOf[GlobalParamKey[Any]]).map(_.asInstanceOf[T])
  }

  private def getGlobalServiceParam[T](key: GlobalServiceParamKey[T]): Option[Either[T, String]] = {
    GlobalServiceParams.get(key.asInstanceOf[GlobalServiceParamKey[Any]]).map(_.asInstanceOf[Either[T, String]])
  }

  def getParam[T](p: Param[T]): Option[T] = {
    ParamToKeyMap.get(p).flatMap { key =>
      key match {
        case k: GlobalParamKey[T] => getGlobalParam(k)
        case _ => None
      }
    }
  }

  def getServiceParam[T](sp: ServiceParam[T]): Option[Either[T, String]] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalServiceParamKey[T] => getGlobalServiceParam(k)
        case _ => None
      }
    }
  }

  def getServiceParamScalar[T](sp: ServiceParam[T]): Option[T] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalServiceParamKey[T] =>
          getGlobalServiceParam(k) match {
            case Some(Left(value)) => Some(value)
            case _ => None
          }
        case _ => None
      }
    }
  }

  def getServiceParamVector[T](sp: ServiceParam[T]): Option[String] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalServiceParamKey[T] =>
          getGlobalServiceParam(k) match {
            case Some(Right(colName)) => Some(colName)
            case _ => None
          }
        case _ => None
      }
    }
  }

  def registerParam[T](p: Param[T], key: GlobalParamKey[T]): Unit = {
    ParamToKeyMap(p) = key
  }

  def registerServiceParam[T](sp: ServiceParam[T], key: GlobalServiceParamKey[T]): Unit = {
    ParamToKeyMap(sp) = key
  }

  def setDeploymentName(deploymentName: String): Unit = {
    setGlobalServiceParamKey(OpenAIDeploymentNameKey, deploymentName)
  }

  def setDeploymentNameCol(colName: String): Unit = {
    setGlobalServiceParamKeyCol(OpenAIDeploymentNameKey, colName)
  }
}



trait HasGlobalParams extends HasServiceParams {

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

  def getGlobalParam[T](name: String): T = {
    val param = this.getParam(name).asInstanceOf[Param[T]]
    try {
      this.getOrDefault(param)
    }
    catch {
      case e: Exception =>
        GlobalParams.getParam(param) match {
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
        GlobalParams.getServiceParamScalar(p) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamVector[T](p: ServiceParam[T]): String= {
    try {
      this.getVectorParam(p)
    }
    catch {
      case e: Exception =>
        GlobalParams.getServiceParamVector(p)match {
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
        GlobalParams.getServiceParamScalar(serviceParam) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  def getGlobalServiceParamVector[T](name: String): String = {
    val serviceParam = this.getParam(name).asInstanceOf[ServiceParam[T]]
    try {
      this.getVectorParam(serviceParam)
    }
    catch {
      case e: Exception =>
        GlobalParams.getServiceParamVector(serviceParam) match {
          case Some(v) => v
          case None => throw e
        }
    }
  }

  protected def getGlobalServiceParamValueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = {
    val globalParam: Option[T] = GlobalParams.getServiceParam(p).flatMap {
      case Right(colName) => Option(row.getAs[T](colName))
      case Left(value) => Some(value)
    }
    try {
      get(p).orElse(getDefault(p)).flatMap {
        case Right(colName) => Option(row.getAs[T](colName))
        case Left(value) => Some(value)
      }
      match {
        case some @ Some(_) => some
        case None => globalParam
      }
    }
    catch {
      case e: Exception =>
        globalParam match {
          case Some(v) => Some(v)
          case None => throw e
        }
    }
  }


  protected def getGlobalServiceParamValue[T](row: Row, p: ServiceParam[T]): T =
    getGlobalServiceParamValueOpt(row, p).get
}