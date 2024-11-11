package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.reflect.ClassTag

trait GlobalKey[T] {
  val name: String
  val isServiceParam: Boolean
}

case object OpenAIDeploymentNameKey extends GlobalKey[String]
  {val name: String = "OpenAIDeploymentName"; val isServiceParam = true}
case object TestParamKey extends GlobalKey[Double] {val name: String = "TestParam"; val isServiceParam = false}
case object TestServiceParamKey extends GlobalKey[Int]
  {val name: String = "TestServiceParam"; val isServiceParam = true}

object GlobalParams {
  private val ParamToKeyMap: mutable.Map[Any, GlobalKey[_]] = mutable.Map.empty
  private val GlobalParams: mutable.Map[GlobalKey[_], Any] = mutable.Map.empty

  private def boxedClass(c: Class[_]): Class[_] = {
    if (!c.isPrimitive) c
    c match {
      case java.lang.Integer.TYPE   => classOf[java.lang.Integer]
      case java.lang.Long.TYPE      => classOf[java.lang.Long]
      case java.lang.Double.TYPE    => classOf[java.lang.Double]
      case java.lang.Float.TYPE     => classOf[java.lang.Float]
      case java.lang.Boolean.TYPE   => classOf[java.lang.Boolean]
      case _                        => c // Fallback for any other primitive types
    }
  }

  private val StringtoKeyMap: Map[String, GlobalKey[_]] = Map(
    "OpenAIDeploymentName" -> OpenAIDeploymentNameKey,
    "TestParam" -> TestParamKey,
    "TestServiceParam" -> TestServiceParamKey,
  )

  private def findGlobalKeyByName(keyName: String): Option[GlobalKey[_]] = {
    StringtoKeyMap.get(keyName)
  }

  def setGlobalParam[T](key: GlobalKey[T], value: T)(implicit ct: ClassTag[T]): Unit = {
    assert(!key.isServiceParam, s"${key.name} is a Service Param. setGlobalServiceParamKey should be used.")
    val expectedClass = boxedClass(ct.runtimeClass)
    val actualClass = value.getClass
    assert(
      expectedClass.isAssignableFrom(actualClass),
      s"Value of type ${actualClass.getName} is not compatible with expected type ${expectedClass.getName}"
    )
    GlobalParams(key) = value
  }

  def setGlobalParam[T](keyName: String, value: T)(implicit ct: ClassTag[T]): Unit = {
    val expectedClass = boxedClass(ct.runtimeClass)
    val actualClass = value.getClass
    assert(
      expectedClass.isAssignableFrom(actualClass),
      s"Value of type ${actualClass.getName} is not compatible with expected type ${expectedClass.getName}"
    )
    val key = findGlobalKeyByName(keyName)
    key match {
      case Some(k) =>
        assert(!k.isServiceParam, s"${k.name} is a Service Param. setGlobalServiceParamKey should be used.")
        GlobalParams(k) = value
      case None => throw new IllegalArgumentException("${keyName} is not a valid key in GlobalParams")
    }
  }

  def setGlobalServiceParam[T](key: GlobalKey[T], value: T)(implicit ct: ClassTag[T]): Unit = {
    assert(key.isServiceParam, s"${key.name} is a Param. setGlobalParamKey should be used.")
    val expectedClass = boxedClass(ct.runtimeClass)
    val actualClass = value.getClass
    assert(
      expectedClass.isAssignableFrom(actualClass),
      s"Value of type ${actualClass.getName} is not compatible with expected type ${expectedClass.getName}"
    )
    GlobalParams(key) = Left(value)
  }

  def setGlobalServiceParam[T](keyName: String, value: T)(implicit ct: ClassTag[T]): Unit = {
    val expectedClass = boxedClass(ct.runtimeClass)
    val actualClass = value.getClass
    assert(
      expectedClass.isAssignableFrom(actualClass),
      s"Value of type ${actualClass.getName} is not compatible with expected type ${expectedClass.getName}"
    )
    val key = findGlobalKeyByName(keyName)
    key match {
      case Some(k) =>
        assert(k.isServiceParam, s"${k.name} is a Param. setGlobalParamKey should be used.")
        GlobalParams(k) = Left(value)
      case None => throw new IllegalArgumentException("${keyName} is not a valid key in GlobalParams")
    }
  }

  def setGlobalServiceParamCol[T](key: GlobalKey[T], value: String): Unit = {
    assert(key.isServiceParam, s"${key.name} is a Param. setGlobalParamKey should be used.")
    GlobalParams(key) = Right(value)
  }

  def setGlobalServiceParamCol[T](keyName: String, value: String): Unit = {
    val key = findGlobalKeyByName(keyName)
    key match {
      case Some(k) =>
        assert(k.isServiceParam, s"${k.name} is a Param. setGlobalParamKey should be used.")
        GlobalParams(k) = Right(value)
      case None => throw new IllegalArgumentException("${keyName} is not a valid key in GlobalParams")
    }
  }

  private def getGlobalParam[T](key: GlobalKey[T]): Option[T] = {
    GlobalParams.get(key.asInstanceOf[GlobalKey[Any]]).map(_.asInstanceOf[T])
  }

  private def getGlobalServiceParam[T](key: GlobalKey[T]): Option[Either[T, String]] = {
    val value = GlobalParams.get(key.asInstanceOf[GlobalKey[Any]]).map(_.asInstanceOf[Either[T, String]])
    value match {
      case some @ Some(v) =>
        assert(v.isInstanceOf[Either[T, String]],
        "getGlobalServiceParam used to fetch a normal Param")
        value
      case None => None
    }
  }

  def getParam[T](p: Param[T]): Option[T] = {
    ParamToKeyMap.get(p).flatMap { key =>
      key match {
        case k: GlobalKey[T] =>
          assert(!k.isServiceParam, s"${k.name} is a Service Param. getServiceParam should be used.")
          getGlobalParam(k)
        case _ => None
      }
    }
  }

  def getServiceParam[T](sp: ServiceParam[T]): Option[Either[T, String]] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalKey[T] =>
          assert(k.isServiceParam, s"${k.name} is a Param. getParam should be used.")
          getGlobalServiceParam(k)
        case _ => None
      }
    }
  }

  def getServiceParamScalar[T](sp: ServiceParam[T]): Option[T] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalKey[T] =>
          getGlobalServiceParam(k) match {
            case Some(Left(value)) =>
              assert(k.isServiceParam, s"${k.name} is a Param. getParam should be used.")
              Some(value)
            case _ => None
          }
        case _ => None
      }
    }
  }

  def getServiceParamVector[T](sp: ServiceParam[T]): Option[String] = {
    ParamToKeyMap.get(sp).flatMap { key =>
      key match {
        case k: GlobalKey[T] =>
          getGlobalServiceParam(k) match {
            case Some(Right(colName)) =>
              assert(k.isServiceParam, s"${k.name} is a Param. getParam should be used.")
              Some(colName)
            case _ => None
          }
        case _ => None
      }
    }
  }

  def registerParam[T](p: Param[T], key: GlobalKey[T]): Unit = {
    assert(!key.isServiceParam, s"${key.name} is a Service Param. registerServiceParam should be used.")
    ParamToKeyMap(p) = key
  }

  def registerServiceParam[T](sp: ServiceParam[T], key: GlobalKey[T]): Unit = {
    assert(key.isServiceParam, s"${key.name} is a Param. registerParam should be used.")
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