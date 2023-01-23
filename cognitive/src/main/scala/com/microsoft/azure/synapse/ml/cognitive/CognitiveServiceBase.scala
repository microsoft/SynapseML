// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda}
import org.apache.http.NameValuePair
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import org.apache.spark.ml.{ComplexParamsWritable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json.DefaultJsonProtocol._

import java.net.URI
import java.util.UUID
import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.internal.util.ScalaClassLoader

trait HasServiceParams extends Params {
  def getVectorParam(p: ServiceParam[_]): String = {
    this.getOrDefault(p).right.get
  }

  def getScalarParam[T](p: ServiceParam[T]): T = {
    this.getOrDefault(p).left.get
  }

  def setVectorParam[T](p: ServiceParam[T], value: String): this.type = {
    set(p, Right(value))
  }

  def setScalarParam[T](p: ServiceParam[T], value: T): this.type = {
    set(p, Left(value))
  }

  def getVectorParam(name: String): String = {
    getVectorParam(this.getParam(name).asInstanceOf[ServiceParam[_]])
  }

  def getScalarParam[T](name: String): T = {
    getScalarParam(this.getParam(name).asInstanceOf[ServiceParam[T]])
  }

  def setVectorParam(name: String, value: String): this.type = {
    setVectorParam(this.getParam(name).asInstanceOf[ServiceParam[_]], value)
  }

  def setScalarParam[T](name: String, value: T): this.type = {
    setScalarParam(this.getParam(name).asInstanceOf[ServiceParam[T]], value)
  }

  protected def getVectorParamMap: Map[String, String] = this.params.flatMap {
    case p: ServiceParam[_] =>
      get(p).orElse(getDefault(p)).flatMap(v =>
        v.right.toOption.map(colName => (p.name, colName)))
    case _ => None
  }.toMap

  protected def getRequiredParams: Array[ServiceParam[_]] = this.params.filter {
    case p: ServiceParam[_] if p.isRequired => true
    case _ => false
  }.map(_.asInstanceOf[ServiceParam[_]])

  protected def getUrlParams: Array[ServiceParam[_]] = this.params.filter {
    case p: ServiceParam[_] if p.isURLParam => true
    case _ => false
  }.map(_.asInstanceOf[ServiceParam[_]])

  protected def emptyParamData[T](row: Row, p: ServiceParam[T]): Boolean = {
    if (get(p).isEmpty && getDefault(p).isEmpty) {
      true
    } else {
      val value = getOrDefault(p)
      value match {
        case Left(_) => false
        case Right(colName) =>
          Option(row.get(row.fieldIndex(colName))).isEmpty
        case _ => true
      }
    }
  }

  protected def shouldSkip(row: Row): Boolean = getRequiredParams.exists { p =>
    emptyParamData(row, p)
  }

  protected def getValueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = {
    get(p).orElse(getDefault(p)).flatMap {
      case Right(colName) => Option(row.getAs[T](colName))
      case Left(value) => Some(value)
    }
  }


  protected def getValue[T](row: Row, p: ServiceParam[T]): T =
    getValueOpt(row, p).get

  protected def getValueAnyOpt(row: Row, p: ServiceParam[_]): Option[Any] = {
    get(p).orElse(getDefault(p)).flatMap {
      case Right(colName) => Option(row.get(row.fieldIndex(colName)))
      case Left(value) => Some(value)
    }
  }


  protected def getValueAny(row: Row, p: ServiceParam[_]): Any =
    getValueAnyOpt(row, p).get

  protected def getValueMap(row: Row, excludes: Set[ServiceParam[_]] = Set()): Map[String, Any] = {
    this.params.flatMap {
      case p: ServiceParam[_] if !excludes(p) =>
        getValueOpt(row, p).map(v => (p.name, v))
      case _ => None
    }.toMap
  }
}

trait HasSubscriptionKey extends HasServiceParams {
  val subscriptionKey = new ServiceParam[String](
    this, "subscriptionKey", "the API key to use")

  def getSubscriptionKey: String = getScalarParam(subscriptionKey)

  def setSubscriptionKey(v: String): this.type = {
    setScalarParam(subscriptionKey, v)
  }

  def getSubscriptionKeyCol: String = getVectorParam(subscriptionKey)

  def setSubscriptionKeyCol(v: String): this.type = setVectorParam(subscriptionKey, v)

}

trait HasAADToken extends HasServiceParams {
  // scalastyle:off field.name
  val AADToken = new ServiceParam[String](
    this, "AADToken", "AAD Token used for authentication"
  )
  // scalastyle:on field.name

  def setAADToken(v: String): this.type = {
    setScalarParam(AADToken, v)
  }

  def getAADToken: String = getScalarParam(AADToken)

  def setAADTokenCol(v: String): this.type = setVectorParam(AADToken, v)

  def getAADTokenCol: String = getVectorParam(AADToken)
}

trait HasCustomCogServiceDomain extends Wrappable with HasURL with HasUrlPath {
  def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.cognitiveservices.azure.com/" + urlPath.stripPrefix("/"))
  }

  def setEndpoint(v: String): this.type = {
    setUrl(v + urlPath.stripPrefix("/"))
  }

  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """def setCustomServiceName(self, value):
      |    self._java_obj = self._java_obj.setCustomServiceName(value)
      |    return self
      |
      |def setEndpoint(self, value):
      |    self._java_obj = self._java_obj.setEndpoint(value)
      |    return self
      |
      |def _transform(self, dataset: DataFrame) -> DataFrame:
      |    if running_on_synapse_internal():
      |        from synapse.ml.mlflow import get_mlflow_env_config
      |        mlflow_env_configs = get_mlflow_env_config()
      |        self.setAADToken(mlflow_env_configs.driver_aad_token)
      |        self.setEndpoint(mlflow_env_configs.workload_endpoint + "/cognitive/api/")
      |    return super()._transform(dataset)
      |""".stripMargin
  }

  override def dotnetAdditionalMethods: String = super.dotnetAdditionalMethods + {
    s"""/// <summary>
       |/// Sets value for service name
       |/// </summary>
       |/// <param name=\"value\">
       |/// Service name of the cognitive service if it's custom domain
       |/// </param>
       |/// <returns> New $dotnetClassName object </returns>
       |public $dotnetClassName SetCustomServiceName(string value) =>
       |    $dotnetClassWrapperName(Reference.Invoke(\"setCustomServiceName\", value));
       |
       |/// <summary>
       |/// Sets value for endpoint
       |/// </summary>
       |/// <param name=\"value\">
       |/// Endpoint of the cognitive service
       |/// </param>
       |/// <returns> New $dotnetClassName object </returns>
       |public $dotnetClassName SetEndpoint(string value) =>
       |    $dotnetClassWrapperName(Reference.Invoke(\"setEndpoint\", value));
       |""".stripMargin
  }
}

object URLEncodingUtils {

  private case class NameValuePairInternal(t: (String, String)) extends NameValuePair {
    override def getName: String = t._1

    override def getValue: String = t._2
  }

  def format(m: Map[String, String]): String = {
    URLEncodedUtils.format(m.toList.map(NameValuePairInternal).asJava, "UTF-8")
  }
}

trait HasCognitiveServiceInput extends HasURL with HasSubscriptionKey with HasAADToken {

  protected def paramNameToPayloadName(p: Param[_]): String = p match {
    case p: ServiceParam[_] => p.payloadName
    case _ => p.name
  }

  protected def prepareUrlRoot: Row => String = {
    _ => getUrl
  }

  protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val appended = if (!urlParams.isEmpty) {
        "?" + URLEncodingUtils.format(urlParams.flatMap(p =>
          getValueOpt(row, p).map(v => paramNameToPayloadName(p) -> p.toValueString(v))
        ).toMap)
      } else {
        ""
      }
      prepareUrlRoot(row) + appended
    }
  }

  protected def prepareEntity: Row => Option[AbstractHttpEntity]

  protected def prepareMethod(): HttpRequestBase = new HttpPost()

  protected val subscriptionKeyHeaderName = "Ocp-Apim-Subscription-Key"

  protected val aadHeaderName = "Authorization"

  protected def contentType: Row => String = { _ => "application/json" }

  protected def addHeaders(req: HttpRequestBase,
                           subscriptionKey: Option[String],
                           aadToken: Option[String],
                           contentType: String = ""): Unit = {
    if (subscriptionKey.nonEmpty) {
      req.setHeader(subscriptionKeyHeaderName, subscriptionKey.get)
    } else {
      aadToken.foreach(s => {
        req.setHeader(aadHeaderName, "Bearer " + s)
        // this is required for internal workload
        req.setHeader("x-ms-workload-resource-moniker", UUID.randomUUID().toString)
      })
    }
    if (contentType != "") req.setHeader("Content-Type", contentType)
  }

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    val rowToUrl = prepareUrl
    val rowToEntity = prepareEntity;
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val req = prepareMethod()
        req.setURI(new URI(rowToUrl(row)))
        addHeaders(req, getValueOpt(row, subscriptionKey), getValueOpt(row, AADToken), contentType(row))

        req match {
          case er: HttpEntityEnclosingRequestBase =>
            rowToEntity(row).foreach(er.setEntity)
          case _ =>
        }
        Some(req)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc(schema))
  }
}

trait HasInternalJsonOutputParser {

  protected def responseDataType: DataType

  protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new JSONOutputParser().setDataType(responseDataType)
  }
}

trait HasUrlPath {
  def urlPath: String
}

trait HasSetLinkedService extends Wrappable with HasURL with HasSubscriptionKey with HasUrlPath {
  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def setLinkedService(self, value):
      |    self._java_obj = self._java_obj.setLinkedService(value)
      |    return self
      |""".stripMargin
  }

  override def dotnetAdditionalMethods: String = super.dotnetAdditionalMethods + {
    s"""/// <summary>
       |/// Sets value for linkedService
       |/// </summary>
       |/// <param name=\"value\">
       |/// linkedService name
       |/// </param>
       |/// <returns> New $dotnetClassName object </returns>
       |public $dotnetClassName SetLinkedService(string value) =>
       |    $dotnetClassWrapperName(Reference.Invoke(\"setLinkedService\", value));
       |""".stripMargin
  }

  def setLinkedService(v: String): this.type = {
    val classPath = "mssparkutils.cognitiveService"
    val linkedServiceClass = ScalaClassLoader(getClass.getClassLoader).tryToLoadClass(classPath)
    val endpointMethod = linkedServiceClass.get.getMethod("getEndpoint", v.getClass)
    val keyMethod = linkedServiceClass.get.getMethod("getKey", v.getClass)
    val endpoint = endpointMethod.invoke(linkedServiceClass.get, v).toString
    val key = keyMethod.invoke(linkedServiceClass.get, v).toString
    setUrl(endpoint + urlPath)
    setSubscriptionKey(key)
  }
}

trait HasSetLinkedServiceUsingLocation extends HasSetLinkedService with HasSetLocation {
  override def setLinkedService(v: String): this.type = {
    val classPath = "mssparkutils.cognitiveService"
    val linkedServiceClass = ScalaClassLoader(getClass.getClassLoader).tryToLoadClass(classPath)
    val locationMethod = linkedServiceClass.get.getMethod("getLocation", v.getClass)
    val keyMethod = linkedServiceClass.get.getMethod("getKey", v.getClass)
    val location = locationMethod.invoke(linkedServiceClass.get, v).toString
    val key = keyMethod.invoke(linkedServiceClass.get, v).toString
    setLocation(location)
    setSubscriptionKey(key)
  }
}

trait HasSetLocation extends Wrappable with HasURL with HasUrlPath with DomainHelper {
  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def setLocation(self, value):
      |    self._java_obj = self._java_obj.setLocation(value)
      |    return self
      |""".stripMargin
  }

  override def dotnetAdditionalMethods: String = super.dotnetAdditionalMethods + {
    s"""/// <summary>
       |/// Sets value for location
       |/// </summary>
       |/// <param name=\"value\">
       |/// Location of the cognitive service
       |/// </param>
       |/// <returns> New $dotnetClassName object </returns>
       |public $dotnetClassName SetLocation(string value) =>
       |    $dotnetClassWrapperName(Reference.Invoke(\"setLocation\", value));
       |""".stripMargin
  }


  def setLocation(v: String): this.type = {
    val domain = getLocationDomain(v)
    setUrl(s"https://$v.api.cognitive.microsoft.$domain/" + urlPath.stripPrefix("/"))
  }
}

trait DomainHelper {

  protected def getLocationDomain(location: String): String = {
    val usGovRegions = Array("usgovarizona", "usgovvirginia")
    val cnVianet = Array("chinaeast2", "chinanorth")
    val domain = location match {
      case v if usGovRegions.contains(v) => "us"
      case v if cnVianet.contains(v) => "cn"
      case _ => "com"
    }
    domain
  }
}

abstract class CognitiveServicesBaseNoHandler(val uid: String) extends Transformer
  with ConcurrencyParams with HasOutputCol
  with HasURL with ComplexParamsWritable
  with HasSubscriptionKey with HasErrorCol
  with HasAADToken with HasCustomCogServiceDomain
  with SynapseMLLogging {

  setDefault(
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"))

  protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData

  protected def getInternalInputParser(schema: StructType): HTTPInputParser

  protected def getInternalOutputParser(schema: StructType): HTTPOutputParser

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val missingRequiredParams = this.getRequiredParams.filter {
      p => this.get(p).isEmpty && this.getDefault(p).isEmpty
    }
    assert(missingRequiredParams.isEmpty,
      s"Missing required params: ${missingRequiredParams.map(s => s.name).mkString("(", ", ", ")")}")

    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }

    val stages = Array(
      Lambda(_.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc _)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamColName)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getInternalTransformer(dataset.schema).transform(dataset)
    )
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }
}

abstract class CognitiveServicesBase(uid: String) extends
  CognitiveServicesBaseNoHandler(uid) with HasHandler
