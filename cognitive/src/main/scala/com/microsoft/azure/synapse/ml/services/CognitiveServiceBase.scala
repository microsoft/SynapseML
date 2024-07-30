// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.fabric.{FabricClient, TokenLibrary}
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails
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

  def setDefaultAADToken(v: String): this.type = {
    setDefault(AADToken -> Left(v))
  }
}

trait HasCustomAuthHeader extends HasServiceParams {
  // scalastyle:off field.name
  val CustomAuthHeader = new ServiceParam[String](
    this, "CustomAuthHeader", "A Custom Value for Authorization Header"
  )
  // scalastyle:on field.name

  def setCustomAuthHeader(v: String): this.type = {
    setScalarParam(CustomAuthHeader, v)
  }

  def getCustomAuthHeader: String = getScalarParam(CustomAuthHeader)

  def setCustomAuthHeaderCol(v: String): this.type = setVectorParam(CustomAuthHeader, v)

  def getCustomAuthHeaderCol: String = getVectorParam(CustomAuthHeader)

  def setDefaultCustomAuthHeader(v: String): this.type = {
    setDefault(CustomAuthHeader -> Left(v))
  }
}

trait HasCustomHeaders extends HasServiceParams {

  val customHeaders = new ServiceParam[Map[String, String]](
    this, "customHeaders", "Map of Custom Header Key-Value Tuples."
  )

  def setCustomHeaders(v: Map[String, String]): this.type = {
    setScalarParam(customHeaders, v)
  }

  // For Pyspark compatability accept Java HashMap as input to parameter
  // py4J only natively supports conversions from Python Dict to Java HashMap
  def setCustomHeaders(v: java.util.HashMap[String,String]): this.type = {
    setCustomHeaders(v.asScala.toMap)
  }

  def getCustomHeaders: Map[String, String] = getScalarParam(customHeaders)
}

trait HasCustomCogServiceDomain extends Wrappable with HasURL with HasUrlPath {
  def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.cognitiveservices.azure.com/" + urlPath.stripPrefix("/"))
  }

  def setEndpoint(v: String): this.type = {
    setUrl(v + urlPath.stripPrefix("/"))
  }

  override def getUrl: String = "https://synapseml-openai-2.openai.azure.com/openai/deployments/gpt-4/chat/completions"

  def setDefaultInternalEndpoint(v: String): this.type = setDefault(
    url, v + s"/cognitive/${this.internalServiceType}/" + urlPath.stripPrefix("/"))

  private[ml] def internalServiceType: String = ""

  def getInternalServiceType: String = internalServiceType

  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """def setCustomServiceName(self, value):
      |    self._java_obj = self._java_obj.setCustomServiceName(value)
      |    return self
      |
      |def setEndpoint(self, value):
      |    self._java_obj = self._java_obj.setEndpoint(value)
      |    return self
      |
      |def setDefaultInternalEndpoint(self, value):
      |    self._java_obj = self._java_obj.setDefaultInternalEndpoint(value)
      |    return self
      |
      |def _transform(self, dataset: DataFrame) -> DataFrame:
      |    return super()._transform(dataset)
      |""".stripMargin
  }

}

trait HasAPIVersion extends HasServiceParams {
  val apiVersion: ServiceParam[String] = new ServiceParam[String](
    this, "apiVersion", "version of the api", isRequired = true, isURLParam = true) {
    override val payloadName: String = "api-version"
  }

  def getApiVersion: String = getScalarParam(apiVersion)

  def setApiVersion(v: String): this.type = setScalarParam(apiVersion, v)

  def getApiVersionCol: String = getVectorParam(apiVersion)

  def setApiVersionCol(v: String): this.type = setVectorParam(apiVersion, v)

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

trait HasCognitiveServiceInput extends HasURL with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
  with HasCustomHeaders with SynapseMLLogging {

  val customUrlRoot: Param[String] = new Param[String](
    this, "customUrlRoot", "The custom URL root for the service. " +
      "This will not append OpenAI specific model path completions (i.e. /chat/completions) to the URL.")

  def getCustomUrlRoot: String = $(customUrlRoot)

  def setCustomUrlRoot(v: String): this.type = set(customUrlRoot, v)

  protected def paramNameToPayloadName(p: Param[_]): String = p match {
    case p: ServiceParam[_] => p.payloadName
    case _ => p.name
  }

  override def getUrl: String = "https://synapseml-openai-2.openai.azure.com/openai/deployments/gpt-4/chat/completions"

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
      if (get(customUrlRoot).nonEmpty) {
        $(customUrlRoot)
      } else {
        prepareUrlRoot(row) + appended
      }
    }
  }

  protected def prepareEntity: Row => Option[AbstractHttpEntity]

  protected def prepareMethod(): HttpRequestBase = new HttpPost()

  protected val subscriptionKeyHeaderName = "Ocp-Apim-Subscription-Key"

  protected val aadHeaderName = "Authorization"

  protected def contentType: Row => String = { _ => "application/json" }

  protected def getCustomAuthHeader(row: Row): Option[String] = {
    val providedCustomAuthHeader = getValueOpt(row, CustomAuthHeader)
    if (providedCustomAuthHeader .isEmpty && PlatformDetails.runningOnFabric()) {
      logInfo("Using Default AAD Token On Fabric")
      Option(TokenLibrary.getAuthHeader)
    } else {
      providedCustomAuthHeader
    }
  }

  protected def getCustomHeaders(row: Row): Option[Map[String, String]] = {
    getValueOpt(row, customHeaders)
  }

  protected def addHeaders(req: HttpRequestBase,
                           subscriptionKey: Option[String],
                           aadToken: Option[String],
                           contentType: String = "",
                           customAuthHeader: Option[String] = None,
                           customHeaders: Option[Map[String, String]] = None): Unit = {

    if (subscriptionKey.nonEmpty) {
      req.setHeader(subscriptionKeyHeaderName, subscriptionKey.get)
    } else if (aadToken.nonEmpty) {
      aadToken.foreach(s => {
        req.setHeader(aadHeaderName, "Bearer " + s)
        // this is required for internal workload
        req.setHeader("x-ms-workload-resource-moniker", UUID.randomUUID().toString)
      })
    } else if (customAuthHeader.nonEmpty) {
      customAuthHeader.foreach(s => {
        req.setHeader(aadHeaderName, s)
        // this is required for internal workload
        req.setHeader("x-ms-workload-resource-moniker", UUID.randomUUID().toString)
      })
    }
    if (customHeaders.nonEmpty) {
      customHeaders.foreach(m => {
        m.foreach {
          case (headerName, headerValue) => req.setHeader(headerName, headerValue)
        }
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
        addHeaders(req,
          getValueOpt(row, subscriptionKey),
          getValueOpt(row, AADToken),
          contentType(row),
          getCustomAuthHeader(row),
          getCustomHeaders(row))

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


  def setLinkedService(v: String): this.type = {
    val classPath = "mssparkutils.cognitiveService"
    val cognitiveServiceClassLoader = new java.net.URLClassLoader(
      Array(new java.io.File(classPath).toURI.toURL),
      getClass.getClassLoader
    )
    val linkedServiceClass = cognitiveServiceClassLoader.loadClass("mssparkutils.cognitiveService")
    val endpointMethod = linkedServiceClass.getMethod("getEndpoint", v.getClass)
    val keyMethod = linkedServiceClass.getMethod("getKey", v.getClass)
    val endpoint = endpointMethod.invoke(linkedServiceClass, v).toString
    val key = keyMethod.invoke(linkedServiceClass, v).toString
    setUrl(endpoint + urlPath)
    setSubscriptionKey(key)
  }
}

trait HasSetLinkedServiceUsingLocation extends HasSetLinkedService with HasSetLocation {
  override def setLinkedService(v: String): this.type = {
    val classPath = "mssparkutils.cognitiveService"
    val cognitiveServiceClassLoader = new java.net.URLClassLoader(
      Array(new java.io.File(classPath).toURI.toURL),
      getClass.getClassLoader
    )
    val linkedServiceClass = cognitiveServiceClassLoader.loadClass("mssparkutils.cognitiveService")
    val locationMethod = linkedServiceClass.getMethod("getLocation", v.getClass)
    val keyMethod = linkedServiceClass.getMethod("getKey", v.getClass)
    val location = locationMethod.invoke(linkedServiceClass, v).toString
    val key = keyMethod.invoke(linkedServiceClass, v).toString
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
    errorCol -> (this.uid + "_error")
  )

  if(PlatformDetails.runningOnFabric()) {
    setDefaultInternalEndpoint(FabricClient.MLWorkloadEndpointML)
  }

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
        .setTimeout(getTimeout)
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamColName)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](getInternalTransformer(dataset.schema).transform(dataset), dataset.columns.length
    )
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }
}

abstract class CognitiveServicesBase(uid: String) extends
  CognitiveServicesBaseNoHandler(uid) with HasHandler
