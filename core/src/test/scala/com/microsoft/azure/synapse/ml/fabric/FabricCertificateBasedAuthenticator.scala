package com.microsoft.azure.synapse.ml.fabric

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.{KeyStore, MessageDigest, SecureRandom}
import java.util.Base64
import java.util.UUID
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import javax.xml.parsers.DocumentBuilderFactory
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

case class CertificateAuthConfig(
  authority: String,
  clientId: String,
  scope: String,
  redirectUri: String,
  username: String
)

class FabricCertificateBasedAuthenticator extends AutoCloseable {

  private lazy val httpClientWithRedirect: CloseableHttpClient = createHttpClient(withRedirect = true)
  private lazy val httpClientWithoutRedirect: CloseableHttpClient = createHttpClient(withRedirect = false)

  override def close(): Unit = {
    try {
      if (httpClientWithRedirect != null) {
        httpClientWithRedirect.close()
      }
    } catch {
      case _: Exception =>
    }
    try {
      if (httpClientWithoutRedirect != null) {
        httpClientWithoutRedirect.close()
      }
    } catch {
      case _: Exception =>
    }
  }

  private val timeoutInMillis = 60000

  def getTokenWithCertificate(
    clientId: String,
    username: String,
    certificate: String,
    password: Option[String] = None,
    authority: String = "https://login.microsoftonline.com",
    scope: String = "https://analysis.windows.net/powerbi/api/.default",
    redirectUri: String = "https://login.microsoftonline.com/common/oauth2/nativeclient"
  ): String = {

    val config = CertificateAuthConfig(
      authority = authority,
      clientId = clientId,
      scope = scope,
      redirectUri = redirectUri,
      username = username
    )

    val (pfxFilePath, pfxBase64Content) = if (Files.exists(Paths.get(certificate))) {
      (Some(certificate), None)
    } else {
      (None, Some(certificate))
    }

    getTokenWithCertificateSilently(config, pfxFilePath, pfxBase64Content, password)
  }

  def getTokenWithCertificateSilently(
    config: CertificateAuthConfig,
    pfxFilePath: Option[String] = None,
    pfxBase64Content: Option[String] = None,
    password: Option[String] = None
  ): String = {

    require(pfxFilePath.isDefined || pfxBase64Content.isDefined, "Either pfxFilePath or pfxBase64Content must be provided")
    require(config.clientId.nonEmpty, "ClientId cannot be empty")
    require(config.username.nonEmpty, "Username cannot be empty")

    try {
      val (keyStore, alias) = loadCertificate(pfxFilePath, pfxBase64Content, password)

      val codeVerifier = generateCodeVerifier()
      val codeChallenge = generateCodeChallengeFromCodeVerifier(codeVerifier)
      val nonce = UUID.randomUUID().toString

      val authUrl = s"${config.authority}/common/oauth2/v2.0/authorize" +
        s"?client_id=${config.clientId}" +
        s"&scope=${java.net.URLEncoder.encode(config.scope, "UTF-8")}" +
        s"&redirect_uri=${java.net.URLEncoder.encode(config.redirectUri, "UTF-8")}" +
        s"&response_mode=fragment" +
        s"&response_type=code" +
        s"&code_challenge=${java.net.URLEncoder.encode(codeChallenge, "UTF-8")}" +
        s"&code_challenge_method=S256" +
        s"&nonce=$nonce"

      val authConfigMap = getLoginContext(authUrl)
      val certAuthUrl = getCertAuthUrl(config.authority, config.username, authConfigMap)
      val certAuthParameters = getCertAuthParameters(config.authority, keyStore, alias, password, certAuthUrl, authConfigMap)
      val loginResult = login(config.authority, certAuthParameters)

      val tokenResponse = postMSOAuth2Token(
        config.authority,
        config.clientId,
        config.scope,
        config.redirectUri,
        codeVerifier,
        loginResult("code")
      )

      tokenResponse.asJsObject.fields("access_token").convertTo[String]
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to obtain access token using certificate authentication: ${e.getMessage}", e)
    }
  }

  def refreshTokenPair(
    config: CertificateAuthConfig,
    refreshToken: String
  ): (String, String) = {
    val tokenResponse = postMSOAuth2TokenWithRefresh(
      config.authority,
      config.clientId,
      config.scope,
      config.redirectUri,
      refreshToken
    )

    val accessToken = tokenResponse.asJsObject.fields("access_token").convertTo[String]
    val newRefreshToken = tokenResponse.asJsObject.fields("refresh_token").convertTo[String]
    (accessToken, newRefreshToken)
  }

  private def loadCertificate(
    pfxFilePath: Option[String],
    pfxBase64Content: Option[String],
    password: Option[String]
  ): (KeyStore, String) = {
    val pfxBytes = (pfxFilePath, pfxBase64Content) match {
      case (Some(filePath), _) =>
        if (!Files.exists(Paths.get(filePath))) {
          throw new IllegalArgumentException(s"Certificate file not found: $filePath")
        }
        Files.readAllBytes(Paths.get(filePath))
      case (_, Some(base64Content)) =>
        val cleanedBase64 = base64Content.replaceAll("\\s", "")
        Try(Base64.getDecoder.decode(cleanedBase64)) match {
          case Success(bytes) => bytes
          case Failure(ex) =>
            throw new IllegalArgumentException(s"Invalid base64 content for certificate: ${ex.getMessage}")
        }
      case _ =>
        throw new IllegalArgumentException("Either pfxFilePath or pfxBase64Content must be provided")
    }

    val keyStore = KeyStore.getInstance("PKCS12")
    val passwordChars = password.map(_.toCharArray).getOrElse(Array.empty[Char])

    Try {
      keyStore.load(new ByteArrayInputStream(pfxBytes), passwordChars)
    } match {
      case Success(_) =>
      case Failure(_) =>
        if (passwordChars.nonEmpty) {
          Try {
            keyStore.load(new ByteArrayInputStream(pfxBytes), null)
          } match {
            case Success(_) =>
            case Failure(e) => throw new IllegalArgumentException(s"Failed to load certificate: ${e.getMessage}", e)
          }
        } else {
          throw new IllegalArgumentException("Failed to load certificate with provided password")
        }
    }

    val aliases = keyStore.aliases().asScala.toSeq
    if (aliases.isEmpty) {
      throw new IllegalStateException("No certificates found in the keystore")
    }

    val validAlias = aliases.find { alias =>
      keyStore.getCertificate(alias) != null && keyStore.getKey(alias, passwordChars) != null
    }.getOrElse {
      aliases.find { alias =>
        keyStore.getCertificate(alias) != null && keyStore.getKey(alias, null) != null
      }.getOrElse(aliases.head)
    }

    (keyStore, validAlias)
  }

  private def createSSLContext(keyStore: KeyStore, alias: String, password: Option[String]): SSLContext = {
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    val passwordChars = password.map(_.toCharArray).getOrElse(Array.empty[Char])

    Try {
      keyManagerFactory.init(keyStore, passwordChars)
    } match {
      case Success(_) =>
      case Failure(_) =>
        if (passwordChars.nonEmpty) {
          Try {
            keyManagerFactory.init(keyStore, null)
          } match {
            case Success(_) =>
            case Failure(e) => throw new IllegalStateException(s"Failed to initialize key manager: ${e.getMessage}", e)
          }
        } else {
          throw new IllegalStateException("Failed to initialize key manager with provided password")
        }
    }

    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(null.asInstanceOf[KeyStore])

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom())
    sslContext
  }

  private def createHttpClient(
    withRedirect: Boolean = true,
    keyStore: Option[KeyStore] = None,
    alias: Option[String] = None,
    password: Option[String] = None
  ): CloseableHttpClient = {
    val builder = HttpClientBuilder.create()
      .setDefaultRequestConfig(
        RequestConfig.custom()
          .setSocketTimeout(timeoutInMillis)
          .setConnectTimeout(timeoutInMillis)
          .setConnectionRequestTimeout(timeoutInMillis)
          .build()
      )

    if (!withRedirect) {
      builder.disableRedirectHandling()
    }

    (keyStore, alias) match {
      case (Some(ks), Some(a)) =>
        val sslContext = createSSLContext(ks, a, password)
        val sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)
        builder.setSSLSocketFactory(sslSocketFactory)
      case _ =>
    }

    builder.build()
  }

  private def generateCodeVerifier(): String = {
    val random = new SecureRandom()
    val bytes = new Array[Byte](32)
    random.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)
  }

  private def generateCodeChallengeFromCodeVerifier(codeVerifier: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(codeVerifier.getBytes(StandardCharsets.US_ASCII))
    Base64.getUrlEncoder.withoutPadding().encodeToString(hash)
  }

  private def getLoginContext(loginUrl: String): mutable.Map[String, String] = {
    val request = new HttpGet(loginUrl)
    val response = httpClientWithRedirect.execute(request)
    try {
      val responseContent = EntityUtils.toString(response.getEntity)
      val authConfig = parseAuthResponse(responseContent)
      mutable.Map(
        "ctx" -> authConfig.getOrElse("sCtx", ""),
        "flowToken" -> authConfig.getOrElse("sFT", ""),
        "canary" -> authConfig.getOrElse("canary", "")
      )
    } finally {
      response.close()
    }
  }

  private def getCertAuthUrl(
    authority: String,
    username: String,
    authConfig: mutable.Map[String, String]
  ): URI = {
    val request = new HttpPost(s"$authority/common/GetCredentialType?mkt=en-US")

    val requestBody = Map(
      "username" -> username,
      "flowToken" -> authConfig("flowToken")
    ).toJson.compactPrint

    request.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON))

    val response = httpClientWithoutRedirect.execute(request)
    try {
      val responseContent = EntityUtils.toString(response.getEntity)
      val jobject = responseContent.parseJson.asJsObject

      authConfig("flowToken") = jobject.fields.get("FlowToken").map(_.convertTo[String]).getOrElse("")

      val certAuthUrl = jobject.fields("Credentials").asJsObject
        .fields("CertAuthParams").asJsObject
        .fields("CertAuthUrl").convertTo[String]

      new URI(certAuthUrl)
    } finally {
      response.close()
    }
  }

  private def createCertificateHttpClient(
    keyStore: KeyStore,
    alias: String,
    password: Option[String]
  ): CloseableHttpClient = {
    createHttpClient(withRedirect = false, Some(keyStore), Some(alias), password)
  }

  private def getCertAuthParameters(
    authority: String,
    keyStore: KeyStore,
    alias: String,
    password: Option[String],
    certAuthUrl: URI,
    authConfig: mutable.Map[String, String]
  ): Map[String, String] = {
    val client = createCertificateHttpClient(keyStore, alias, password)
    try {
      val request = new HttpPost(certAuthUrl)

      val params = List(
        new BasicNameValuePair("ctx", authConfig("ctx")),
        new BasicNameValuePair("flowToken", authConfig("flowToken"))
      )
      request.setEntity(new UrlEncodedFormEntity(params.asJava))

      request.setHeader("Connection", "keep-alive")
      request.setHeader("Referer", s"$authority/common")
      request.setHeader("Cache-Control", "no-cache, no-store")

      val response = client.execute(request)
      try {
        val htmlContent = EntityUtils.toString(response.getEntity)
        parseHiddenInputs(htmlContent)
      } finally {
        response.close()
      }
    } finally {
      client.close()
    }
  }

  private def login(
    authority: String,
    certTokenParams: Map[String, String]
  ): Map[String, String] = {
    val loginRequest = new HttpPost(s"$authority/common/login")
    val params = List(
      new BasicNameValuePair("ctx", certTokenParams("ctx")),
      new BasicNameValuePair("flowtoken", certTokenParams("flowtoken")),
      new BasicNameValuePair("certificatetoken", certTokenParams("certificatetoken"))
    )
    loginRequest.setEntity(new UrlEncodedFormEntity(params.asJava))

    val response = httpClientWithoutRedirect.execute(loginRequest)
    try {
      val responseContent = EntityUtils.toString(response.getEntity)

      if (response.getStatusLine.getStatusCode == 302) {
        val location = response.getFirstHeader("Location").getValue
        val uri = new URI(location)
        val fragment = uri.getFragment
        if (fragment != null) {
          val code = fragment.split("&")(0).split("=")(1)
          return Map("code" -> code)
        }
      }

      val loginResponse = parseAuthResponse(responseContent)
      if (loginResponse.contains("strMainMessage")) {
        throw new IllegalArgumentException(loginResponse.getOrElse("strServiceExceptionMessage", "Authentication failed"))
      }

      val loginResult = mutable.Map[String, String]()

      val setCookieHeaders = response.getHeaders("Set-Cookie")
      for (header <- setCookieHeaders) {
        val cookieValue = header.getValue
        val cookieParts = cookieValue.split(";", 2)(0).split("=", 2)
        if (cookieParts.length == 2) {
          loginResult(cookieParts(0)) = cookieParts(1)
        }
      }

      val kmsiRequest = new HttpPost(s"$authority/kmsi")
      val kmsiParams = List(
        new BasicNameValuePair("ctx", loginResponse.getOrElse("sCtx", "")),
        new BasicNameValuePair("flowToken", loginResponse.getOrElse("sFT", "")),
        new BasicNameValuePair("login", "true"),
        new BasicNameValuePair("type", "28")
      )
      kmsiRequest.setEntity(new UrlEncodedFormEntity(kmsiParams.asJava))

      val kmsiResponse = httpClientWithoutRedirect.execute(kmsiRequest)
      try {
        EntityUtils.consumeQuietly(kmsiResponse.getEntity)
      } finally {
        kmsiResponse.close()
      }

      loginResult.toMap + ("code" -> loginResponse.getOrElse("code", ""))
    } finally {
      response.close()
    }
  }

  private def postMSOAuth2Token(
    authority: String,
    clientId: String,
    scope: String,
    redirectUri: String,
    codeVerifier: String,
    code: String
  ): JsValue = {
    val tokenRequest = new HttpPost(s"$authority/common/oauth2/v2.0/token")
    val params = List(
      new BasicNameValuePair("client_id", clientId),
      new BasicNameValuePair("scope", scope),
      new BasicNameValuePair("redirect_uri", redirectUri),
      new BasicNameValuePair("grant_type", "authorization_code"),
      new BasicNameValuePair("code_verifier", codeVerifier),
      new BasicNameValuePair("code", code)
    )
    tokenRequest.setEntity(new UrlEncodedFormEntity(params.asJava))

    val response = httpClientWithoutRedirect.execute(tokenRequest)
    try {
      val responseContent = EntityUtils.toString(response.getEntity)
      val tokenResponseJson = responseContent.parseJson.asJsObject

      if (!response.getStatusLine.toString.startsWith("HTTP/1.1 2")) {
        val errorDesc = tokenResponseJson.fields.get("error_description")
          .map(_.convertTo[String]).getOrElse("Unknown error")
        throw new IllegalArgumentException(errorDesc)
      }

      tokenResponseJson
    } finally {
      response.close()
    }
  }

  private def postMSOAuth2TokenWithRefresh(
    authority: String,
    clientId: String,
    scope: String,
    redirectUri: String,
    refreshToken: String
  ): JsValue = {
    val tokenRequest = new HttpPost(s"$authority/common/oauth2/v2.0/token")

    val params = List(
      new BasicNameValuePair("client_id", clientId),
      new BasicNameValuePair("scope", scope),
      new BasicNameValuePair("redirect_uri", redirectUri),
      new BasicNameValuePair("grant_type", "refresh_token"),
      new BasicNameValuePair("refresh_token", refreshToken)
    )
    tokenRequest.setEntity(new UrlEncodedFormEntity(params.asJava))

    val response = httpClientWithoutRedirect.execute(tokenRequest)
    try {
      val responseContent = EntityUtils.toString(response.getEntity)
      val tokenResponseJson = responseContent.parseJson.asJsObject

      if (!response.getStatusLine.toString.startsWith("HTTP/1.1 2")) {
        val errorDesc = tokenResponseJson.fields.get("error_description")
          .map(_.convertTo[String]).getOrElse("Unknown error")
        throw new IllegalArgumentException(errorDesc)
      }

      tokenResponseJson
    } finally {
      response.close()
    }
  }

  private def parseAuthResponse(authResponse: String): Map[String, String] = {
    val authResponseMap = mutable.Map[String, String]()
    val startDelimiter = "$Config="
    val endDelimiter = ";\n"

    val startIndex = authResponse.indexOf(startDelimiter) + startDelimiter.length
    if (startIndex < startDelimiter.length) {
      return authResponseMap.toMap
    }

    val endIndex = authResponse.indexOf(endDelimiter, startIndex)
    if (endIndex == -1) {
      return authResponseMap.toMap
    }

    val jsonContent = authResponse.substring(startIndex, endIndex)

    Try {
      val json = jsonContent.parseJson.asJsObject
      json.fields.foreach {
        case (key, value: JsString) => authResponseMap(key) = value.value
        case (key, JsNumber(num))   => authResponseMap(key) = num.toString
        case (key, JsBoolean(bool)) => authResponseMap(key) = bool.toString
        case (key, JsNull)          => authResponseMap(key) = ""
        case _ =>
      }
    } match {
      case Success(_) =>
      case Failure(_) =>
    }

    authResponseMap.toMap
  }

  private def parseHiddenInputs(htmlContent: String): Map[String, String] = {
    val result = mutable.Map[String, String]()

    Try {
      val factory = DocumentBuilderFactory.newInstance()

      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
      factory.setFeature("http://xml.org/sax/features/external-general-entities", false)
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false)

      val builder = factory.newDocumentBuilder()
      val doc = builder.parse(new ByteArrayInputStream(htmlContent.getBytes))

      val inputs = doc.getElementsByTagName("input")
      var i = 0
      while (i < inputs.getLength) {
        val input = inputs.item(i)
        val attributes = input.getAttributes

        val inputType = Option(attributes.getNamedItem("type")).map(_.getNodeValue)
        val nameAttr = Option(attributes.getNamedItem("name")).map(_.getNodeValue)
        val valueAttr = Option(attributes.getNamedItem("value")).map(_.getNodeValue)

        (inputType, nameAttr, valueAttr) match {
          case (Some("hidden"), Some(name), Some(value)) =>
            result(name) = value
          case _ =>
        }
        i += 1
      }
    } match {
      case Success(_) =>
      case Failure(_) =>
    }

    result.toMap
  }
}

object FabricCertificateBasedAuthenticator {

  def getTokenWithCertificate(
    clientId: String,
    username: String,
    certificate: String,
    password: Option[String] = None,
    authority: String = "https://login.microsoftonline.com",
    scope: String = "https://analysis.windows.net/powerbi/api/.default",
    redirectUri: String = "https://login.microsoftonline.com/common/oauth2/nativeclient"
  ): String = {
    Using(new FabricCertificateBasedAuthenticator()) { authenticator =>
      authenticator.getTokenWithCertificate(clientId, username, certificate, password, authority, scope, redirectUri)
    }.get
  }

  def refreshTokenPair(
    config: CertificateAuthConfig,
    refreshToken: String
  ): (String, String) = {
    Using(new FabricCertificateBasedAuthenticator()) { authenticator =>
      authenticator.refreshTokenPair(config, refreshToken)
    }.get
  }
}
