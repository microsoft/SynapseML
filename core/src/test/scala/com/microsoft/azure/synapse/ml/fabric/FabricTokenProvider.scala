package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.message.BasicNameValuePair
import spray.json.DefaultJsonProtocol
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json._

import java.util.Base64
import scala.jdk.CollectionConverters._

case class AccessTokenConfiguration(
  ClientId: String,
  RedirectUri: String,
  Resource: String,
  Authority: String,
  Tenant: String,
  Username: String,
  Certificate: Option[String] = None,
  CertificatePassword: Option[String] = None
)

object FabricTokenProvider {

  private var TokenMap: Map[AccessTokenConfiguration, String] = Map.empty

  def getAccessToken(
    clientId: String,
    redirectUri: String,
    resource: String,
    authority: String,
    tenant: String,
    username: String
  ): String = {
    val config = AccessTokenConfiguration(
      ClientId = clientId,
      RedirectUri = redirectUri,
      Resource = resource,
      Authority = authority,
      Tenant = tenant,
      Username = username
    )
    getAccessToken(config)
  }

  def getAccessToken(tokenConfig: AccessTokenConfiguration): String = {
    val resolvedConfig = resolveAccessTokenConfiguration(tokenConfig)
    val token = TokenMap.get(resolvedConfig) match {
      case Some(existing) if !JwtUtils.isTokenExpired(existing) => existing
      case _ =>
        val fresh = fetchTokenByCertificate(resolvedConfig)
        TokenMap += resolvedConfig -> fresh
        fresh
    }
    token
  }

  private def resolveAccessTokenConfiguration(tokenConfig: AccessTokenConfiguration): AccessTokenConfiguration = {
    val resolvedCertificate = tokenConfig.Certificate.orElse(sys.env.get("ut_certificate"))
    val resolvedCertificatePassword = tokenConfig.CertificatePassword.orElse(sys.env.get("ut_certificate_password"))

    val resolvedConfig = tokenConfig.copy(
      Certificate = resolvedCertificate,
      CertificatePassword = resolvedCertificatePassword
    )

    if (resolvedConfig.Certificate.isEmpty) {
      throw new IllegalArgumentException("Certificate is required for certificate-based authentication")
    }

    resolvedConfig
  }

  private def fetchTokenByCertificate(tokenConfig: AccessTokenConfiguration): String = {
    println("Fetching token with configuration (using certificate):")
    println(s"\tTenant: ${tokenConfig.Tenant}")
    println(s"\tResource: ${tokenConfig.Resource}")
    println(s"\tAuthority: ${tokenConfig.Authority}")
    println(s"\tCert Length: ${tokenConfig.Certificate.getOrElse("").length}")
    println(s"\tRedirect: ${tokenConfig.RedirectUri}")
    println(s"\tClient ID: ${tokenConfig.ClientId}")
    println(s"\tUsername: ${tokenConfig.Username}")

    tokenConfig.Certificate match {
      case Some(certificate) =>
        FabricCertificateBasedAuthenticator.getTokenWithCertificate(
          tokenConfig.ClientId,
          s"${tokenConfig.Username}@${tokenConfig.Tenant}",
          certificate,
          password = tokenConfig.CertificatePassword,
          authority = tokenConfig.Authority,
          scope = tokenConfig.Resource,
          redirectUri = tokenConfig.RedirectUri
        )
      case None =>
        throw new IllegalArgumentException("Certificate is required for certificate-based authentication")
    }
  }

  private object JwtUtils {
    private case class Token(exp: Long)
    private object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val TokenFormat: RootJsonFormat[Token] = jsonFormat1(Token)
    }

    private def decodeBase64Url(str: String): String = new String(Base64.getUrlDecoder.decode(str.getBytes))

    def isTokenExpired(token: String): Boolean = {
      import MyJsonProtocol._
      try {
        val parts = token.split("\\.")
        if (parts.length != 3) {
          throw new IllegalArgumentException("Invalid JWT token format")
        }

        val payload = decodeBase64Url(parts(1))
        val jsonAst = payload.parseJson
        val tokenData = jsonAst.convertTo[Token]
        val currentTime = System.currentTimeMillis() / 1000

        tokenData.exp < currentTime
      } catch {
        case e: Exception =>
          println(s"Failed to process token: ${e.getMessage}")
          true
      }
    }
  }
}

