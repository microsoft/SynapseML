// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.Secrets.getSynapseExtensionSecret
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.message.BasicNameValuePair
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json._

import java.util.Base64
import scala.jdk.CollectionConverters.seqAsJavaListConverter

case class AccessTokenConfiguration(ClientId: String,
                                    RedirectUri: String,
                                    Resource: String = "https://analysis.windows.net/powerbi/api/.default",
                                    Authority: String = "https://login.windows.net",
                                    Tenant: String = FabricTestConstants.INTEGRATION_TENANT,
                                    Username: String = FabricTestConstants.INTEGRATION_USERNAME,
                                    Password: Option[String] = None,
                                    Certificate: Option[String] = None,
                                    CertificatePassword: Option[String] = None)

object FabricTokenProvider {
  private var TokenMap: Map[AccessTokenConfiguration, String] = Map()

  def getAccessToken(clientId: String, redirectUri: String): String =
    getAccessToken(AccessTokenConfiguration(clientId, redirectUri))

  def getAccessToken(tokenConfig: AccessTokenConfiguration): String = {
    val resolvedConfig = resolveAccessTokenConfiguration(tokenConfig)

    if (!TokenMap.contains(resolvedConfig) || JwtUtils.isTokenExpired(TokenMap(resolvedConfig))) {
      TokenMap += resolvedConfig -> fetchToken(resolvedConfig)
    }
    TokenMap(resolvedConfig)
  }

  def getMWCToken(clientId: String,
                  redirectUri: String,
                  metadataUri: String,
                  capacityId: String,
                  workspaceId: String,
                  artifactId: String,
                  workloadType: String): String = {

    val url = s"$metadataUri/v201606/generatemwctoken"

    val reqBody: String =
      s"""
         |{
         |  "artifactObjectIds": ["$artifactId"],
         |  "capacityObjectId": "$capacityId",
         |  "type": "[Start] GetMWCToken",
         |  "workloadType": "$workloadType",
         |  "workspaceObjectId": "$workspaceId"
         |}
         |""".stripMargin

    val client = FabricAuthenticatedHttpClient(clientId, redirectUri)
    val token = client.postRequest(url, reqBody).asJsObject().fields("Token").convertTo[String]
    s"MwcToken $token"
  }

  private def resolveAccessTokenConfiguration(
    tokenConfig: AccessTokenConfiguration): AccessTokenConfiguration = {
    val resolvedCertificate = tokenConfig.Certificate.orElse(sys.env.get("INTEGRATION_CERTIFICATE"))
    val resolvedCertificatePassword = tokenConfig.CertificatePassword
      .orElse(sys.env.get("INTEGRATION_CERTIFICATE_PASSWORD"))

    val resolvedPassword = resolvedCertificate match {
      case Some(_) => None
      case None =>
        tokenConfig.Password.orElse(
          sys.env.get("ut_password").orElse(
            Some(getSynapseExtensionSecret("SynapseMLFabricIntegration", "password"))
          )
        )
    }

    tokenConfig.copy(
      Certificate = resolvedCertificate,
      Password = resolvedPassword,
      CertificatePassword = resolvedCertificatePassword
    )
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

  private def fetchTokenByPassword(tokenConfig: AccessTokenConfiguration): String = {
    val createRequest = new HttpPost(s"https://${tokenConfig.Authority}/${tokenConfig.Tenant}/oauth2/token")
    createRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")
    createRequest.setEntity(new UrlEncodedFormEntity(
      List(
        ("resource", tokenConfig.Resource),
        ("client_id", tokenConfig.ClientId),
        ("grant_type", "password"),
        ("username", s"${tokenConfig.Username}@${tokenConfig.Tenant}"),
        ("password", tokenConfig.Password.getOrElse("")),
        ("scope", "openid")
      ).map(p => new BasicNameValuePair(p._1, p._2)).asJava, "UTF-8")
    )

    println("Fetching token with configuration (using password):")
    println(s"\tTenant: ${tokenConfig.Tenant}")
    println(s"\tResource: ${tokenConfig.Resource}")
    println(s"\tAuthority: ${tokenConfig.Authority}")
    println(s"\tClient ID: ${tokenConfig.ClientId}")
    println(s"\tUsername: ${tokenConfig.Username}")

    RESTHelpers.sendAndParseJson(createRequest).asJsObject().fields("access_token").convertTo[String]
  }

  private def fetchToken(tokenConfig: AccessTokenConfiguration): String = {
    (tokenConfig.Certificate, tokenConfig.Password) match {
      case (Some(_), _) =>
        fetchTokenByCertificate(tokenConfig)
      case (None, Some(_)) =>
        fetchTokenByPassword(tokenConfig)
      case (None, None) =>
        throw new IllegalArgumentException("Either Certificate or Password must be provided for authentication")
    }
  }

  private object JwtUtils {
    private case class Token(exp: Long)
    private object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val TokenFormat: RootJsonFormat[Token] = jsonFormat1(Token)
    }

    private def decodeBase64Url(str: String): String =
      new String(Base64.getUrlDecoder.decode(str.getBytes))

    def isTokenExpired(token: String): Boolean = {
      import MyJsonProtocol._
      try {
        val parts = token.split("\\.")
        if (parts.length != 3) throw new IllegalArgumentException("Invalid JWT token format")

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
