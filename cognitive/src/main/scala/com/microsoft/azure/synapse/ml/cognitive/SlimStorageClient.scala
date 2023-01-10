// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.{InvalidKeyException, NoSuchAlgorithmException}
import java.time.{OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Base64, Locale}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.collection.mutable

object SlimStorageClient {

  private def computeHMac256(base64Key: String, stringToSign: String): String = try {
    val key = Base64.getDecoder.decode(base64Key)
    val hmacSHA256 = Mac.getInstance("HmacSHA256")
    hmacSHA256.init(new SecretKeySpec(key, "HmacSHA256"))
    val utf8Bytes = stringToSign.getBytes(StandardCharsets.UTF_8)
    Base64.getEncoder.encodeToString(hmacSHA256.doFinal(utf8Bytes))
  } catch {
    case ex@(_: NoSuchAlgorithmException | _: InvalidKeyException) =>
      throw new RuntimeException(ex)
  }

  private val DateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT).withZone(ZoneId.of("UTC"))

  private def stringToSign(canonicalName: String, expiryTime: OffsetDateTime): String = {
    Seq(
      "r", // Permissions
      "", // Start Time
      DateFormatter.format(expiryTime), // expiry time
      canonicalName, //canonical name
      "", // identifier
      "", // sasIpRange
      "", //protocol
      "2020-10-02", //Version
      "b", // resource
      "", // version segment
      "", // cache control
      "", // content disposition
      "", // content encoding
      "", // content language
      "" // content type
    ).mkString("\n")
  }

  private def urlEncode(stringToEncode: String): String = {
    if (stringToEncode == null) null //scalastyle:ignore null
    else if (stringToEncode.isEmpty) ""
    else if (!stringToEncode.contains(" ")) URLEncoder.encode(stringToEncode, "utf8")
    else {
      val outBuilder = new mutable.StringBuilder
      var startDex = 0
      for (m <- 0 until stringToEncode.length) {
        if (stringToEncode.charAt(m) == ' ') {
          if (m > startDex) {
            outBuilder.append(URLEncoder.encode(stringToEncode.substring(startDex, m), "utf8"))
          }
          outBuilder.append("%20")
          startDex = m + 1
        }
      }
      if (startDex != stringToEncode.length) {
        outBuilder.append(URLEncoder.encode(stringToEncode.substring(startDex), "utf8"))
      }
      outBuilder.toString
    }
  }

  private def encode(signature: String, offset: OffsetDateTime): String = {
    val sig = urlEncode(signature)
    val se = urlEncode(DateFormatter.format(offset))
    s"sv=2020-10-02&se=$se&sr=b&sp=r&sig=$sig"
  }

  private[ml] def parseConnectionString(connectionString: String): (String, String) = {
    val connectionStringPieces = new util.HashMap[String, String]
    for (connectionStringPiece <- connectionString.split(";")) {
      val kvp = connectionStringPiece.split("=", 2)
      connectionStringPieces.put(kvp(0).toLowerCase(Locale.ROOT), kvp(1))
    }
    val accountName = connectionStringPieces.get("accountname")
    val accountKey = connectionStringPieces.get("accountkey")
    if (Option(accountName).getOrElse("").isEmpty || Option(accountKey).getOrElse("").isEmpty) {
      throw new IllegalArgumentException("Connection string must contain 'AccountName' and 'AccountKey'.")
    }
    (accountName, accountKey)
  }

  def generateReadSAS(account: String,
                      container: String,
                      blob: String,
                      key: String,
                      offset: OffsetDateTime): String = {
    val canonicalName = s"/blob/$account/$container/${blob.replace("\\", "/")}"
    encode(computeHMac256(key, stringToSign(canonicalName, offset)), offset)
  }

}
