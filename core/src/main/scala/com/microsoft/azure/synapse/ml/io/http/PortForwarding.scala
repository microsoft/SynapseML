// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.jcraft.jsch.{JSch, Session}
import org.apache.commons.io.IOUtils

import java.io.File
import java.net.URI

object PortForwarding {

  lazy val Jsch = new JSch()

  def forwardPortToRemote(username: String,
                          sshHost: String,
                          sshPort: Int,
                          bindAddress: String,
                          remotePortStart: Int,
                          localHost: String,
                          localPort: Int,
                          keyDir: Option[String],
                          keySas: Option[String],
                          maxRetries: Int,
                          timeout: Int
                         ): (Session, Int) = {
    keyDir.foreach(kd =>
      new File(kd).listFiles().foreach(f =>
        try {
          Jsch.addIdentity(f.getAbsolutePath)
        } catch {
          case _: com.jcraft.jsch.JSchException =>
          case e: Exception => throw e
        }
      )
    )

    keySas.foreach { ks =>
      val privateKeyBytes = IOUtils.toByteArray(new URI(ks))
      Jsch.addIdentity("forwardingKey", privateKeyBytes, null, null) //scalastyle:ignore null
    }

    val session = Jsch.getSession(username, sshHost, sshPort)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setTimeout(timeout)
    session.connect()
    var attempt = 0
    var foundPort: Option[Int] = None
    while (foundPort.isEmpty && attempt <= maxRetries) {  //scalastyle:ignore while
      try {
        session.setPortForwardingR(
          bindAddress, remotePortStart + attempt, localHost, localPort)
        foundPort = Some(remotePortStart + attempt)
      } catch {
        case _: Exception =>
          println(s"failed to forward port. Attempt: $attempt")
          attempt += 1
      }
    }
    if (foundPort.isEmpty) {
      throw new RuntimeException(s"Could not find open port between " +
        s"$remotePortStart and ${remotePortStart + maxRetries}")
    }
    println(s"forwarding to ${foundPort.get}")
    (session, foundPort.get)
  }

  def forwardPortToRemote(options: Map[String, String]): (Session, Int) = {
    forwardPortToRemote(
      options("forwarding.username"),
      options("forwarding.sshhost"),
      options.getOrElse("forwarding.sshport", "22").toInt,
      options.getOrElse("forwarding.bindaddress", "*"),
      options.get("forwarding.remoteportstart")
        .orElse(options.get("forwarding.localport")).get.toInt,
      options.getOrElse("forwarding.localhost", "0.0.0.0"),
      options("forwarding.localport").toInt,
      options.get("forwarding.keydir"),
      options.get("forwarding.keysas"),
      options.getOrElse("forwarding.maxretires", "50").toInt,
      options.getOrElse("forwarding.timeout", "20000").toInt
    )
  }

}
