// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.speech

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.FileUtils

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.TimeUnit

private[speech] object SpeechHlsFixture {
  def runFfmpeg(command: Seq[String], output: File): Unit = {
    val process = new ProcessBuilder(command: _*)
      .redirectError(Redirect.INHERIT)
      .redirectOutput(output)
      .start()
    try {
      require(process.waitFor(60, TimeUnit.SECONDS), "Test ffmpeg process timed out")
      require(process.exitValue() == 0, "Test ffmpeg process failed")
    } finally {
      if (process.isAlive) {
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS)
      }
    }
  }

  def withStream[T](test: String => T): T = {
    val directory = Files.createTempDirectory("speech-hls-").toFile
    try {
      val audio = new File(getClass.getResource("/audio2.wav").toURI)
      val playlist = new File(directory, "audio.m3u8")
      runFfmpeg(Seq("ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-i", audio.toString, "-c:a", "aac", "-f", "hls", "-hls_time", "4",
        "-hls_playlist_type", "vod", "-hls_segment_filename",
        new File(directory, "segment%03d.ts").toString, playlist.toString),
        new File(directory, "ffmpeg.log"))
      val files = directory.listFiles().filter(f => f.getName.endsWith(".m3u8") || f.getName.endsWith(".ts"))
        .map(f => ("/" + f.getName, Files.readAllBytes(f.toPath))).toMap
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      try {
        server.createContext("/", new HttpHandler {
          override def handle(exchange: HttpExchange): Unit = {
            try {
              files.get(exchange.getRequestURI.getPath) match {
                case Some(bytes) =>
                  val contentType = if (exchange.getRequestURI.getPath.endsWith(".m3u8")) {
                    "application/vnd.apple.mpegurl"
                  } else {
                    "video/mp2t"
                  }
                  exchange.getResponseHeaders.add("Content-Type", contentType)
                  exchange.sendResponseHeaders(200, bytes.length)
                  exchange.getResponseBody.write(bytes)
                case None =>
                  exchange.sendResponseHeaders(404, -1)
              }
            } finally {
              exchange.close()
            }
          }
        })
        server.start()
        test(s"http://127.0.0.1:${server.getAddress.getPort}/audio.m3u8")
      } finally {
        server.stop(0)
      }
    } finally {
      FileUtils.deleteDirectory(directory)
    }
  }
}
