// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.build.BuildInfo

/*
 * Centralized values for package repositories and coordinates (mostly used by test pipeline frameworks)
 */
object PackageUtils {
  val SparkMLRepository = "https://mmlspark.azureedge.net/maven"
  val SonatypeSnapshotsRepository = "https://oss.sonatype.org/content/repositories/snapshots"

  val ScalaVersionSuffix = BuildInfo.scalaVersion.split(".".toCharArray).dropRight(1).mkString(".")

  val PackageGroup = "com.microsoft.azure"

  val PackageName = s"synapseml_${ScalaVersionSuffix}"
  val PackageMavenCoordinate = s"$PackageGroup:$PackageName:${BuildInfo.version}"
  val PackageRepository = SparkMLRepository

  val OnnxProtobufPackageName = s"onnx-protobuf_${ScalaVersionSuffix}"
  val OnnxProtobufVersion = "0.9.0-1-e7f9a758-20221026-2041-SNAPSHOT"
  val OnnxProtobufMavenCoordinate = s"$PackageGroup:$OnnxProtobufPackageName:$OnnxProtobufVersion"
  val OnnxProtobufRepository = SonatypeSnapshotsRepository

  // Note: this is also hardwired in core/src/main/dotnet/test/E2ETestUtils.cs AND website/doctest.py
  val SparkMavenPackageList = s"$PackageMavenCoordinate"
  val SparkMavenRepositoryList = s"$PackageRepository,$OnnxProtobufRepository"
}
