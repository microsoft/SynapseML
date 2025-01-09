// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.build.BuildInfo

/*
 * Centralized values for package repositories and coordinates (mostly used by test pipeline frameworks)
 */
object PackageUtils {
  private val SparkMLRepository = "https://mmlspark.azureedge.net/maven"
  private val SonatypeSnapshotsRepository = "https://oss.sonatype.org/content/repositories/snapshots"

  val ScalaVersionSuffix: String = BuildInfo.scalaVersion.split(".".toCharArray).dropRight(1).mkString(".")

  val PackageGroup = "com.microsoft.azure"

  val PackageName = s"synapseml_$ScalaVersionSuffix"
  val PackageMavenCoordinate = s"$PackageGroup:$PackageName:${BuildInfo.version}"
  // Use a fixed version for local testing
  // val PackageMavenCoordinate = s"$PackageGroup:$PackageName:1.0.9"

  private val AvroCoordinate = "org.apache.spark:spark-avro_2.12:3.4.1"
  val PackageRepository: String = SparkMLRepository

  // If testing onnx package with snapshots repo, make sure to switch to using
  // OnnxProtobufRepository = SonatypeSnapshotsRepository and also adding it to SparkMavenRepositoryList
  private val OnnxProtobufPackageName = s"onnx-protobuf_$ScalaVersionSuffix"
  private val OnnxProtobufVersion = "0.9.1"
  private val OnnxProtobufMavenCoordinate = s"$PackageGroup:$OnnxProtobufPackageName:$OnnxProtobufVersion"
  private val OnnxProtobufRepository: String = SparkMLRepository

  // Note: this is also hardwired in website/doctest.py
  // val SparkMavenPackageList = s"$PackageMavenCoordinate"
  val SparkMavenPackageList = Array(PackageMavenCoordinate, AvroCoordinate).mkString(",")
  val SparkMavenRepositoryList = s"$PackageRepository"
}
