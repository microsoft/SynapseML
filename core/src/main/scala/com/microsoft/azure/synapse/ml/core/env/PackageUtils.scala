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
  // val PackageMavenCoordinate = s"$PackageGroup:$PackageName:${BuildInfo.version}"
  // Use a fixed version for local testing
  val PackageMavenCoordinate = s"$PackageGroup:$PackageName:1.0.10-spark3.5"

  private val AvroCoordinate = "org.apache.spark:spark-avro_2.12:3.5.0"
  val PackageRepository: String = SparkMLRepository

  // If testing onnx package with snapshots repo, make sure to switch to using
  // OnnxProtobufRepository = SonatypeSnapshotsRepository and also adding it to SparkMavenRepositoryList
  private val OnnxProtobufPackageName = s"onnx-protobuf_$ScalaVersionSuffix"
  private val OnnxProtobufVersion = "0.9.1"
  private val OnnxProtobufMavenCoordinate = s"$PackageGroup:$OnnxProtobufPackageName:$OnnxProtobufVersion"
  private val OnnxProtobufRepository: String = SparkMLRepository

  // Note: this is also hardwired in website/doctest.py
  // val SparkMavenPackageList = s"$PackageMavenCoordinate"
  // val SparkMavenPackageList = Array(PackageMavenCoordinate, AvroCoordinate).mkString(",")
  // val SparkMavenPackageList = "org.scalanlp:breeze_2.12:2.1.0"
  // val SparkMavenPackageList = ""
  val SparkMavenPackageList = Array(PackageMavenCoordinate, AvroCoordinate).mkString(",")
  // val SparkMavenPackageList = Array(PackageMavenCoordinate, AvroCoordinate, "org.apache.commons:commons-configuration2:2.9.0", "org.apache.commons:commons-test:1.10.0").mkString(",")
  // val SparkMavenPackageList = Array(
  //   "com.microsoft.cognitiveservices.speech:client-sdk:1.24.1",
  //   "com.microsoft.ml.lightgbm:lightgbmlib:3.3.510",
  //   "com.microsoft.azure:onnx-protobuf_2.12:0.9.3",
  //   "com.linkedin.isolation-forest:isolation-forest_3.5.0_2.12:3.0.5",
  //   "com.microsoft.onnxruntime:onnxruntime_gpu:1.8.1",
  // ).mkString(",")
  val SparkMavenRepositoryList = s"$PackageRepository"
}

// OK
// org.apache.httpcomponents.client5:httpclient5:5.1.3,org.apache.httpcomponents:httpmime:4.5.13,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-core_2.12:3.5.0,org.apache.spark:spark-mllib_2.12:3.5.0,org.apache.spark:spark-tags_2.12:3.5.0,org.openpnp:opencv:3.2.0-1,org.scala-lang:scala-compiler:2.12.17,org.scala-lang:scala-library:2.12.17,org.scalactic:scalactic_2.12:3.2.14,org.scalatest:scalatest_2.12:3.2.14
// com.github.vowpalwabbit:vw-jni:9.3.0,com.globalmentor:hadoop-bare-naked-local-fs:0.1.0,com.jcraft:jsch:0.1.54,commons-lang:commons-lang:2.6,io.spray:spray-json_2.12:1.3.5,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-core_2.12:3.5.0,org.apache.spark:spark-mllib_2.12:3.5.0,org.apache.spark:spark-tags_2.12:3.5.0,org.scala-lang:scala-compiler:2.12.17,org.scala-lang:scala-library:2.12.17,org.scalactic:scalactic_2.12:3.2.14,org.scalanlp:breeze_2.12:2.1.0,org.scalatest:scalatest_2.12:3.2.14

// NOT OK
// "com.microsoft.cognitiveservices.speech:client-sdk:1.24.1",
// "com.microsoft.ml.lightgbm:lightgbmlib:3.3.510",
// "com.microsoft.azure:onnx-protobuf_2.12:0.9.3",
// "com.linkedin.isolation-forest:isolation-forest_3.5.0_2.12:3.0.5",
// "com.microsoft.onnxruntime:onnxruntime_gpu:1.8.1",