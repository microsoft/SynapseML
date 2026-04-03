// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyPackageUtils extends TestBase {

  test("ScalaVersionSuffix is extracted from BuildInfo.scalaVersion") {
    // Scala version is typically like "2.12.15" or "2.13.x"
    // ScalaVersionSuffix should be "2.12" or "2.13"
    val suffix = PackageUtils.ScalaVersionSuffix
    assert(suffix.split("\\.").length === 2)
    assert(suffix.startsWith("2."))
  }

  test("PackageGroup has expected value") {
    assert(PackageUtils.PackageGroup === "com.microsoft.azure")
  }

  test("PackageName contains scala version suffix") {
    assert(PackageUtils.PackageName.startsWith("synapseml_"))
    assert(PackageUtils.PackageName.contains(PackageUtils.ScalaVersionSuffix))
  }

  test("PackageMavenCoordinate has correct format") {
    val coord = PackageUtils.PackageMavenCoordinate
    // Format should be: group:artifact:version
    val parts = coord.split(":")
    assert(parts.length === 3)
    assert(parts(0) === PackageUtils.PackageGroup)
    assert(parts(1) === PackageUtils.PackageName)
    assert(parts(2) === BuildInfo.version)
  }

  test("PackageRepository is a valid URL") {
    val repo = PackageUtils.PackageRepository
    assert(repo.startsWith("https://"))
  }

  test("SparkMavenPackageList contains package coordinate") {
    val packages = PackageUtils.SparkMavenPackageList
    assert(packages.contains(PackageUtils.PackageMavenCoordinate))
  }

  test("SparkMavenPackageList contains spark-avro") {
    val packages = PackageUtils.SparkMavenPackageList
    assert(packages.contains("spark-avro"))
  }

  test("SparkMavenRepositoryList is set") {
    val repos = PackageUtils.SparkMavenRepositoryList
    assert(repos.nonEmpty)
    assert(repos === PackageUtils.PackageRepository)
  }
}
