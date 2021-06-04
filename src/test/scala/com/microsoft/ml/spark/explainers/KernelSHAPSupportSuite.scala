// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.sum
import com.microsoft.ml.spark.core.test.base.TestBase
import org.scalatest.Matchers.{an, convertToAnyShouldWrapper, thrownBy}

class KernelSHAPSupportSuite extends TestBase with KernelSHAPSamplerSupport {

  override protected def featureSize: Int = 0
  override protected def numSamples: Int = 0

  test("generateCoalitions should generate correct coalitions") {

    an[AssertionError] shouldBe thrownBy {
      generateCoalitions(0, 1000)
    }

    an[AssertionError] shouldBe thrownBy {
      generateCoalitions(5, -1)
    }

    val c1 = this.generateCoalitions(5, 1000).toList
    c1.length shouldBe 32

    c1.count(coal => sum(coal) == 0) shouldBe 1
    c1.count(coal => sum(coal) == 5) shouldBe 1
    c1.count(coal => sum(coal) == 1) shouldBe 5
    c1.count(coal => sum(coal) == 4) shouldBe 5
    c1.count(coal => sum(coal) == 2) shouldBe 10
    c1.count(coal => sum(coal) == 3) shouldBe 10

    val c2 = this.generateCoalitions(1000, 1).toList
    c2.length shouldBe 1
    c2.count(coal => sum(coal) == 0) shouldBe 1

    val c3 = this.generateCoalitions(1000, 2003).toList
    c3.length shouldBe 2003
    c3.count(coal => sum(coal) == 0) shouldBe 1
    c3.count(coal => sum(coal) == 1000) shouldBe 1
    c3.count(coal => sum(coal) == 1) shouldBe 1000
    c3.count(coal => sum(coal) == 999) shouldBe 1000
    c3.count(coal => sum(coal) == 2) shouldBe 1
  }
}
