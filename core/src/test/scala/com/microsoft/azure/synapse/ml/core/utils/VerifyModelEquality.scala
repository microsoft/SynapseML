// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyModelEquality extends TestBase {

  test("jaccardSimilarity of identical strings is 1.0") {
    assert(ModelEquality.jaccardSimilarity("hello", "hello") === 1.0)
  }

  test("jaccardSimilarity of different strings is 0.0") {
    assert(ModelEquality.jaccardSimilarity("hello", "world") === 0.0)
  }

  test("jaccardSimilarity is symmetric") {
    val s1 = "abc"
    val s2 = "def"
    assert(ModelEquality.jaccardSimilarity(s1, s2) === ModelEquality.jaccardSimilarity(s2, s1))
  }
}
