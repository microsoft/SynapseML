// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

class ValidateConfiguration extends TestBase {

  test("Basic BrainScript config E2E") {
    val relativeOutRoot = "out"
    val remappedInPath = "in.txt"
    val dataFormat = "text"

    val config = new BrainScriptBuilder()
      .setOutputRoot(relativeOutRoot)
      .setInputFile(
        remappedInPath,
        dataFormat,
        Map("features" -> InputShape(10000, "sparse"),
            "labels" -> InputShape(1, "dense")))

    val cb = new CNTKCommandBuilder(false)
      .appendOverrideConfig(config.toOverrideConfig)

    // TODO: add assertions to really validate instead
    println(cb.buildCommand)
  }

}
