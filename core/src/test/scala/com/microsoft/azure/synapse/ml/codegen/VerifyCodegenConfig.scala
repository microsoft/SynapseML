// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import java.io.File

class VerifyCodegenConfig extends TestBase {

  private val config = CodegenConfig(
    name = "testmod",
    jarName = Some("testmod.jar"),
    topDir = "/top",
    targetDir = "/target",
    version = "1.0.0",
    pythonizedVersion = "1.0.0",
    rVersion = "1.0.0",
    packageName = "com.test"
  )

  test("generatedDir returns correct path") {
    assert(config.generatedDir === new File("/target", "generated"))
  }

  test("pySrcDir derives from srcDir") {
    assert(config.pySrcDir === new File(config.srcDir, "python"))
  }

  test("rSrcDir derives from rSrcRoot") {
    assert(config.rSrcDir === new File(config.rSrcRoot, "synapseml/R"))
  }

  test("srcDir derives from generatedDir") {
    assert(config.srcDir === new File(config.generatedDir, "src"))
  }

  test("testDir derives from generatedDir") {
    assert(config.testDir === new File(config.generatedDir, "test"))
  }

  test("copyrightLines is non-empty") {
    assert(config.copyrightLines.nonEmpty)
    assert(config.copyrightLines.contains("Copyright"))
  }

  test("scopeDepth is 4 spaces") {
    assert(config.scopeDepth === "    ")
    assert(config.scopeDepth.length === 4) // scalastyle:off magic.number
  }

  test("internalPrefix is underscore") {
    assert(config.internalPrefix === "_")
  }

  test("packageHelp produces valid content") {
    val help = config.packageHelp("import foo")
    assert(help.contains("SynapseML"))
    assert(help.contains("import foo"))
    assert(help.contains(config.pythonizedVersion))
  }
}
