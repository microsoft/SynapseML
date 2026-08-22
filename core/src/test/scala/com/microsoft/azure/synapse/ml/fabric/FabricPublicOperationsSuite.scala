// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class FabricPublicOperationsSuite extends AnyFunSuite {
  private val workspaceId = "01234567-89ab-cdef-0123-456789abcdef"
  private val storeId = "fedcba98-7654-3210-fedc-ba9876543210"
  private val digest = "0123456789abcdef" * 4

  test("Inject exact package provenance into a direct batch script") {
    val script = Files.createTempFile("fabric-public-batch", ".py")
    val source = "print(1 + 1)\n"
    try {
      Files.write(script, source.getBytes(StandardCharsets.UTF_8))
      val batchSource = new String(
        FabricPublicOperations.scriptSource(script.toFile, Some(digest)),
        StandardCharsets.UTF_8)

      assert(batchSource.endsWith(source))
      assert(batchSource.contains("SYNAPSEML_CORE_PROVENANCE"))
      assert(batchSource.contains(digest))
      intercept[IllegalArgumentException] {
        FabricPublicOperations.scriptSource(script.toFile, Some("not-a-digest"))
      }
    } finally {
      Files.deleteIfExists(script)
    }
  }

  test("Build a direct batch command with the exact core package") {
    val directory = Files.createTempDirectory("fabric-public-command")
    val script = directory.resolve("notebook.py")
    val jar = directory.resolve("synapseml-core.jar")
    try {
      Files.write(script, "print(1)\n".getBytes(StandardCharsets.UTF_8))
      Files.write(jar, Array[Byte](1, 2, 3))
      val command = FabricPublicOperations.batchSubmitCommand(
        "fabric-spark-cli",
        script.toFile,
        "SynapseML-test",
        workspaceId,
        storeId,
        "msit",
        directory.toFile,
        Some(jar.toFile))

      assert(command.take(5) == Seq(
        "fabric-spark-cli", "batch", "submit", "--backend", "fabric"))
      assert(optionValue(command, "--py") == script.toFile.getAbsolutePath)
      assert(optionValue(command, "--workspace") == workspaceId)
      assert(optionValue(command, "--lakehouse") == storeId)
      assert(optionValue(command, "--extra-jars") == jar.toFile.getAbsolutePath)
      assert(command.contains("--no-create-lakehouse"))
      assert(command.contains("--no-m2"))
      assert(!command.contains("--environment"))
    } finally {
      Files.deleteIfExists(script)
      Files.deleteIfExists(jar)
      Files.deleteIfExists(directory)
    }
  }

  test("Omit package options from a package-free smoke batch") {
    val directory = Files.createTempDirectory("fabric-public-smoke")
    val script = directory.resolve("smoke.py")
    try {
      Files.write(script, "print(1 + 1)\n".getBytes(StandardCharsets.UTF_8))
      val command = FabricPublicOperations.batchSubmitCommand(
        "fabric-spark-cli",
        script.toFile,
        "OnePlusOne",
        workspaceId,
        storeId,
        "msit",
        directory.toFile,
        coreJar = None)

      assert(!command.contains("--extra-jars"))
      assert(!command.contains("--no-m2"))
    } finally {
      Files.deleteIfExists(script)
      Files.deleteIfExists(directory)
    }
  }

  test("Build a scoped non-interactive batch cancellation command") {
    val command = FabricPublicOperations.batchCancelCommand(
      "fabric-spark-cli",
      "SynapseML-test",
      workspaceId,
      "Lakehouse-test",
      "msit")

    assert(command.take(5) == Seq(
      "fabric-spark-cli", "batch", "cancel", "--backend", "fabric"))
    assert(optionValue(command, "--name") == "SynapseML-test")
    assert(optionValue(command, "--workspace") == workspaceId)
    assert(optionValue(command, "--lakehouse") == "Lakehouse-test")
    assert(command.contains("--yes"))
    assert(command.contains("--no-create-lakehouse"))
  }

  test("Resolve exactly one current core package") {
    val root = Files.createTempDirectory("fabric-core-package")
    val scalaTarget = root.resolve("core").resolve("target").resolve("scala-test")
    val corePackage = scalaTarget.resolve("synapseml-core_2.12-1.2.3.jar")
    try {
      Files.createDirectories(scalaTarget)
      Files.write(corePackage, Array[Byte](1, 2, 3))

      assert(
        FabricPublicOperations.resolveCorePackage(
          root.toFile, "1.2.3", configuredPath = None) == corePackage.toFile.getCanonicalFile)
    } finally {
      Files.deleteIfExists(corePackage)
      Files.deleteIfExists(scalaTarget)
      Files.deleteIfExists(scalaTarget.getParent)
      Files.deleteIfExists(scalaTarget.getParent.getParent)
      Files.deleteIfExists(root)
    }
  }

  test("Reject a missing configured core package") {
    val error = intercept[IllegalArgumentException] {
      FabricPublicOperations.resolveCorePackage(
        new java.io.File("."),
        "1.2.3",
        Some("missing-core-assembly.jar"))
    }
    assert(error.getMessage.contains("exactly one current SynapseML core package jar"))
  }

  private def optionValue(command: Seq[String], option: String): String = {
    command(command.indexOf(option) + 1)
  }
}
