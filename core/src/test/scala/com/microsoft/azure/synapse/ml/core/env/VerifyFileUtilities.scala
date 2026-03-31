// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.{FileUtils => ApacheFileUtils}
import org.apache.hadoop.fs.Path

import java.io.File
import java.nio.file.Files

class VerifyFileUtilities extends TestBase {

  private def withTempDir(testCode: File => Unit): Unit = {
    val tmpDir = Files.createTempDirectory("test").toFile
    try {
      testCode(tmpDir)
    } finally {
      ApacheFileUtils.deleteDirectory(tmpDir)
    }
  }

  test("join(String*) produces correct path") {
    val result = FileUtilities.join("a", "b", "c")
    val expected = new File(new File("a", "b"), "c")
    assert(result === expected)
  }

  test("join(File, String*) produces correct path") {
    val base = new File("/tmp")
    val result = FileUtilities.join(base, "x", "y")
    val expected = new File(new File("/tmp", "x"), "y")
    assert(result === expected)
  }

  test("join(Path, String*) produces correct Hadoop path") {
    val base = new Path("/data")
    val result = FileUtilities.join(base, "sub", "file.txt")
    val expected = new Path(new Path("/data", "sub"), "file.txt")
    assert(result === expected)
  }

  test("writeFile and readFile roundtrip") {
    withTempDir { dir =>
      val file = new File(dir, "test.txt")
      val content = "hello world"
      writeFile(file, content, StandardOpenOption.CREATE)
      val result = readFile(file)
      assert(result === content)
    }
  }

  test("copyFile copies to target directory") {
    withTempDir { dir =>
      val src = new File(dir, "source.txt")
      writeFile(src, "copy me", StandardOpenOption.CREATE)

      val destDir = new File(dir, "dest")
      destDir.mkdirs()

      copyFile(src, destDir)
      val copied = new File(destDir, "source.txt")
      assert(copied.exists())
      assert(readFile(copied) === "copy me")
    }
  }

  test("allFiles lists all files recursively") {
    withTempDir { dir =>
      val f1 = new File(dir, "a.txt")
      writeFile(f1, "a", StandardOpenOption.CREATE)

      val sub = new File(dir, "sub")
      sub.mkdirs()
      val f2 = new File(sub, "b.txt")
      writeFile(f2, "b", StandardOpenOption.CREATE)

      val result = allFiles(dir)
      val names = result.map(_.getName).toSet
      assert(names === Set("a.txt", "b.txt"))
    }
  }

  test("allFiles with predicate filters correctly") {
    withTempDir { dir =>
      val txt = new File(dir, "keep.txt")
      writeFile(txt, "keep", StandardOpenOption.CREATE)

      val csv = new File(dir, "skip.csv")
      writeFile(csv, "skip", StandardOpenOption.CREATE)

      val result = allFiles(dir, _.getName.endsWith(".txt"))
      assert(result.length === 1)
      assert(result.head.getName === "keep.txt")
    }
  }
}
