// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.io.DataInputStream

class VerifyNativeLoader extends TestBase {

  private val classFileMagic = 0xcafebabe
  private val java8MajorVersion = 52

  test("NativeLoader bytecode remains Java 8 compatible") {
    val stream = Option(classOf[NativeLoader].getResourceAsStream("NativeLoader.class"))
      .getOrElse(fail("NativeLoader.class was not found"))
    val input = new DataInputStream(stream)

    try {
      assert(input.readInt() == classFileMagic)
      input.readUnsignedShort()
      val majorVersion = input.readUnsignedShort()
      assert(
        majorVersion <= java8MajorVersion,
        s"NativeLoader class file version $majorVersion requires a newer runtime than Java 8"
      )
    } finally {
      input.close()
    }
  }
}
