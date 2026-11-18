// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import org.vowpalwabbit.spark.VowpalWabbitNative

private[vw] object VowpalWabbitNativeSupport {
  def requireSupported(): Unit = {
    if (NativeLoader.isArm64) {
      withNativeSupport {
        common.Native.load()
      }
    }
  }

  def withNativeSupport[T](operation: => T): T = {
    try {
      operation
    } catch {
      // Includes the first initializer failure and subsequent NoClassDefFoundError failures.
      case error: LinkageError if NativeLoader.isArm64 =>
        throw new UnsupportedOperationException(
          "Vowpal Wabbit is not supported on ARM with the available native library. " +
            "Install a compatible ARM64 vw_jni library or use x64 compute.", error)
    }
  }

  def apply(args: String): VowpalWabbitNative = withNativeSupport {
    new VowpalWabbitNative(args)
  }

  def apply(args: String, model: Array[Byte]): VowpalWabbitNative = withNativeSupport {
    new VowpalWabbitNative(args, model)
  }
}
