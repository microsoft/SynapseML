// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

class ContextObjectInputStream(input: InputStream) extends ObjectInputStream(input) {
  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
    } catch {
      case _: ClassNotFoundException => super.resolveClass(desc)
    }
  }
}
