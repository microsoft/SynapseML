// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

// Wrapper for codegen system
trait WrappableParam[T]
  extends RWrappableParam[T]
    with PythonWrappableParam[T]

trait ExternalWrappableParam[T]
  extends ExternalPythonWrappableParam[T]
    with ExternalRWrappableParam[T]

