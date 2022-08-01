// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

trait ExternalWrappableParam[T]
  extends ExternalPythonWrappableParam[T]
     with ExternalDotnetWrappableParam[T]
     with ExternalRWrappableParam[T]
