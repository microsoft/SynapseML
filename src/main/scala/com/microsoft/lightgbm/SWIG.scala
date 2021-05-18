// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.lightgbm

import com.microsoft.ml.lightgbm.SWIGTYPE_p_void

class SwigPtrWrapper(val value: SWIGTYPE_p_void) extends SWIGTYPE_p_void {
  /** Helper function to get the underlying pointer address from the SWIG pointer object.
    * @return The underlying pointer address as a long.
    */
  def getCPtrValue(): Long = SWIGTYPE_p_void.getCPtr(value)
}
