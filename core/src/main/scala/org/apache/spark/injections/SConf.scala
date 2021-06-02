// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.injections

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration

class SConf(@transient var value2: Configuration) extends SerializableConfiguration(value2)
