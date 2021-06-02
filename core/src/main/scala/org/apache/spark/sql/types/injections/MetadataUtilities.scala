// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.types.injections

import org.apache.spark.sql.types.Metadata

object MetadataUtilities {

  def getMetadataKeys(metadata: Metadata): Iterable[String] = metadata.map.keys

}
