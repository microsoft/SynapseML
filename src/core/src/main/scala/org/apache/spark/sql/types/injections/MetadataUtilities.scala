package org.apache.spark.sql.types.injections

import org.apache.spark.sql.types.Metadata

object MetadataUtilities {

  def getMetadataKeys(metadata: Metadata): Iterable[String] = metadata.map.keys

}
