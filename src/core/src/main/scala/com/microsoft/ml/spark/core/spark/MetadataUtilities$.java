object MetadataUtilities {

  def getMetadataKeys(metadata: Metadata): Iterable[String] = metadata.map.keys

}
