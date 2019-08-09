
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.accumulo


import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import java.io.IOException
import java.util

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.clientImpl.{ClientContext, ScannerImpl, Tables}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.Schema
import org.apache.spark.sql.avro.AvroDeserializer
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConversions

case class SchemaMappingField(val columnFamily: String, val columnQualifier: String, val typeName: String)

// TODO: remove rowKeyTargetColumn
case class SchemaMapping(val rowKeyTargetColumn: String, mapping: Map[String, SchemaMappingField])

@SerialVersionUID(1L)
object AccumuloInputPartitionReader {
  private def catalystSchemaToJson(schema: StructType) = {

    val mappingFields = schema.fields.map(field => field.name -> new SchemaMappingField(
      field.metadata.getString("cf"),
      field.metadata.getString("cq"),
      // TODO: toUpperCase() is weird...
      catalystToAvroType(field.dataType).getName.toUpperCase
    )).toMap


    try
      new ObjectMapper().writeValueAsString(new SchemaMapping("", mappingFields))
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  private def catalystSchemaToAvroSchema(schema: StructType) = { // compile-time method binding. yes it's deprecated. yes it's the only version
    // available in the spark version deployed
    val avroFields = schema.fields.map(field =>
      new Schema.Field(
        field.name,
        Schema.create(catalystToAvroType(field.dataType)),
        null.asInstanceOf[String],
        null.asInstanceOf[JsonNode]))

    Schema.createRecord(JavaConversions.seqAsJavaList(avroFields))
  }

  private def catalystToAvroType(dataType: DataType): Schema.Type =
    dataType match {
      case DataTypes.StringType => Schema.Type.STRING
      case DataTypes.IntegerType=>Schema.Type.INT
      case DataTypes.FloatType=>Schema.Type.FLOAT
      case DataTypes.DoubleType=>Schema.Type.DOUBLE
      case DataTypes.BooleanType=>Schema.Type.BOOLEAN
      case DataTypes.LongType => Schema.Type.LONG
      case _ => throw new UnsupportedOperationException(s"Unsupported type: '${dataType}")
    }
}

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(val tableName: String, val props: Map[String, String], val schema: StructType)
  extends InputPartitionReader[InternalRow] with Serializable {
  val authorizations = new Authorizations()

  val properties = new java.util.Properties()
  properties.putAll(JavaConversions.mapAsJavaMap(props))

  val client = new ClientContext(properties)

  val tableId = Tables.getTableId(client, tableName)

  val scanner = new ScannerImpl(client, tableId, authorizations)

  // TODO: replace 20 (the priority with something...)
  val avroIterator = new IteratorSetting(20, "AVRO", "org.apache.accumulo.spark.AvroRowEncoderIterator")
  val json: String = AccumuloInputPartitionReader.catalystSchemaToJson(schema)

  // TODO: support additional user-supplied iterators
  avroIterator.addOption("schema", json)
  scanner.addScanIterator(avroIterator)

  // TODO: ?
  // scanner.setRange(baseSplit.getRange());
  val scannerIterator = scanner.iterator

  val avroSchema = AccumuloInputPartitionReader.catalystSchemaToAvroSchema(schema)
  val deserializer = new AvroDeserializer(avroSchema, schema)
  val reader = new SpecificDatumReader[GenericRecord](avroSchema)

  var row: InternalRow

  @throws[IOException]
  override def close(): Unit = {
    if (scanner != null)
      scanner.close
  }

  @throws[IOException]
  override def next: Boolean = {
    if (!scannerIterator.hasNext) return false

    val entry = scannerIterator.next
    // TODO: handle key
    // key.set(currentKey = entry.getKey());

    val data = entry.getValue.get

    // byte[] -> avro
    val decoder = DecoderFactory.get.binaryDecoder(data, null)
    val avroRecord = new GenericData.Record(avroSchema)
    reader.read(avroRecord, decoder)

    // avro to catalyst
    row = deserializer.deserialize(avroRecord).asInstanceOf[InternalRow]

    true
  }

  override def get: InternalRow = row
}
