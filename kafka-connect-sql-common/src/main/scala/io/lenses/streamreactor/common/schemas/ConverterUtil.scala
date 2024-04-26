/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.schemas

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.common.schemas.StructHelper._
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 22/02/16.
  * stream-reactor
  */
@deprecated("Consolidated into SinkRecord.newFilteredRecord", "3.0")
trait ConverterUtil {
  type avroSchema = org.apache.avro.Schema

  lazy val simpleJsonConverter = new SimpleJsonConverter()
  lazy val avroData            = new AvroData(100)

  //for converting json to
  @deprecated("Consolidated into SinkRecord.newFilteredRecord", "3.0")
  def connectSchemaTypeToSchema(schemaType: Schema.Type, value: Any): Schema =
    schemaType match {
      case Schema.Type.INT8    => Schema.OPTIONAL_INT8_SCHEMA
      case Schema.Type.INT16   => Schema.OPTIONAL_INT16_SCHEMA
      case Schema.Type.INT32   => Schema.OPTIONAL_INT32_SCHEMA
      case Schema.Type.INT64   => Schema.OPTIONAL_INT64_SCHEMA
      case Schema.Type.FLOAT32 => Schema.OPTIONAL_FLOAT32_SCHEMA
      case Schema.Type.FLOAT64 => Schema.OPTIONAL_FLOAT64_SCHEMA
      case Schema.Type.BOOLEAN => Schema.OPTIONAL_BOOLEAN_SCHEMA
      case Schema.Type.STRING  => Schema.OPTIONAL_STRING_SCHEMA
      case Schema.Type.BYTES   => Schema.OPTIONAL_BYTES_SCHEMA
      case Schema.Type.STRUCT  => value.asInstanceOf[Struct].schema()
      case Schema.Type.ARRAY =>
        val first = value.asInstanceOf[java.util.List[Any]].asScala
        if (first.nonEmpty) {
          val schemaType = connectSchemaTypeToSchema(ConnectSchema.schemaType(first.head.getClass), first.head)
          SchemaBuilder.array(schemaType).build()
        } else {
          SchemaBuilder.array(Schema.STRING_SCHEMA).build()
        }

      case Schema.Type.MAP =>
        val first       = value.asInstanceOf[Map[Any, Any]].head
        val keySchema   = connectSchemaTypeToSchema(ConnectSchema.schemaType(first._1.getClass), first._1)
        val valueSchema = connectSchemaTypeToSchema(ConnectSchema.schemaType(first._2.getClass), first._2)
        SchemaBuilder.map(keySchema, valueSchema).build()
    }

  /**
    * For a schemaless payload when used for a json the connect JsonConverter will provide a Map[_,_] instance as it deserializes
    * the payload
    *
    * @param record       - The connect record to extract the fields from
    * @param fields       - A map of fields alias
    * @param ignoreFields - The list of fields to leave out
    * @param key          - if true it will use record.key to do the transformation; if false will use record.value
    * @return
    */
  @deprecated("Consolidated into SinkRecord.newFilteredRecord", "3.0")
  def convertSchemalessJson(
    record:           SinkRecord,
    fields:           Map[String, String],
    ignoreFields:     Set[String] = Set.empty[String],
    key:              Boolean     = false,
    includeAllFields: Boolean     = true,
  ): java.util.Map[String, Any] = {
    val value: java.util.Map[String, Any] =
      (if (key) record.key() else record.value()) match {
        case s: java.util.Map[_, _] =>
          s.asInstanceOf[java.util.Map[String, Any]]
        case other =>
          throw new ConnectException(
            s"${other.getClass} is not valid. Expecting a Struct",
          )
      }

    ignoreFields.foreach(value.remove)
    if (!includeAllFields) {
      value.keySet().asScala.filterNot(fields.contains).foreach(value.remove)
    }

    fields
      .filter { case (field, alias) => field != alias }
      .foreach {
        case (field, alias) =>
          Option(value.get(field)).foreach { v =>
            value.remove(field)
            value.put(alias, v)
          }
      }
    value
  }

  /**
    * Handles scenarios where the sink record schema is set to string and the payload is json
    *
    * @param record              - the sink record instance
    * @param fields              - fields to include/select
    * @param ignoreFields        - fields to ignore/remove
    * @param key                 -if true it targets the sinkrecord key; otherwise it uses the sinkrecord.value
    * @param includeAllFields    - if false it will remove the fields not present in the fields parameter
    * @param ignoredFieldsValues - We need to retain the removed fields; in influxdb we might choose to set tags from ignored fields
    * @return
    */
  @deprecated("Consolidated into SinkRecord.newFilteredRecord", "3.0")
  def convertFromStringAsJson(
    record:              SinkRecord,
    fields:              Map[String, String],
    ignoreFields:        Set[String]                      = Set.empty[String],
    key:                 Boolean                          = false,
    includeAllFields:    Boolean                          = true,
    ignoredFieldsValues: Option[mutable.Map[String, Any]] = None,
  ): Either[String, ConversionResult] = {

    val schema        = if (key) record.keySchema() else record.valueSchema()
    val expectedInput = schema != null && schema.`type`() == Schema.STRING_SCHEMA.`type`()
    if (!expectedInput) Left(s"[$schema] is not handled. Expecting Schema.String")
    else {
      (if (key) record.key() else record.value()) match {
        case s: String =>
          Try(parse(s)) match {
            case Success(json) =>
              val withFieldsRemoved = ignoreFields.foldLeft(json) {
                case (j, ignored) =>
                  j.removeField {
                    case (`ignored`, v) =>
                      ignoredFieldsValues.foreach { map =>
                        val value = v match {
                          case JString(s)  => s
                          case JDouble(d)  => d
                          case JInt(i)     => i
                          case JLong(l)    => l
                          case JDecimal(d) => d
                          case _           => null
                        }
                        map += ignored -> value
                      }
                      true
                    case _ => false
                  }
              }

              val jvalue = if (!includeAllFields) {
                withFieldsRemoved.removeField { case (field, _) => !fields.contains(field) }
              } else withFieldsRemoved

              val converted = fields.filter { case (field, alias) => field != alias }
                .foldLeft(jvalue) {
                  case (j, (field, alias)) =>
                    j.transformField {
                      case JField(`field`, v) => (alias, v)
                      case other: JField => other
                    }
                }
              Right(ConversionResult(json, converted))
            case Failure(_) => Left(
                s"Invalid json with the record on topic [${record.topic}], partition [${record.kafkaPartition()}] and offset [${record.kafkaOffset()}]",
              )
          }
        case other => Left(s"${other.getClass} is not valid. Expecting a Struct")
      }
    }
  }

  case class ConversionResult(original: JValue, converted: JValue)

  /**
    * Create a Struct based on a set of fields to extract from a ConnectRecord
    *
    * @param record       The connectRecord to extract the fields from.
    * @param fields       The fields to extract.
    * @param ignoreFields Fields to ignore from the sink records.
    * @param key          Extract the fields from the key or the value of the ConnectRecord.
    * @return A new Struct with the fields specified in the fieldsMappings.
    */
  @deprecated("Consolidated into SinkRecord.newFilteredRecord", "3.0")
  def convert(
    record:       SinkRecord,
    fields:       Map[String, String],
    ignoreFields: Set[String] = Set.empty[String],
    key:          Boolean     = false,
  ): SinkRecord =
    if (
      (fields.isEmpty && ignoreFields.isEmpty) || (ignoreFields.isEmpty && fields
        .contains("*"))
    ) {
      record
    } else {
      val struct = if (key) record.key() else record.value()
      val schema = if (key) record.keySchema() else record.valueSchema()

      struct match {
        case s: Struct =>
          // apply ignore only on value
          val newStruct = s.reduceSchema(
            schema,
            fields,
            if (!key) ignoreFields else Set.empty,
          )
          new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            Schema.STRING_SCHEMA,
            "key",
            newStruct.schema(),
            newStruct,
            record.kafkaOffset(),
            record.timestamp(),
            record.timestampType(),
          )

        case other =>
          new ConnectException(
            s"[${other.getClass}] is not valid. Expecting a Struct.",
          )
          record
      }
    }

  /**
    * Convert a ConnectRecord value to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    */
  def convertValueToJson[T <: ConnectRecord[T]](
    record: ConnectRecord[T],
  ): JsonNode =
    simpleJsonConverter.fromConnectData(record.valueSchema(), record.value())

  /**
    * Convert a ConnectRecord key to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    */
  def convertKeyToJson[T <: ConnectRecord[T]](
    record: ConnectRecord[T],
  ): JsonNode =
    simpleJsonConverter.fromConnectData(record.keySchema(), record.key())

  /**
    * Convert SinkRecord to GenericRecord
    *
    * @param record ConnectRecord to convert
    * @return a GenericRecord
    */
  def convertValueToGenericAvro[T <: ConnectRecord[T]](
    record: ConnectRecord[T],
  ): GenericRecord = {
    val avro = avroData.fromConnectData(record.valueSchema(), record.value())
    avro.asInstanceOf[GenericRecord]
  }

}
