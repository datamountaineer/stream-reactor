/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.mongodb.sink

import java.util

import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSettings
import com.datamountaineer.streamreactor.connect.mongodb.converters.SinkRecordConverter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.json4s.jackson.JsonMethods.parse
import org.json4s.JField
import org.json4s.JsonAST.JDecimal
import org.json4s.JsonAST.JDouble
import org.json4s.JsonAST.JInt
import org.json4s.JsonAST.JLong
import org.json4s.JsonAST.JString
import org.json4s.JValue

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SinkRecordToDocument extends ConverterUtil {
  def apply(record: SinkRecord, keys: Set[String] = Set.empty)(implicit settings: MongoSettings): (Document, Iterable[(String, Any)]) = {
    val schema = record.valueSchema()
    val value = record.value()
    val fields = settings.fields.getOrElse(record.topic(), Map.empty)

    val allFields = if (fields.size == 1 && fields.head._1 == "*") true else false

    if (schema == null) {
      //try to take it as string
      value match {
        case _: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty)
          )
          //not ideal; but the compile is hashmap anyway

          val document: Document = SinkRecordConverter.fromMap(extracted.asInstanceOf[util.Map[String, AnyRef]])
          val mapDocument: util.Map[String, Any] = new util.HashMap[String, Any]()
          document.forEach((k, v) => mapDocument.put(k, v))
          document ->
            keys.headOption.map(_ => KeysExtractor.fromMap(mapDocument, keys)).getOrElse(Iterable.empty)
        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          convertFromStringAsJson(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty),
            includeAllFields = allFields) match {
            case Right(ConversionResult(original, extracted)) =>
              val extractedKeys = keys.headOption.map(_ => KeysExtractor.fromJson(original, keys)).getOrElse(Iterable.empty)
              SinkRecordConverter.fromJson(extracted) -> extractedKeys

            case Left(value) =>
              //This needs full refactor to cleanup and write FP style scala
              sys.error(value)
          }
        case Schema.Type.STRUCT =>
          val extracted = convert(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty)
          )
          SinkRecordConverter.fromStruct(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromStruct(record.value().asInstanceOf[Struct], keys)).getOrElse(Iterable.empty)

        case other => sys.error(s"$other schema is not supported")
      }
    }
  }

  def convertFromStringAsJson(record: SinkRecord,
                              fields: Map[String, String],
                              ignoreFields: Set[String] = Set.empty[String],
                              key: Boolean = false,
                              includeAllFields: Boolean = true,
                              ignoredFieldsValues: Option[mutable.Map[String, Any]] = None): Either[String, ConversionResult] = {

    val schema = if (key) record.keySchema() else record.valueSchema()
    val expectedInput = schema != null && schema.`type`() == Schema.STRING_SCHEMA.`type`()
    if (!expectedInput) Left(s"$schema is not handled. Expecting Schema.String")
    else {
      (if (key) record.key() else record.value()) match {
        case s: String =>
          Try(parse(s)) match {
            case Success(json) =>
              val withFieldsRemoved = ignoreFields.foldLeft(json) { case (j, ignored) =>
                j.removeField {
                  case (`ignored`, v) =>
                    ignoredFieldsValues.foreach { map =>
                      val value = v match {
                        case JString(s) => s
                        case JDouble(d) => d
                        case JInt(i) => i
                        case JLong(l) => l
                        case JDecimal(d) => d
                        case _ => null
                      }
                      map += ignored -> value
                    }
                    true
                  case _ => false
                }
              }

              val converted = fields.filter { case (field, alias) => field != alias }
                .foldLeft(withFieldsRemoved) { case (j, (field, alias)) =>
                  j.transformField {
                    case JField(`field`, v) => (alias, v)
                    case other: JField => other
                  }
                }
              Right(ConversionResult(json, converted))
            case Failure(_) => Left(s"Invalid json with the record on topic ${record.topic} and offset ${record.kafkaOffset()}")
          }
        case other => Left(s"${other.getClass} is not valid. Expecting a Struct")
      }
    }
  }

  case class ConversionResult(original: JValue, converted: JValue)

}
