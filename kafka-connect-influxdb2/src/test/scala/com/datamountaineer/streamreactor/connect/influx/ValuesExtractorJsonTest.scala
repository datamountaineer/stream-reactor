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

package com.datamountaineer.streamreactor.connect.influx

import com.datamountaineer.streamreactor.connect.influx.converters.InfluxPoint
import com.datamountaineer.streamreactor.connect.influx.writers.ValuesExtractor
import com.landoop.json.sql.JacksonJson
import com.typesafe.scalalogging.LazyLogging
import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.GenericData
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ValuesExtractorJsonTest extends AnyWordSpec with Matchers with LazyLogging {
  val avroData = new AvroData(8)

  "ValuesExtractor" should {
    "return all the fields and their values" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct: Struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)


      val json = JacksonJson.asJson(avroData.fromConnectData(schema, struct).toString)
      val map = ValuesExtractor.extractAllFields(json, Set.empty[String]).toMap
      map("firstName") shouldBe "Alex"
      map("lastName") shouldBe "Smith"
      map("age") shouldBe 30
    }

    "return all the non filtered fields and their values" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct: Struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val json = JacksonJson.asJson(avroData.fromConnectData(schema, struct).toString)
      val map = ValuesExtractor.extractAllFields(json, Set("lastName")).toMap
      map("firstName") shouldBe "Alex"
      map.contains("lastName") shouldBe false
      map("age") shouldBe 30
    }


    "throw an exception if the ts field is not present in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val json = JacksonJson.asJson(avroData.fromConnectData(schema, struct).toString)
      val path = Vector("ts")
      val result = InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(json, path), path)
      result shouldBe Symbol("Failure")
      result.failed.get shouldBe a[IllegalArgumentException]

    }


    "throw an exception if the select * from includes another struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build()

      val schemaParent = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("dependant", schema)
        .build()


      val dependant = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val parent = new Struct(schemaParent)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)
        .put("dependant", dependant)

      val json = JacksonJson.asJson(avroData.fromConnectData(parent.schema(), parent).toString)
      intercept[IllegalArgumentException] {
        ValuesExtractor.extractAllFields(json, Set.empty[String])
      }
    }

    "extract nested fields from inner struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build()

      val schemaParent = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("dependant", schema)
        .build()


      val dependant = new Struct(schema)
        .put("firstName", "Olivia")
        .put("lastName", "Miru")
        .put("age", 3)

      val parent = new Struct(schemaParent)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)
        .put("dependant", dependant)

      val json = JacksonJson.asJson(avroData.fromConnectData(parent.schema(), parent).toString)
      ValuesExtractor.extract(json, Vector("dependant", "firstName")) shouldBe "Olivia"
      ValuesExtractor.extract(json, Vector("dependant", "lastName")) shouldBe "Miru"
      ValuesExtractor.extract(json, Vector("dependant", "age")) shouldBe 3

    }

    "extract from Struct when map is involved" in {

      //val s = RecordFormat[Foo]
      //val avro = s.to(Foo(100, Map("key1" -> FooInner("value1", 1.4), "key2" -> FooInner("value2", 0.11))))
      //val avroAsString = avro.toString

      // TODO: The behaviour of avro.toString has changed so it no longer converts to JSON, breaking this test.

      val avroAsString = """{"v": 100, "map": {"key1": {"s": "value1", "t": 1.4}, "key2": {"s": "value2", "t": 0.11}}}"""
      val json = JacksonJson.asJson(avroAsString)

      ValuesExtractor.extract(json, Vector("map", "key1", "s")) shouldBe "value1"
      ValuesExtractor.extract(json, Vector("map", "key2", "t")) shouldBe 0.11
    }

    "does not throw an exception if the 'select * from' excludes the complex type" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build()

      val schemaParent = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("dependant", schema)
        .build()


      val dependant = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val parent = new Struct(schemaParent)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)
        .put("dependant", dependant)

      val json = JacksonJson.asJson(avroData.fromConnectData(parent.schema(), parent).toString)

      val map = ValuesExtractor.extractAllFields(json, Set("dependant", "lastName")).toMap
      map.size shouldBe 2
      map("firstName") shouldBe "Alex"
      map("age") shouldBe 30
    }


    "throw an exception if the timestamp field is a string and incorrect format" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("good", Schema.STRING_SCHEMA)
        .field("millis", Schema.STRING_SCHEMA)
        .field("bad", Schema.STRING_SCHEMA).build()

      val struct = new Struct(schema)
        .put("good", "2017-01-01T00:00:00Z")
        .put("millis", "2017-01-01T00:00:00.123Z")
        .put("bad", "not a time")

      val json = JacksonJson.asJson(avroData.fromConnectData(schema, struct).toString)

      InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(json, Vector("good")), Vector("good")) shouldBe Success(1483228800000L)
      InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(json, Vector("millis")), Vector("millis")) shouldBe Success(1483228800123L)

      val result = InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(json, Vector("bad")), Vector("bad"))
      result shouldBe Symbol("Failure")
      result.failed.get shouldBe a[IllegalArgumentException]
    }

    "throw an exception if a field is in bytes" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("bibble", Schema.BYTES_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("bibble", Array(1.toByte, 121.toByte, -111.toByte))
        .put("age", 30)

      avroData.fromConnectData(schema, struct) match {
        case _: GenericData.Record =>
          // TODO: avroData.fromConnectData has changed the format that it serialises to
          // Therefore the string below is hard-coded for the time being
          //val json = avroData.toString
          val json = JacksonJson.asJson("{\"bibble\": {\"bytes\": \"\\u0001y\\u0091\"}}")
          val result: Try[(Any, Vector[String])] = Try((ValuesExtractor.extract(json, Vector("bibble")), Vector("bibble")))
          result shouldBe Symbol("Failure")
          result.failed.get shouldBe a[IllegalArgumentException]

        case _ => fail("Should have been a GenericData.Record");
      }

    }
  }
}
