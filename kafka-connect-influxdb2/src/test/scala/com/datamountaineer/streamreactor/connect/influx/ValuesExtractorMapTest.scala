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
import com.datamountaineer.streamreactor.connect.influx.data.{Foo, FooInner}
import com.datamountaineer.streamreactor.connect.influx.writers.ValuesExtractor
import com.sksamuel.avro4s.RecordFormat
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ValuesExtractorMapTest extends AnyWordSpec with Matchers {
  "ValuesExtractor" should {
    "return all the fields and their values" in {
      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)


      val map = ValuesExtractor.extractAllFields(payload, Set.empty[String]).toMap
      map("firstName") shouldBe "Alex"
      map("lastName") shouldBe "Smith"
      map("age") shouldBe 30
    }

    "return all the non filtered fields and their values" in {

      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)


      val map = ValuesExtractor.extractAllFields(payload, Set("lastName")).toMap
      map("firstName") shouldBe "Alex"
      map.contains("lastName") shouldBe false
      map("age") shouldBe 30
    }


    "throw an exception if the ts field is not present in the struct" in {
      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)

      val path = Vector("ts")
      val result = InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(payload, path), path)
      result shouldBe Symbol("Failure")
      result.failed.get shouldBe a[IllegalArgumentException]
    }


    "throw an exception if the select * from includes another complex type" in {
      val dependantMap = new java.util.HashMap[String, Any]()
      dependantMap.put("firstName", "Alex")
      dependantMap.put("lastName", "Smith")
      dependantMap.put("age", 30)

      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)
      payload.put("dependant", dependantMap)

      intercept[IllegalArgumentException] {
        ValuesExtractor.extractAllFields(payload, Set.empty[String])
      }
    }

    "extract nested fields from inner struct" in {
      val dependantMap = new java.util.HashMap[String, Any]()
      dependantMap.put("firstName", "Olivia")
      dependantMap.put("lastName", "Miru")
      dependantMap.put("age", 3)

      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)
      payload.put("dependant", dependantMap)


      ValuesExtractor.extract(payload, Vector("dependant", "firstName")) shouldBe "Olivia"
      ValuesExtractor.extract(payload, Vector("dependant", "lastName")) shouldBe "Miru"
      ValuesExtractor.extract(payload, Vector("dependant", "age")) shouldBe 3

    }

    "extract from Struct when map is involved" in {

      val dependantMap1 = new java.util.HashMap[String, Any]()
      dependantMap1.put("s", "value1")
      dependantMap1.put("t", 1.4)

      val dependantMap2 = new java.util.HashMap[String, Any]()
      dependantMap2.put("s", "value2")
      dependantMap2.put("t", 0.11)


      val inner = new java.util.HashMap[String, Any]()
      inner.put("key1", dependantMap1)
      inner.put("key2", dependantMap2)

      val payload = new java.util.HashMap[String, Any]()

      payload.put("threshold", 100)
      payload.put("map", inner)

      val s = RecordFormat[Foo]
      val avro = s.to(Foo(100, Map("key1" -> FooInner("value1", 1.4), "key2" -> FooInner("value2", 0.11))))
      val avroData = new AvroData(1)
      val foo = avroData.toConnectData(avro.getSchema, avro).value().asInstanceOf[Struct]

      ValuesExtractor.extract(foo, Vector("map", "key1", "s")) shouldBe "value1"
      ValuesExtractor.extract(foo, Vector("map", "key2", "t")) shouldBe 0.11
    }

    "does not throw an exception if the 'select * from' excludes the complex type" in {

      val dependant = new java.util.HashMap[String, Any]()
      dependant.put("firstName", "Alex")
      dependant.put("lastName", "Smith")
      dependant.put("age", 30)

      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("lastName", "Smith")
      payload.put("age", 30)
      payload.put("dependant", dependant)


      val map = ValuesExtractor.extractAllFields(payload, Set("dependant", "lastName")).toMap
      map.size shouldBe 2
      map("firstName") shouldBe "Alex"
      map("age") shouldBe 30
    }


    "throw an exception if the timestamp field is a string and incorrect format" in {

      val payload = new java.util.HashMap[String, Any]()

      payload.put("good", "2017-01-01T00:00:00Z")
      payload.put("millis", "2017-01-01T00:00:00.123Z")
      payload.put("bad", "not a time")

      InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(payload, Vector("good")), Vector("good")) shouldBe Success(1483228800000L)
      InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(payload, Vector("millis")), Vector("millis")) shouldBe Success(1483228800123L)

      val result = InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(payload, Vector("bad")), Vector("bad"))
      result shouldBe Symbol("Failure")
      result.failed.get shouldBe a[IllegalArgumentException]
    }

    "assume unix timestamp in seconds if type is double and coerce to Long in microseconds" in {

      val payload = new java.util.HashMap[String, Any]()

      payload.put("double", 1651794924.081999)

      InfluxPoint.coerceTimeStamp(ValuesExtractor.extract(payload, Vector("double")),Vector("double")) shouldBe Success(1651794924081999L)

    }

    "throw an exception if a field is in bytes" in {
      val payload = new java.util.HashMap[String, Any]()
      payload.put("firstName", "Alex")
      payload.put("bibble", Array(1.toByte, 121.toByte, -111.toByte))
      payload.put("age", 30)

      val result = Try((ValuesExtractor.extract(payload, Vector("bibble")), Vector("bibble")))
      result shouldBe Symbol("Failure")
      result.failed.get shouldBe a[IllegalArgumentException]

    }
  }
}
