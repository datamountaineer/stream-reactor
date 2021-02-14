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

package com.datamountaineer.streamreactor.connect.mongodb

import java.util

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

case class Simple(timeValue: String) {
  def toHashMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("timeValue", timeValue)
    map
  }
}

object Simple {
  val ConnectSchema: Schema = SchemaBuilder.struct
    .name("datamountaineer.blockchain.simple")
    .doc("The input instance part of a transaction.")
    .field("timeValue", Schema.STRING_SCHEMA)
    .build()

  implicit class InputToStructConverter(val simple: Simple) extends AnyVal {
    def toStruct(): Struct = {
      val struct = new Struct(ConnectSchema)
        .put("timeValue", simple.timeValue)
      struct
    }
  }

}