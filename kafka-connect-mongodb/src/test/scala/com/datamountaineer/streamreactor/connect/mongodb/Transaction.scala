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
import java.util.Collections

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

case class Transaction(lock_time: Long,
                       ver: Int,
                       size: Long,
                       inputs: Seq[Input],
                       rbf: Option[Boolean],
                       time: Long,
                       tx_index: Long,
                       vin_sz: Int,
                       hash: String,
                       vout_sz: Int,
                       relayed_by: String,
                       out: Seq[Output],
                       date: String,
                       simple: Simple ) {
  def toHashMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("lock_time", lock_time)
    map.put("ver", ver)
    map.put("size", size)
    val inputsArr = new util.ArrayList[Any]()
    inputs.foreach(i=>inputsArr.add(i.toHashMap))
    map.put("inputs", inputsArr)
    rbf.foreach(map.put("rbf", _))
    map.put("time", time)
    map.put("tx_index", tx_index)
    map.put("vin_sz", vin_sz)
    map.put("hash", hash)
    map.put("vout_sz", vout_sz)
    map.put("relayed_by", relayed_by)
    map.put("date", date)
    map.put("simple", simple.toHashMap)

    val outArr = new util.ArrayList[Any]()
    out.foreach(o => outArr.add(o.toHashMap))
    map.put("out", outArr)
    map.put("_id", lock_time)
    map
  }
}


object Transaction {
  val ConnectSchema = SchemaBuilder.struct
    .name("datamountaineer.blockchain.transaction")
    .field("lock_time", Schema.INT64_SCHEMA)
    .field("ver", Schema.INT32_SCHEMA)
    .field("size", Schema.INT64_SCHEMA)
    .field("inputs", SchemaBuilder.array(Input.ConnectSchema).optional().build())
    .field("rbf", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("time", Schema.INT64_SCHEMA)
    .field("tx_index", Schema.INT64_SCHEMA)
    .field("vin_sz", Schema.INT32_SCHEMA)
    .field("hash", Schema.STRING_SCHEMA)
    .field("vout_sz", Schema.INT32_SCHEMA)
    .field("relayed_by", Schema.STRING_SCHEMA)
    .field("out", SchemaBuilder.array(Output.ConnectSchema).optional().build())
    .field("simple",  Simple.ConnectSchema.schema())
    .build()

  implicit class TransactionToSourceRecordConverter(val tx: Transaction) extends AnyVal {
    def toSourceRecord(topic: String, partition: Int, key: Option[String]) = {
      new SourceRecord(Option(key).map(Collections.singletonMap("Blockchain", _)).orNull,
        getOffset(),
        topic,
        partition,
        key.map(_ => Schema.STRING_SCHEMA).orNull,
        key.orNull,
        ConnectSchema,
        tx.toStruct()
      )
    }

    private def getOffset() = Collections.singletonMap("position", System.currentTimeMillis())

    def toStruct(): Struct = {
      val struct = new Struct(ConnectSchema)
        .put("lock_time", tx.lock_time)
        .put("ver", tx.ver)
        .put("size", tx.size)
        .put("time", tx.time)
        .put("tx_index", tx.tx_index)
        .put("vin_sz", tx.vin_sz)
        .put("hash", tx.hash)
        .put("vout_sz", tx.vout_sz)
        .put("relayed_by", tx.relayed_by)
        .put("simple", tx.simple
        )

      tx.out.headOption.foreach { _ =>
        import scala.collection.JavaConverters._
        struct.put("out", tx.out.map(_.toStruct()).asJava)
      }
      tx.rbf.foreach(struct.put("rbf", _))
      tx.inputs.headOption.foreach { _ =>
        val inputs = new util.ArrayList[Struct]
        tx.inputs.foreach(i => inputs.add(i.toStruct()))
        struct.put("inputs", inputs)
      }
      tx.out.headOption.foreach { _ =>
        val outputs = new util.ArrayList[Struct]
        tx.out.foreach(output => outputs.add(output.toStruct()))
      }

      struct
    }
  }

}