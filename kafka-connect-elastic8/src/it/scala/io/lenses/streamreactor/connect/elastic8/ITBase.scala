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

package io.lenses.streamreactor.connect.elastic8

import cats.effect.unsafe.implicits.global
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import io.lenses.streamreactor.connect.elastic.common.client.ElasticClientWrapper
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettingsReader
import io.lenses.streamreactor.connect.elastic.common.writers.ElasticJsonWriter
import io.lenses.streamreactor.connect.elastic8.client.Elastic8ClientWrapper
import io.lenses.streamreactor.connect.elastic8.config.Elastic8ConfigDef
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter._
import scala.util.Using

trait ITBase extends AnyWordSpec with Matchers with BeforeAndAfter {

  val configDef = new Elastic8ConfigDef
  import configDef._

  val ELASTIC_SEARCH_HOSTNAMES = "localhost:9300"
  val BASIC_AUTH_USERNAME      = "usertest"
  val BASIC_AUTH_PASSWORD      = "userpassword"
  val TOPIC                    = "sink_test"
  val INDEX                    = "index_andrew"
  val INDEX_WITH_DATE          = s"${INDEX}_${LocalDateTime.now.format(ofPattern("YYYY-MM-dd"))}"
  val QUERY                    = s"INSERT INTO $INDEX SELECT * FROM $TOPIC"
  val QUERY_PK                 = s"INSERT INTO $INDEX SELECT * FROM $TOPIC PK id"
  val QUERY_SELECTION          = s"INSERT INTO $INDEX SELECT id, string_field FROM $TOPIC"
  val UPDATE_QUERY             = s"UPSERT INTO $INDEX SELECT * FROM $TOPIC PK id"
  val UPDATE_QUERY_SELECTION   = s"UPSERT INTO $INDEX SELECT id, string_field FROM $TOPIC PK id"

  protected val PARTITION:        Int                 = 12
  protected val PARTITION2:       Int                 = 13
  protected val TOPIC_PARTITION:  TopicPartition      = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition      = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT:       Set[TopicPartition] = Set(TOPIC_PARTITION, TOPIC_PARTITION2)

  //build a test record schema
  def createSchema: Schema =
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .build

  def createSchemaNested: Schema =
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("nested", createSchema)
      .build

  def createRecordNested(id: String): Struct =
    new Struct(createSchemaNested)
      .put("id", id)
      .put("int_field", 11)
      .put("long_field", 11L)
      .put("string_field", "11")
      .put("nested",
           new Struct(createSchema)
             .put("id", id)
             .put("int_field", 21)
             .put("long_field", 21L)
             .put("string_field", "21"),
      )

  //build a test record
  def createRecord(schema: Schema, id: String): Struct =
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")

  //generate some test records
  def getTestRecords: Vector[SinkRecord] = {
    val schema = createSchema

    ASSIGNMENT.flatMap { a =>
      (1 to 7).map { i =>
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(),
                       a.partition(),
                       Schema.STRING_SCHEMA,
                       "key",
                       schema,
                       record,
                       i.toLong,
                       System.currentTimeMillis(),
                       TimestampType.CREATE_TIME,
        )
      }
    }.toVector
  }

  def getTestRecordsNested: Vector[SinkRecord] = {
    val schema = createSchemaNested

    ASSIGNMENT.flatMap { a =>
      (1 to 7).map { i =>
        val record: Struct = createRecordNested(a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(),
                       a.partition(),
                       Schema.STRING_SCHEMA,
                       "key",
                       schema,
                       record,
                       i.toLong,
                       System.currentTimeMillis(),
                       TimestampType.CREATE_TIME,
        )
      }
    }.toVector
  }

  def getUpdateTestRecord: Vector[SinkRecord] = {
    val schema = createSchema

    ASSIGNMENT.flatMap { a =>
      (1 to 2).map { i =>
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(),
                       a.partition(),
                       Schema.STRING_SCHEMA,
                       "key",
                       schema,
                       record,
                       i.toLong,
                       System.currentTimeMillis(),
                       TimestampType.CREATE_TIME,
        )
      }
    }.toVector
  }

  def getUpdateTestRecordNested: Vector[SinkRecord] = {
    val schema = createSchemaNested

    ASSIGNMENT.flatMap { a =>
      (1 to 2).map { i =>
        val record: Struct = createRecordNested(a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(),
                       a.partition(),
                       Schema.STRING_SCHEMA,
                       "key",
                       schema,
                       record,
                       i.toLong,
                       System.currentTimeMillis(),
                       TimestampType.CREATE_TIME,
        )
      }
    }.toVector
  }

  def getElasticSinkConfigProps(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(QUERY, clusterName)

  def getElasticSinkConfigPropsSelection(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(QUERY_SELECTION, clusterName)

  def getElasticSinkConfigPropsPk(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(QUERY_PK, clusterName)

  def getElasticSinkUpdateConfigProps(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(UPDATE_QUERY, clusterName)

  def getElasticSinkUpdateConfigPropsSelection(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(UPDATE_QUERY_SELECTION, clusterName)

  def getBaseElasticSinkConfigProps(
    query:       String,
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    Map(
      "topics"        -> TOPIC,
      HOSTS           -> ELASTIC_SEARCH_HOSTNAMES,
      ES_CLUSTER_NAME -> clusterName,
      PROTOCOL        -> PROTOCOL_DEFAULT,
      KCQL            -> query,
    )

  def getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(
    autoCreate:  Boolean,
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    Map(
      HOSTS           -> ELASTIC_SEARCH_HOSTNAMES,
      ES_CLUSTER_NAME -> clusterName,
      PROTOCOL        -> PROTOCOL_DEFAULT,
      KCQL -> (QUERY + (if (autoCreate) " AUTOCREATE "
                        else "") + " WITHINDEXSUFFIX=_{YYYY-MM-dd}"),
    )

  def getElasticSinkConfigPropsHTTPClient(
    auth:        Boolean = false,
    clusterName: String  = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    Map(
      HOSTS           -> ELASTIC_SEARCH_HOSTNAMES,
      ES_CLUSTER_NAME -> clusterName,
      PROTOCOL        -> PROTOCOL_DEFAULT,
      KCQL            -> QUERY,
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> (if (auth) BASIC_AUTH_USERNAME
                                          else
                                            CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT),
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> (if (auth) BASIC_AUTH_PASSWORD
                                          else
                                            CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT),
    )

  def writeRecords(writer: ElasticJsonWriter, records: Vector[SinkRecord]): Unit =
    writer.write(records).attempt.map {
      case Left(value) => fail(value)
      case Right(_)    => ()
    }.unsafeRunSync()
  protected def writeAndVerifyTestRecords(
    props:         Map[String, String],
    testRecords:   Vector[SinkRecord],
    updateRecords: Vector[SinkRecord] = Vector.empty,
    index:         String             = INDEX,
  ): Any =
    Using.resource(LocalNode()) {

      case LocalNode(_, client) =>
        Using.resource(createElasticJsonWriter(new Elastic8ClientWrapper(client), props)) {
          writer =>
            //write records to elastic
            writeRecords(writer, testRecords)
            checkCounts(testRecords, client, index)

            if (updateRecords.nonEmpty) {
              writeRecords(writer, updateRecords)
              Thread.sleep(2000)
              checkCounts(testRecords, client, index)
            }
        }
    }

  private def checkCounts(testRecords: Vector[SinkRecord], client: ElasticClient, index: String): Assertion =
    eventually {
      val res = client.execute {
        search(index)
      }.await
      res.result.totalHits shouldBe testRecords.size
    }

  protected def createElasticJsonWriter(client: ElasticClientWrapper, props: Map[String, String]): ElasticJsonWriter =
    ElasticCommonSettingsReader.read(new Elastic8ConfigDef, props).map(new ElasticJsonWriter(client, _)).getOrElse(fail(
      "Unable to construct writer",
    ))

}
