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
package io.lenses.streamreactor.connect.elastic8

import io.lenses.streamreactor.connect.elastic8.config.Elastic8ConfigDef
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter._
import java.util
import scala.jdk.CollectionConverters.SetHasAsJava

trait TestBase extends AnyWordSpec with Matchers with BeforeAndAfter {

  val configDef = new Elastic8ConfigDef()
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

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] =
    ASSIGNMENT.asJava

  def getElasticSinkConfigProps(
    clusterName: String = ES_CLUSTER_NAME_DEFAULT,
  ): Map[String, String] =
    getBaseElasticSinkConfigProps(QUERY, clusterName)

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
}
