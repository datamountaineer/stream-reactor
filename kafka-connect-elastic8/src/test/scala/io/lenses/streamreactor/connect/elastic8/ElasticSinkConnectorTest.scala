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
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class ElasticSinkConnectorTest extends TestBase {

  override val configDef = new Elastic8ConfigDef()
  import configDef._

  "Should start a Elastic Search Connector" in {
    //get config
    val config = getElasticSinkConfigProps()
    //get connector
    val connector = new Elastic8SinkConnector()
    //start with config
    connector.start(config.asJava)
    //check config
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.asScala.head.get(HOSTS) shouldBe ELASTIC_SEARCH_HOSTNAMES
    taskConfigs.size() shouldBe 10
    //check connector
    connector.taskClass() shouldBe classOf[Elastic8SinkTask]
    connector.stop()
  }
}
