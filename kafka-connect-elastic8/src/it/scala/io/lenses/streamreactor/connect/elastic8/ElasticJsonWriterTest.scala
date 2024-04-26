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

import com.sksamuel.elastic4s.ElasticDsl._
import io.lenses.streamreactor.connect.elastic8.client.Elastic8ClientWrapper
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually

import java.util.UUID
import scala.util.Using

class ElasticJsonWriterTest extends ITBase with MockitoSugar with BeforeAndAfterEach {

  class TestContext {

    val RandomClusterName: String             = UUID.randomUUID().toString
    val TestRecords:       Vector[SinkRecord] = getTestRecords

  }

  "A ElasticWriter should insert into Elastic Search a number of records" in new TestContext {

    val props = getElasticSinkConfigProps(RandomClusterName)

    writeAndVerifyTestRecords(props, TestRecords)

  }

  "A ElasticWriter should update a number of records in Elastic Search" in new TestContext {
    val props = getElasticSinkUpdateConfigProps(RandomClusterName)

    writeAndVerifyTestRecords(props, getTestRecords, getUpdateTestRecord)
  }

  "A ElasticWriter should update a number of records in Elastic Search with index suffix defined" in new TestContext {

    val props = getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = true)

    writeAndVerifyTestRecords(props, getTestRecords, getUpdateTestRecord, INDEX_WITH_DATE)

  }

  "It should fail writing to a non-existent index when auto creation is disabled" in new TestContext {

    val props = getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = false, RandomClusterName)

    Using.resource(LocalNode()) {
      case LocalNode(_, client) =>
        Using.resource(createElasticJsonWriter(new Elastic8ClientWrapper(client), props)) {
          writer =>
            writer.write(TestRecords)
            eventually {
              val searchResponse = client.execute {
                search(INDEX_WITH_DATE)
              }.await
              searchResponse.isError should be(true)
              searchResponse.error.`type` should be("index_not_found_exception")
            }
        }
    }

  }

  "A ElasticWriter should insert into Elastic Search a number of records with the HTTP Client" in new TestContext {

    val props = getElasticSinkConfigPropsHTTPClient()

    writeAndVerifyTestRecords(props, TestRecords)
  }

  "A ElasticWriter should insert into with PK Elastic Search a number of records" in new TestContext {

    val props = getElasticSinkConfigPropsPk(RandomClusterName)

    writeAndVerifyTestRecords(props, TestRecords, TestRecords)

  }

  "A ElasticWriter should insert into without PK Elastic Search a number of records" in new TestContext {

    val props = getElasticSinkConfigProps(RandomClusterName)

    writeAndVerifyTestRecords(props, TestRecords, TestRecords)

  }
}
