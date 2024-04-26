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

import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when

import scala.jdk.CollectionConverters.SetHasAsJava

class ElasticJsonWriterSelectionTest extends ITBase {

  "A ElasticWriter should insert into Elastic Search a number of records" in {

    mockContextForAssignment()

    val props = getElasticSinkConfigPropsSelection()
    writeAndVerifyTestRecords(props, getTestRecords)
  }

  "A ElasticWriter should insert into Elastic Search a number of records when nested fields are selected" in {

    mockContextForAssignment()

    val props = getBaseElasticSinkConfigProps(s"INSERT INTO $INDEX SELECT id, nested.string_field FROM $TOPIC")
    writeAndVerifyTestRecords(props, getTestRecordsNested)

  }

  "A ElasticWriter should update records in Elastic Search" in {

    mockContextForAssignment()

    val props = getElasticSinkUpdateConfigPropsSelection()
    writeAndVerifyTestRecords(props, getTestRecords, getUpdateTestRecord)

  }

  "A ElasticWriter should update records in Elastic Search with PK nested field" in {

    mockContextForAssignment()

    val props =
      getBaseElasticSinkConfigProps(s"UPSERT INTO $INDEX SELECT nested.id, string_field FROM $TOPIC PK nested.id")
    writeAndVerifyTestRecords(props, getTestRecordsNested, getUpdateTestRecordNested)

  }

  private def mockContextForAssignment(): Unit = {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(ASSIGNMENT.asJava)
    ()
  }

}
