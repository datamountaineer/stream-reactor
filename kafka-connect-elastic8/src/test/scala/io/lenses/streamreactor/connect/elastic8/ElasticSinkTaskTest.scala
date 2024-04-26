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

import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar

import scala.jdk.CollectionConverters.MapHasAsJava

class ElasticSinkTaskTest extends TestBase with MockitoSugar {
  "A ElasticSinkTask should start and write to Elastic Search" in {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get config
    val config = getElasticSinkConfigProps()
    //get task
    val task = new Elastic8SinkTask()
    //initialise the tasks context
    task.initialize(context)
    //check version
    task.version() shouldBe ""
    //start task
    task.start(config.asJava)
    //simulate the call from Connect
    //task.put(testRecords.asJava)
    //stop task
    task.stop()
  }
}
