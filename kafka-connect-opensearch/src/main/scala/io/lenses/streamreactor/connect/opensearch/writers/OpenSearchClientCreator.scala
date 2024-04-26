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
package io.lenses.streamreactor.connect.opensearch.writers

import io.lenses.streamreactor.connect.elastic.common.writers.ElasticClientCreator
import io.lenses.streamreactor.connect.opensearch.client.OpenSearchClientWrapper
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.opensearch.client.opensearch.OpenSearchClient

object OpenSearchClientCreator extends ElasticClientCreator[OpenSearchSettings] {

  /**
    * Construct a JSONWriter.
    *
    * @param config An elasticSinkConfig to extract settings from.
    * @return An ElasticJsonWriter to write records from Kafka to ElasticSearch.
    */
  override def create(config: OpenSearchSettings): Either[Throwable, OpenSearchClientWrapper] =
    for {
      transport <- config.connection.toTransport
    } yield new OpenSearchClientWrapper(transport, new OpenSearchClient(transport))

}
