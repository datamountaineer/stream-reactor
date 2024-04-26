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
package io.lenses.streamreactor.connect.opensearch.client

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.elastic.common.client.ElasticClientWrapper
import io.lenses.streamreactor.connect.elastic.common.client.InsertRequest
import io.lenses.streamreactor.connect.elastic.common.client.Request
import io.lenses.streamreactor.connect.elastic.common.client.UpsertRequest
import org.opensearch.client.opensearch.{ OpenSearchClient => UnderlyingOpenSearchClient }
import org.opensearch.client.opensearch._types.Refresh
import org.opensearch.client.opensearch.core.BulkRequest
import org.opensearch.client.opensearch.core.bulk.BulkOperation
import org.opensearch.client.opensearch.core.bulk.IndexOperation
import org.opensearch.client.opensearch.core.bulk.UpdateOperation
import org.opensearch.client.opensearch.indices.CreateIndexRequest
import org.opensearch.client.transport.OpenSearchTransport

import scala.jdk.CollectionConverters.SeqHasAsJava

class OpenSearchClientWrapper(transport: OpenSearchTransport, client: UnderlyingOpenSearchClient)
    extends ElasticClientWrapper
    with LazyLogging {

  override def createIndex(indexName: String): IO[Unit] =
    IO {
      val createIndexRequest = new CreateIndexRequest.Builder()
        .index(indexName)
        .build()
      client.indices().create(createIndexRequest)
    } *> IO.unit

  override def close(): IO[Unit] = IO {
    transport.close()
    ()
  }.recover { t: Throwable =>
    logger.error("Error during OpenSearch client shutdown", t)
    ()
  }

  override def execute(reqs: Seq[Request]): IO[Unit] =
    IO {
      val bulkOps: List[BulkOperation] = reqs.map {
        case InsertRequest(index, id, json, pipeline) =>
          new BulkOperation.Builder().index(
            new IndexOperation.Builder().index(index).id(id).document(json).pipeline(pipeline).build(),
          ).build()
        case UpsertRequest(index, id, json) =>
          new BulkOperation.Builder().update(
            new UpdateOperation.Builder().index(index).id(id).document(json).docAsUpsert(true).build(),
          ).build()
      }.toList

      val bulkReq = new BulkRequest.Builder().refresh(Refresh.True).operations(bulkOps.asJava).build()
      client.bulk(bulkReq)
      ()
    }
}
