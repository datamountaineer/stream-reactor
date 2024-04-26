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
package io.lenses.streamreactor.connect.elastic8.client

import cats.effect.IO
import com.fasterxml.jackson.databind.JsonNode
import com.sksamuel.elastic4s.ElasticDsl.{ createIndex => indexCreate, _ }
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.{ ElasticClient => UnderlyingElasticClient }
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.elastic.common.client.ElasticClientWrapper
import io.lenses.streamreactor.connect.elastic.common.client.InsertRequest
import io.lenses.streamreactor.connect.elastic.common.client.Request
import io.lenses.streamreactor.connect.elastic.common.client.UpsertRequest

class Elastic8ClientWrapper(client: UnderlyingElasticClient) extends ElasticClientWrapper with LazyLogging {

  private case object IndexableJsonNode extends Indexable[JsonNode] {
    override def json(t: JsonNode): String = t.toString
  }

  override def createIndex(indexName: String): IO[Unit] =
    IO.fromFuture {
      IO {
        client.execute {
          indexCreate(indexName)
        }
      }
    } *> IO.unit

  override def close(): IO[Unit] = IO {
    client.close()
    ()
  }.recover { t: Throwable =>
    logger.error("Error during OpenSearch client shutdown", t)
    ()
  }

  override def execute(reqs: Seq[Request]): IO[Unit] =
    IO.fromFuture {
      IO {
        val indexes = reqs.map {
          case InsertRequest(index, id, json, pipeline) =>
            indexInto(new Index(index))
              .id(id)
              .pipeline(pipeline)
              .source(json.toString)
          case UpsertRequest(index, id, json) =>
            updateById(new Index(index), id)
              .docAsUpsert(json)(IndexableJsonNode)
        }
        val bulkRequest = bulk(indexes).refreshImmediately
        client.execute(bulkRequest)
      }
    } *> IO.unit
}
