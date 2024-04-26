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

import cats.effect.testing.scalatest.AsyncIOSpec
import com.fasterxml.jackson.databind.node.TextNode
import io.lenses.streamreactor.connect.elastic.common.client.InsertRequest
import io.lenses.streamreactor.connect.elastic.common.client.UpsertRequest
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Answers
import org.mockito.MockitoSugar
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem
import org.opensearch.client.opensearch.core.BulkRequest
import org.opensearch.client.opensearch.core.BulkResponse
import org.opensearch.client.opensearch.indices.CreateIndexRequest
import org.opensearch.client.opensearch.indices.CreateIndexResponse
import org.opensearch.client.transport.OpenSearchTransport
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.SeqHasAsJava

class OpenSearchClientWrapperTest extends AsyncFunSuite with AsyncIOSpec with Matchers with MockitoSugar {

  test("createIndex should create an index successfully") {
    val mockClient    = mock[OpenSearchClient](Answers.RETURNS_DEEP_STUBS)
    val mockTransport = mock[OpenSearchTransport]

    val clientWrapper = new OpenSearchClientWrapper(mockTransport, mockClient)
    val indexName     = "test_index"
    when(mockClient.indices().create(any[CreateIndexRequest])).thenReturn(
      new CreateIndexResponse.Builder().index(indexName).shardsAcknowledged(true).build(),
    )

    clientWrapper.createIndex(indexName).asserting {
      result =>
        verify(mockClient.indices()).create(argThat { request: CreateIndexRequest =>
          request.index() == indexName
        })
        result shouldBe ()
    }
  }

  test("close should close the client successfully") {
    val mockClient    = mock[OpenSearchClient]
    val mockTransport = mock[OpenSearchTransport]

    val clientWrapper = new OpenSearchClientWrapper(mockTransport, mockClient)
    clientWrapper.close().asserting {
      result =>
        verify(mockTransport).close()
        result shouldBe ()
    }

  }

  test("execute should execute bulk requests successfully") {
    val mockClient    = mock[OpenSearchClient]
    val mockTransport = mock[OpenSearchTransport]

    val clientWrapper = new OpenSearchClientWrapper(mockTransport, mockClient)

    val requests = Seq(
      InsertRequest("index1", "id1", new TextNode("no"), "pipe"),
      UpsertRequest("index2", "id2", new TextNode("no")),
    )

    when(mockClient.bulk(any[BulkRequest])).thenReturn(
      new BulkResponse.Builder().errors(false).items(List[BulkResponseItem]().asJava).took(200L).build(),
    )

    clientWrapper.execute(requests).asserting {
      result =>
        verify(mockClient).bulk(any[BulkRequest])
        result shouldBe ()
    }

  }
}
