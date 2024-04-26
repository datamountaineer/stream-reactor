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

import io.lenses.streamreactor.connect.opensearch.client.OpenSearchClientWrapper
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.mockito.Answers
import org.mockito.MockitoSugar
import org.opensearch.client.transport.OpenSearchTransport
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OpenSearchClientCreatorTest extends AnyFunSuite with Matchers with MockitoSugar with EitherValues {

  test("create should return an OpenSearchClientWrapper with a valid OpenSearchClient") {

    val mockSettings  = mock[OpenSearchSettings](Answers.RETURNS_DEEP_STUBS)
    val mockTransport = mock[OpenSearchTransport]
    when(mockSettings.connection.toTransport).thenReturn(Right(mockTransport))

    OpenSearchClientCreator.create(mockSettings).value should be(a[OpenSearchClientWrapper])
    verify(mockSettings.connection).toTransport
  }

  test("create should return an error if creating a transport fails") {
    val ex           = new RuntimeException("Transport error")
    val mockSettings = mock[OpenSearchSettings](Answers.RETURNS_DEEP_STUBS)
    when(mockSettings.connection.toTransport).thenReturn(Left(ex))

    OpenSearchClientCreator.create(mockSettings).left.value should be(ex)
    verify(mockSettings.connection).toTransport
  }
}
