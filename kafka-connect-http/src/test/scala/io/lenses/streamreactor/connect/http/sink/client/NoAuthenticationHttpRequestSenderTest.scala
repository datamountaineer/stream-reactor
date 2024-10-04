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
package io.lenses.streamreactor.connect.http.sink.client

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s.EntityDecoder
import org.http4s.Header
import org.http4s.Method
import org.http4s.Request
import org.http4s.client.Client
import org.http4s.implicits.http4sLiteralsSyntax
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

class NoAuthenticationHttpRequestSenderTest extends AnyFunSuiteLike with Matchers {
  test("returns the same API request") {
    implicit val runtime: IORuntime = IORuntime.global
    val sinkName = "sink"
    val method   = Method.POST
    val client: Client[IO] = mock[Client[IO]]

    val requestCaptor: ArgumentCaptor[Request[IO]] = ArgumentCaptor.forClass(classOf[Request[IO]])
    when(client.expect[String](requestCaptor.capture())(any[EntityDecoder[IO, String]])).thenReturn(IO.pure("OK"))

    val sender = new NoAuthenticationHttpRequestSender(sinkName, method, client)
    val template = ProcessedTemplate(
      "http://localhost:8080",
      "content",
      List("header" -> "value"),
    )

    sender.sendHttpRequest(template).unsafeRunSync()

    // Verify the request sent
    val capturedRequest: Request[IO] = requestCaptor.getValue

    // Assert that the request is as expected
    capturedRequest.method shouldBe method
    capturedRequest.uri shouldBe uri"http://localhost:8080"
    capturedRequest.headers.headers should contain(Header.Raw(CIString("header"), "value"))
    capturedRequest.body.compile.toVector.unsafeRunSync() shouldBe template.content.getBytes.toVector
  }
}