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
package io.lenses.streamreactor.connect.opensearch.config.connection

import io.lenses.streamreactor.connect.opensearch.config.AuthMode
import org.apache.kafka.connect.errors.ConnectException
import org.mockito.MockitoSugar
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

class AWSConnectionSettingsTest extends AnyFunSuite with Matchers with MockitoSugar with EitherValues {

  test("toTransport should return a valid OpenSearchTransport when using credentials") {
    val endpoint   = "test-endpoint"
    val region     = "us-east-1"
    val accessKey  = Some("access-key")
    val secretKey  = Some("secret-key")
    val authMode   = AuthMode.Credentials
    val serverless = false

    val settings = AWSConnectionSettings(endpoint, region, accessKey, secretKey, authMode, serverless)

    settings.toTransport.value.asInstanceOf[AwsSdk2Transport]

  }

  test("toTransport should return an error when using credentials but they are not provided") {
    val endpoint   = "test-endpoint"
    val region     = "us-east-1"
    val accessKey  = None
    val secretKey  = None
    val authMode   = AuthMode.Credentials
    val serverless = false

    val settings = AWSConnectionSettings(endpoint, region, accessKey, secretKey, authMode, serverless)

    val result = settings.toTransport

    result shouldBe a[Left[_, _]]
    result.left.value shouldBe a[ConnectException]
  }

  test("toTransport should return an error when an exception occurs during transport creation") {
    val endpoint   = "test-endpoint"
    val region     = ""
    val accessKey  = Some("access-key")
    val secretKey  = Some("secret-key")
    val authMode   = AuthMode.Credentials
    val serverless = false

    val settings = AWSConnectionSettings(endpoint, region, accessKey, secretKey, authMode, serverless)

    val mockCredentialsProvider = mock[AwsCredentialsProvider]

    when(mockCredentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create("access-key",
                                                                                             "secret-key",
    ))

    val result = settings.toTransport

    result shouldBe a[Left[_, _]]
    result.left.value shouldBe a[IllegalArgumentException]
  }
}
