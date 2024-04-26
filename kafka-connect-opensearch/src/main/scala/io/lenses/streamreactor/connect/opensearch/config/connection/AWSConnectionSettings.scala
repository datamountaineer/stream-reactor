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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.opensearch.config.AuthMode
import io.lenses.streamreactor.connect.opensearch.config.connection.OpenSeearchConnectionSettings.defaultCredentialsProvider
import org.apache.kafka.connect.errors.ConnectException
import org.opensearch.client.transport.OpenSearchTransport
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region

import scala.util.Try

case class AWSConnectionSettings(
  endpoint:   String,
  region:     String,
  accessKey:  Option[String],
  secretKey:  Option[String],
  authMode:   AuthMode,
  serverless: Boolean,
) extends OpenSeearchConnectionSettings {
  override def toTransport: Either[Throwable, OpenSearchTransport] =
    for {
      creds <- credentialsProvider()
      tOpts <- Try {
        val transportOptions = AwsSdk2TransportOptions.builder().setCredentials(creds).build()

        val httpClient = ApacheHttpClient.builder().build()
        new AwsSdk2Transport(
          httpClient,
          endpoint, // OpenSearch endpoint, without https://
          if (serverless) "aoss" else "es",
          Region.of(region),
          transportOptions,
        )
      }.toEither
    } yield tOpts

  private def credentialsProvider(): Either[Throwable, AwsCredentialsProvider] =
    (authMode, accessKey.zip(secretKey)) match {
      case (AuthMode.Credentials, Some((access, secret))) =>
        StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret)).asRight
      case (AuthMode.Credentials, None) => new ConnectException("No credentials specified").asLeft
      case (AuthMode.Default, _)        => defaultCredentialsProvider.asRight
    }
}
