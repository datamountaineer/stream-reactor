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

import io.lenses.streamreactor.connect.opensearch.config.CredentialPair
import io.lenses.streamreactor.connect.security.StoresInfo
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.opensearch.client.RestClient
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.transport.OpenSearchTransport
import org.opensearch.client.transport.rest_client.RestClientTransport

import scala.util.Try

case class RestConnectionSettings(
  hostnames:      Seq[String],
  protocol:       String,
  port:           Int,
  prefix:         Option[String],
  httpBasicCreds: Option[CredentialPair],
  storesInfo:     StoresInfo,
) extends OpenSeearchConnectionSettings {
  override def toTransport: Either[Throwable, OpenSearchTransport] =
    for {
      restClient <- Try(createAndConfigureRestClient()).toEither
      transport   = new RestClientTransport(restClient, new JacksonJsonpMapper())
    } yield transport

  private def hostnameToHttpHost(hostname: String): HttpHost =
    new HttpHost(prefix.map(hostname +).getOrElse(hostname), port, protocol)

  private def createAndConfigureRestClient(): RestClient = {

    val builder = RestClient.builder(
      hostnames.map(hostnameToHttpHost): _*,
    )

    val sslContext    = storesInfo.toSslContext
    val credsProvider = httpBasicCreds.map(creds => createCredsProvider(creds.username, creds.password))

    if (sslContext.nonEmpty || credsProvider.nonEmpty) {
      builder.setHttpClientConfigCallback {
        (httpClientBuilder: HttpAsyncClientBuilder) =>
          credsProvider.foreach {
            httpClientBuilder.setDefaultCredentialsProvider
          }
          sslContext.foreach {
            httpClientBuilder.setSSLContext
          }
          httpClientBuilder
      }
    }
    builder.build()
  }

  private def createCredsProvider(username: String, password: String) = {
    val provider = new BasicCredentialsProvider()
    provider.setCredentials(
      AuthScope.ANY,
      new UsernamePasswordCredentials(username, password),
    )
    provider
  }
}
