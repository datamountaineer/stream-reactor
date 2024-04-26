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
package io.lenses.streamreactor.connect.elastic8.writers

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticNodeEndpoint
import com.sksamuel.elastic4s.ElasticProperties
import io.lenses.streamreactor.connect.elastic.common.client.ElasticClientWrapper
import io.lenses.streamreactor.connect.elastic.common.writers.ElasticClientCreator
import io.lenses.streamreactor.connect.elastic8.client.Elastic8ClientWrapper
import io.lenses.streamreactor.connect.elastic8.config.Elastic8Settings
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import scala.util.Try

object Elastic8ClientCreator extends ElasticClientCreator[Elastic8Settings] {

  /**
    * Construct a JSONWriter.
    *
    * @param config An elasticSinkConfig to extract settings from.
    * @return An ElasticJsonWriter to write records from Kafka to ElasticSearch.
    */
  override def create(settings: Elastic8Settings): Either[Throwable, ElasticClientWrapper] = {
    Try {

      def endpoints(
        hostNames: Seq[String],
        protocol:  String,
        port:      Integer,
        prefix:    Option[String],
      ): Seq[ElasticNodeEndpoint] =
        hostNames
          .map(hostname => ElasticNodeEndpoint(protocol, hostname, port, prefix))

      val elasticProperties =
        ElasticProperties(endpoints(settings.hostnames, settings.protocol, settings.port, settings.prefix).toIndexedSeq)
      val javaClient = if (settings.httpBasicAuthUsername.nonEmpty && settings.httpBasicAuthPassword.nonEmpty) {
        lazy val provider = {
          val provider = new BasicCredentialsProvider
          val credentials =
            new UsernamePasswordCredentials(settings.httpBasicAuthUsername, settings.httpBasicAuthPassword)
          provider.setCredentials(AuthScope.ANY, credentials)
          provider
        }

        JavaClient(
          elasticProperties,
          (requestConfigBuilder: Builder) => requestConfigBuilder,
          (httpClientBuilder: HttpAsyncClientBuilder) => httpClientBuilder.setDefaultCredentialsProvider(provider),
        )

      } else {
        JavaClient(
          elasticProperties,
        )
      }
      new Elastic8ClientWrapper(ElasticClient(javaClient))
    }
  }.toEither
}
