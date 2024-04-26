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
package io.lenses.streamreactor.connect.opensearch.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettingsReader
import io.lenses.streamreactor.connect.elastic.common.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic.common.config.ElasticSettingsReader
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchTransport.Aws
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchTransport.AwsServerless
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchTransport.Http
import io.lenses.streamreactor.connect.opensearch.config.connection.AWSConnectionSettings
import io.lenses.streamreactor.connect.opensearch.config.connection.RestConnectionSettings
import io.lenses.streamreactor.connect.security.StoresInfo
import org.apache.kafka.connect.errors.ConnectException

import scala.util.Try

object OpenSearchSettingsReader extends ElasticSettingsReader[OpenSearchSettings, OpenSearchConfigDef] {
  override def read(configDef: OpenSearchConfigDef, props: Map[String, String]): Either[Throwable, OpenSearchSettings] =
    for {
      config         <- Try(ElasticConfig(configDef, configDef.connectorPrefix, props)).toEither
      commonSettings <- ElasticCommonSettingsReader.read(config.configDef, props)
      transportType = Option(config.getString(configDef.TRANSPORT)).map(_.trim).filterNot(_.isEmpty).flatMap(
        OpenSearchTransport.withNameInsensitiveOption,
      ).getOrElse(OpenSearchTransport.Http)
      hostNames = config.getString(configDef.HOSTS).split(",").toSeq

      connectionSettings <- transportType match {
        case Http =>
          createHttpConnectionSettings(configDef, config, hostNames).asRight
        case Aws | AwsServerless if hostNames.size == 1 =>
          createAwsConnectionSettings(configDef, config, transportType, hostNames).asRight
        case _ => new ConnectException("Multiple hosts not supported for AWS").asLeft
      }

    } yield {
      OpenSearchSettings(
        commonSettings,
        connectionSettings,
      )
    }

  private def createHttpConnectionSettings(
    configDef: OpenSearchConfigDef,
    config:    ElasticConfig,
    hostNames: Seq[String],
  ) = {
    val credentialPair = for {
      httpBasicAuthUsername <- Option(config.getString(configDef.CLIENT_HTTP_BASIC_AUTH_USERNAME)).filterNot(
        _.trim.isEmpty,
      )
      httpBasicAuthPassword <- Option(config.getString(configDef.CLIENT_HTTP_BASIC_AUTH_PASSWORD)).filterNot(
        _.trim.isEmpty,
      )
    } yield {
      CredentialPair(httpBasicAuthUsername, httpBasicAuthPassword)
    }

    val protocol   = config.getString(configDef.PROTOCOL)
    val port       = config.getInt(configDef.ES_PORT)
    val prefix     = Option(config.getString(configDef.ES_PREFIX)).filterNot(_ == "")
    val storesInfo = StoresInfo(config)
    RestConnectionSettings(
      hostNames,
      protocol,
      port,
      prefix,
      credentialPair,
      storesInfo,
    )
  }

  private def createAwsConnectionSettings(
    configDef:     OpenSearchConfigDef,
    config:        ElasticConfig,
    transportType: OpenSearchTransport,
    hostNames:     Seq[String],
  ) =
    AWSConnectionSettings(
      hostNames.head,
      config.getString(configDef.AWS_REGION).trim,
      Option(config.getString(configDef.AWS_ACCESS_KEY)).map(_.trim),
      Option(config.getString(configDef.AWS_SECRET_KEY)).map(_.trim),
      Option(config.getString(configDef.AUTH_MODE)).map(_.trim).flatMap(
        AuthMode.withNameInsensitiveOption,
      ).getOrElse(AuthMode.Default),
      serverless = transportType == OpenSearchTransport.AwsServerless,
    )
}
