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
package io.lenses.streamreactor.connect.elastic8.config

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettingsReader
import io.lenses.streamreactor.connect.elastic.common.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic.common.config.ElasticSettingsReader

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Elastic8SettingsReader extends ElasticSettingsReader[Elastic8Settings, Elastic8ConfigDef] {
  override def read(configDef: Elastic8ConfigDef, props: Map[String, String]): Either[Throwable, Elastic8Settings] =
    for {
      config               <- Try(ElasticConfig(configDef, configDef.connectorPrefix, props)).toEither
      commonSettings       <- ElasticCommonSettingsReader.read(config.configDef, props)
      httpBasicAuthUsername = config.getString(configDef.CLIENT_HTTP_BASIC_AUTH_USERNAME)
      httpBasicAuthPassword = config.getString(configDef.CLIENT_HTTP_BASIC_AUTH_PASSWORD)
      hostNames             = config.getString(configDef.HOSTS).split(",").toSeq
      protocol              = config.getString(configDef.PROTOCOL)
      port                  = config.getInt(configDef.ES_PORT)
      prefix = Try(config.getString(configDef.ES_PREFIX)) match {
        case Success("")           => None
        case Success(configString) => Some(configString)
        case Failure(_)            => None
      }
    } yield {
      Elastic8Settings(
        commonSettings,
        httpBasicAuthUsername,
        httpBasicAuthPassword,
        hostNames,
        protocol,
        port,
        prefix,
      )
    }

}
