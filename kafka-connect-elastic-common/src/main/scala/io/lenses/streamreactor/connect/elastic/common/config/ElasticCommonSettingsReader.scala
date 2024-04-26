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
package io.lenses.streamreactor.connect.elastic.common.config

import io.lenses.kcql.Kcql

import scala.util.Try

object ElasticCommonSettingsReader extends ElasticSettingsReader[ElasticCommonSettings, ElasticConfigDef] {
  override def read(configDef: ElasticConfigDef, props: Map[String, String]): Either[Throwable, ElasticCommonSettings] =
    for {
      config <- Try(ElasticConfig(configDef, configDef.connectorPrefix, props)).toEither

      kcql              = config.getString(configDef.KCQL).split(";").filter(_.trim.nonEmpty).map(Kcql.parse).toIndexedSeq
      pkJoinerSeparator = config.getString(configDef.PK_JOINER_SEPARATOR)
      writeTimeout      = config.getWriteTimeout
      errorPolicy       = config.getErrorPolicy
      retries           = config.getNumberRetries
      progressCounter   = config.getBoolean(configDef.PROGRESS_COUNTER_ENABLED)

      errorRetryInterval = config.getLong(configDef.ERROR_RETRY_INTERVAL).toLong
      batchSize          = config.getInt(configDef.BATCH_SIZE_CONFIG)
    } yield {
      ElasticCommonSettings(
        kcql,
        errorPolicy,
        retries,
        writeTimeout,
        batchSize,
        pkJoinerSeparator,
        progressCounter,
        errorRetryInterval,
      )
    }
}
