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

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.config.ElasticSettings

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class Elastic8Settings(
  elasticCommonSettings: ElasticCommonSettings,
  httpBasicAuthUsername: String,
  httpBasicAuthPassword: String,
  hostnames:             Seq[String],
  protocol:              String,
  port:                  Int,
  prefix:                Option[String] = Option.empty,
) extends ElasticSettings {
  override def common: ElasticCommonSettings = elasticCommonSettings
}
