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

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

abstract class ElasticConfigDef(val connectorPrefix: String) {

  def configDef: ConfigDef =
    new ConfigDef()
      .define(
        WRITE_TIMEOUT_CONFIG,
        Type.INT,
        WRITE_TIMEOUT_DEFAULT,
        Importance.MEDIUM,
        WRITE_TIMEOUT_DOC,
        "Connection",
        6,
        ConfigDef.Width.MEDIUM,
        WRITE_TIMEOUT_DISPLAY,
      )
      .define(
        BATCH_SIZE_CONFIG,
        Type.INT,
        BATCH_SIZE_DEFAULT,
        Importance.MEDIUM,
        BATCH_SIZE_DOC,
        "Connection",
        7,
        ConfigDef.Width.MEDIUM,
        BATCH_SIZE_DISPLAY,
      )
      .define(
        ERROR_POLICY_CONFIG,
        Type.STRING,
        ERROR_POLICY_DEFAULT,
        Importance.HIGH,
        ERROR_POLICY_DOC,
        "Error",
        1,
        ConfigDef.Width.MEDIUM,
        ERROR_POLICY_CONFIG,
      )
      .define(
        NBR_OF_RETRIES_CONFIG,
        Type.INT,
        NBR_OF_RETIRES_DEFAULT,
        Importance.MEDIUM,
        NBR_OF_RETRIES_DOC,
        "Error",
        2,
        ConfigDef.Width.SHORT,
        NBR_OF_RETRIES_CONFIG,
      )
      .define(
        ERROR_RETRY_INTERVAL,
        Type.LONG,
        ERROR_RETRY_INTERVAL_DEFAULT,
        Importance.MEDIUM,
        ERROR_RETRY_INTERVAL_DOC,
        "Error",
        3,
        ConfigDef.Width.LONG,
        ERROR_RETRY_INTERVAL,
      )
      .define(
        KCQL,
        Type.STRING,
        Importance.HIGH,
        KCQL_DOC,
        "KCQL",
        1,
        ConfigDef.Width.LONG,
        KCQL,
      )
      .define(
        PK_JOINER_SEPARATOR,
        Type.STRING,
        PK_JOINER_SEPARATOR_DEFAULT,
        Importance.LOW,
        PK_JOINER_SEPARATOR_DOC,
        "KCQL",
        2,
        ConfigDef.Width.SHORT,
        PK_JOINER_SEPARATOR,
      )
      .define(
        PROGRESS_COUNTER_ENABLED,
        Type.BOOLEAN,
        PROGRESS_COUNTER_ENABLED_DEFAULT,
        Importance.MEDIUM,
        PROGRESS_COUNTER_ENABLED_DOC,
        "Metrics",
        1,
        ConfigDef.Width.MEDIUM,
        PROGRESS_COUNTER_ENABLED_DISPLAY,
      )
      .withClientSslSupport()

  val KCQL: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY_CONFIG = s"$connectorPrefix.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val WRITE_TIMEOUT_CONFIG  = s"$connectorPrefix.$WRITE_TIMEOUT_SUFFIX"
  val WRITE_TIMEOUT_DOC     = "The time to wait in millis. Default is 5 minutes."
  val WRITE_TIMEOUT_DISPLAY = "Write timeout"
  val WRITE_TIMEOUT_DEFAULT = 300000

  val ERROR_RETRY_INTERVAL         = s"$connectorPrefix.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = 60000L

  val NBR_OF_RETRIES_CONFIG  = s"$connectorPrefix.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC     = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val BATCH_SIZE_CONFIG = s"$connectorPrefix.$BATCH_SIZE_PROP_SUFFIX"
  val BATCH_SIZE_DOC =
    "How many records to process at one time. As records are pulled from Kafka it can be 100k+ which will not be feasible to throw at Elastic search at once"
  val BATCH_SIZE_DISPLAY = "Batch size"
  val BATCH_SIZE_DEFAULT = 4000

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val PK_JOINER_SEPARATOR         = s"$connectorPrefix.pk.separator"
  val PK_JOINER_SEPARATOR_DOC     = "Separator used when have more that one field in PK"
  val PK_JOINER_SEPARATOR_DEFAULT = "-"

}
