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

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._
import io.lenses.streamreactor.connect.elastic.common.config.ElasticConfigDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

class Elastic8ConfigDef extends ElasticConfigDef("connect.elastic") {

  val PROTOCOL         = s"$connectorPrefix.protocol"
  val PROTOCOL_DOC     = "URL protocol (http, https)"
  val PROTOCOL_DEFAULT = "http"

  val HOSTS         = s"$connectorPrefix.$CONNECTION_HOSTS_SUFFIX"
  val HOSTS_DOC     = "List of hostnames for Elastic Search cluster node, not including protocol or port."
  val HOSTS_DEFAULT = "localhost"

  val ES_PORT         = s"$connectorPrefix.$CONNECTION_PORT_SUFFIX"
  val ES_PORT_DOC     = "Port on which Elastic Search node listens on"
  val ES_PORT_DEFAULT = 9300

  val ES_PREFIX         = s"$connectorPrefix.tableprefix"
  val ES_PREFIX_DOC     = "Table prefix (optional)"
  val ES_PREFIX_DEFAULT = ""

  val ES_CLUSTER_NAME         = s"$connectorPrefix.$CLUSTER_NAME_SUFFIX"
  val ES_CLUSTER_NAME_DOC     = "Name of the elastic search cluster, used in local mode for setting the connection"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"

  val CLIENT_HTTP_BASIC_AUTH_USERNAME         = s"$connectorPrefix.use.http.username"
  val CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT = ""
  val CLIENT_HTTP_BASIC_AUTH_USERNAME_DOC     = "Username if HTTP Basic Auth required default is null."
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD         = s"$connectorPrefix.use.http.password"
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT = ""
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD_DOC     = "Password if HTTP Basic Auth required default is null."

  override def configDef: ConfigDef = super.configDef
    .define(
      PROTOCOL,
      Type.STRING,
      PROTOCOL_DEFAULT,
      Importance.LOW,
      PROTOCOL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      PROTOCOL,
    )
    .define(
      HOSTS,
      Type.STRING,
      HOSTS_DEFAULT,
      Importance.HIGH,
      HOSTS_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      HOSTS,
    )
    .define(
      ES_PORT,
      Type.INT,
      ES_PORT_DEFAULT,
      Importance.HIGH,
      ES_PORT_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      HOSTS,
    )
    .define(
      ES_PREFIX,
      Type.STRING,
      ES_PREFIX_DEFAULT,
      Importance.HIGH,
      ES_PREFIX_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      HOSTS,
    )
    .define(
      ES_CLUSTER_NAME,
      Type.STRING,
      ES_CLUSTER_NAME_DEFAULT,
      Importance.HIGH,
      ES_CLUSTER_NAME_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      ES_CLUSTER_NAME,
    )
    .define(
      CLIENT_HTTP_BASIC_AUTH_USERNAME,
      Type.STRING,
      CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT,
      Importance.LOW,
      CLIENT_HTTP_BASIC_AUTH_USERNAME_DOC,
      "Connection",
      8,
      ConfigDef.Width.MEDIUM,
      CLIENT_HTTP_BASIC_AUTH_USERNAME,
    )
    .define(
      CLIENT_HTTP_BASIC_AUTH_PASSWORD,
      Type.STRING,
      CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT,
      Importance.LOW,
      CLIENT_HTTP_BASIC_AUTH_PASSWORD_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      CLIENT_HTTP_BASIC_AUTH_PASSWORD,
    )

}
