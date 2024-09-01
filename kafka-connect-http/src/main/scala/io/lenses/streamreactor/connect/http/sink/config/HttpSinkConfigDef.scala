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
package io.lenses.streamreactor.connect.http.sink.config

import io.lenses.streamreactor.common.config.SSLConfig
import io.lenses.streamreactor.connect.http.sink.client.oauth2.OAuth2Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.http4s.Status.BadGateway
import org.http4s.Status.GatewayTimeout
import org.http4s.Status.InternalServerError
import org.http4s.Status.RequestTimeout
import org.http4s.Status.ServiceUnavailable
import org.http4s.Status.TooManyRequests

import scala.jdk.CollectionConverters._

object HttpSinkConfigDef {

  val ConnectorPrefix: String = "connect.http"
  val HttpMethodProp:  String = "connect.http.method"
  val HttpMethodDoc: String =
    """
      |The HTTP method to use.
      |""".stripMargin

  val HttpEndpointProp: String = "connect.http.endpoint"
  val HttpEndpointDoc: String =
    """
      |The HTTP endpoint to send the request to.
      |""".stripMargin

  val HttpRequestContentProp: String = "connect.http.request.content"
  val HttpRequestContentDoc: String =
    """
      |The content of the HTTP request.
      |""".stripMargin

  val HttpRequestHeadersProp: String = "connect.http.request.headers"
  val HttpRequestHeadersDoc: String =
    """
      |A list of headers to include in the HTTP request.
      |""".stripMargin

  val ConnectionTimeoutMsProp: String = "connect.http.connection.timeout.ms"
  val ConnectionTimeoutMsDoc: String =
    """
      |The HTTP connection timeout in milliseconds.
      |""".stripMargin
  val ConnectionTimeoutMsDefault = 10000
  val RetriesMaxTimeoutMsProp: String = "connect.http.retries.max.timeout.ms"
  val RetriesMaxTimeoutMsDoc: String =
    """
      |The maximum time in milliseconds to retry a request. Backoff is used to increase the time between retries, up to this maximum.
      |""".stripMargin
  val RetriesMaxTimeoutMsDefault = 30000

  val RetriesMaxRetriesProp: String = "connect.http.retries.max.retries"
  val RetriesMaxRetriesDoc: String =
    """
      |The maximum number of retries to attempt.
      |""".stripMargin
  val RetriesMaxRetriesDefault = 5

  val RetriesOnStatusCodesProp: String = "connect.http.retries.on.status.codes"
  val RetriesOnStatusCodesDoc: String =
    """
      |The status codes to retry on.
      |""".stripMargin
  val RetriesOnStatusCodesDefault: List[Int] = List(
    RequestTimeout.code,
    TooManyRequests.code,
    BadGateway.code,
    GatewayTimeout.code,
    InternalServerError.code,
    ServiceUnavailable.code,
  )

  val ErrorThresholdProp: String = "connect.http.error.threshold"
  val ErrorThresholdDoc: String =
    """
      |The number of errors to tolerate before failing the sink.
      |""".stripMargin
  val ErrorThresholdDefault = 5

  val UploadSyncPeriodProp: String = "connect.http.upload.sync.period"
  val UploadSyncPeriodDoc: String =
    """
      |The time in milliseconds to wait before sending the request.
      |""".stripMargin
  val UploadSyncPeriodDefault = 100

  val BatchCountProp: String = "connect.http.batch.count"
  val BatchCountDoc: String =
    """
      |The number of records to batch before sending the request.
      |""".stripMargin

  val BatchSizeProp: String = "connect.http.batch.size"
  val BatchSizeDoc: String =
    """
      |The size of the batch in bytes before sending the request.
      |""".stripMargin

  val TimeIntervalProp: String = "connect.http.time.interval"
  val TimeIntervalDoc: String =
    """
      |The time interval in milliseconds to wait before sending the request.
      |""".stripMargin

  val AuthenticationTypeProp: String = "connect.http.authentication.type"
  val AuthenticationTypeDoc: String =
    """
      |The type of authentication to use.
      |""".stripMargin

  val BasicAuthenticationUsernameProp: String = "connect.http.authentication.basic.username"
  val BasicAuthenticationUsernameDoc: String =
    """
      |The username for basic authentication.
      |""".stripMargin

  val BasicAuthenticationPasswordProp: String = "connect.http.authentication.basic.password"
  val BasicAuthenticationPasswordDoc: String =
    """
      |The password for basic authentication.
      |""".stripMargin

  val config: ConfigDef = OAuth2Config.append(
    new ConfigDef(),
  )
    .withClientSslSupport()
    .define(
      HttpMethodProp,
      Type.STRING,
      "POST",
      Importance.HIGH,
      HttpMethodDoc,
    )
    .define(
      HttpEndpointProp,
      Type.STRING,
      Importance.HIGH,
      HttpEndpointDoc,
    )
    .define(
      HttpRequestContentProp,
      Type.STRING,
      Importance.HIGH,
      HttpRequestContentDoc,
    )
    .define(
      HttpRequestHeadersProp,
      Type.LIST,
      List.empty.asJava,
      Importance.HIGH,
      HttpRequestHeadersDoc,
    )
    .define(
      ErrorThresholdProp,
      Type.INT,
      ErrorThresholdDefault,
      Importance.LOW,
      ErrorThresholdDoc,
    )
    .define(
      UploadSyncPeriodProp,
      Type.INT,
      UploadSyncPeriodDefault,
      Importance.LOW,
      UploadSyncPeriodDoc,
    )
    .define(
      ConnectionTimeoutMsProp,
      Type.INT,
      ConnectionTimeoutMsDefault,
      Importance.HIGH,
      ConnectionTimeoutMsDoc,
    )
    .define(
      RetriesMaxTimeoutMsProp,
      Type.INT,
      RetriesMaxTimeoutMsDefault,
      Importance.HIGH,
      RetriesMaxTimeoutMsDoc,
    )
    .define(
      RetriesMaxRetriesProp,
      Type.INT,
      RetriesMaxRetriesDefault,
      Importance.HIGH,
      RetriesMaxRetriesDoc,
    )
    .define(
      RetriesOnStatusCodesProp,
      Type.LIST,
      RetriesOnStatusCodesDefault.map(_.toString).asJava,
      Importance.HIGH,
      RetriesOnStatusCodesDoc,
    )
    .define(
      BatchCountProp,
      Type.LONG,
      1L,
      Importance.HIGH,
      BatchCountDoc,
    )
    .define(
      BatchSizeProp,
      Type.LONG,
      0L,
      Importance.HIGH,
      BatchSizeDoc,
    )
    .define(
      TimeIntervalProp,
      Type.LONG,
      0L,
      Importance.HIGH,
      TimeIntervalDoc,
    )
    .define(
      AuthenticationTypeProp,
      Type.STRING,
      "none",
      Importance.HIGH,
      AuthenticationTypeDoc,
    )
    .define(
      BasicAuthenticationUsernameProp,
      Type.STRING,
      "",
      Importance.HIGH,
      BasicAuthenticationUsernameDoc,
    )
    .define(
      BasicAuthenticationPasswordProp,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      BasicAuthenticationPasswordDoc,
    )

}
