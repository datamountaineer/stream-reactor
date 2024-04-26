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
package io.lenses.streamreactor.connect.testcontainers.connect

import cats.effect.IO
import cats.effect.Resource
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.ConnectorStatus
import org.scalatest.concurrent.Eventually
import org.testcontainers.shaded.org.awaitility.Awaitility.await

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.TimeUnit

class KafkaConnectClient(kafkaConnectContainer: KafkaConnectContainer) extends StrictLogging with Eventually {

  val httpClient: HttpClient = HttpClient.newHttpClient()

  def configureLogging(loggerFQCN: String, loggerLevel: String): Unit = {
    val httpPut = HttpRequest.newBuilder()
      .PUT(HttpRequest.BodyPublishers.ofString(s"""{ "level": "${loggerLevel.toUpperCase}" }"""))
      .uri(URI.create(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/admin/loggers/$loggerFQCN"))
      .header("Accept", "application/json")
      .header("Content-Type", "application/json")
      .build()

    val response = httpClient.send(httpPut, HttpResponse.BodyHandlers.ofString())
    checkRequestSuccessful(response)
  }

  def registerConnector(
    connector:      ConnectorConfiguration,
    timeoutSeconds: Long = 10L,
  ): Unit = {
    val httpPost = HttpRequest.newBuilder()
      .POST(HttpRequest.BodyPublishers.ofString(connector.toJson))
      .uri(URI.create(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors"))
      .header("Accept", "application/json")
      .header("Content-Type", "application/json")
      .build()

    val response = httpClient.send(httpPost, HttpResponse.BodyHandlers.ofString())
    checkRequestSuccessful(response)
    response.body()
    awaitConnectorConfigured(connector.name, timeoutSeconds)
  }

  def deleteConnector(connectorName: String, timeoutSeconds: Long = 10L): Unit = {
    val httpDelete = HttpRequest.newBuilder()
      .DELETE()
      .uri(URI.create(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName"))
      .header("Accept", "application/json")
      .build()

    val response = httpClient.send(httpDelete, HttpResponse.BodyHandlers.ofString())
    checkRequestSuccessful(response)
    response.body()
    awaitConnectorNotConfigured(connectorName, timeoutSeconds)
  }

  def getConnectorStatus(connectorName: String): ConnectorStatus = {
    val httpGet = HttpRequest.newBuilder()
      .GET()
      .uri(URI.create(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName/status"))
      .header("Accept", "application/json")
      .build()

    val response = httpClient.send(httpGet, HttpResponse.BodyHandlers.ofString())
    checkRequestSuccessful(response)
    parseConnectorStatus(response.body())
  }

  def isConnectorConfigured(connectorName: String): Boolean = {
    val httpGet = HttpRequest.newBuilder()
      .uri(URI.create(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName"))
      .header("Accept", "application/json")
      .GET()
      .build()

    val response = httpClient.send(httpGet, HttpResponse.BodyHandlers.ofString())
    response.body()
    response.statusCode() == 200
  }

  def awaitConnectorInRunningState(connectorName: String, timeoutSeconds: Long = 10L): Unit =
    await().atMost(timeoutSeconds, TimeUnit.SECONDS).until { () =>
      try {
        val connectorState: String =
          getConnectorStatus(connectorName).connector.state
        logger.info("Connector State: {}", connectorState)
        connectorState.equals("RUNNING")
      } catch {
        case _: Throwable => false
      }
    }

  private def checkRequestSuccessful(response: HttpResponse[String]): Unit =
    if (!isSuccess(response.statusCode())) {
      throw new IllegalStateException(s"HTTP request failed with response: ${response.body()}")
    }

  private def isSuccess(code: Int): Boolean = code / 100 == 2

  private def awaitConnectorConfigured(connectorName: String, timeoutSeconds: Long): Unit =
    await().atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => isConnectorConfigured(connectorName))

  private def awaitConnectorNotConfigured(connectorName: String, timeoutSeconds: Long): Unit =
    await().atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => !isConnectorConfigured(connectorName))

  private def parseConnectorStatus(json: String): ConnectorStatus = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(json).extract[ConnectorStatus]
  }
}

object KafkaConnectClient {
  case class ConnectorStatus(
    name:      String,
    connector: Connector,
    tasks:     Seq[Tasks],
    `type`:    String,
  )
  case class Connector(state: String, worker_id: String)
  case class Tasks(
    id:        Int,
    state:     String,
    worker_id: String,
    trace:     Option[String],
  )

  def createConnector(
    connectorConfig: ConnectorConfiguration,
    timeoutSeconds:  Long = 10L,
  )(
    implicit
    kafkaConnectClient: KafkaConnectClient,
  ): Resource[IO, String] =
    Resource.make(
      IO {
        val connectorName = connectorConfig.name
        kafkaConnectClient.registerConnector(connectorConfig)
        kafkaConnectClient.configureLogging("org.apache.kafka.connect.runtime.WorkerConfigTransformer", "DEBUG")
        kafkaConnectClient.awaitConnectorInRunningState(
          connectorName,
          timeoutSeconds,
        )
        connectorName
      },
    )(connectorName => IO(kafkaConnectClient.deleteConnector(connectorName)))

}
