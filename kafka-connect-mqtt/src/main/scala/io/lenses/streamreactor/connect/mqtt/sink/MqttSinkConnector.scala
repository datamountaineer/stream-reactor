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
package io.lenses.streamreactor.connect.mqtt.sink

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.mqtt.config.MqttConfigConstants
import io.lenses.streamreactor.connect.mqtt.config.MqttSinkConfig
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Created by andrew@datamountaineer.com on 28/08/2017.
  * stream-reactor
  */
class MqttSinkConnector extends SinkConnector with StrictLogging {
  private val configDef = MqttSinkConfig.config
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Mqtt sink connector.")
    Helpers.checkInputTopics(MqttConfigConstants.KCQL_CONFIG, props.asScala.toMap).leftMap(throw _)
    configProps = Some(props)
  }

  override def taskClass(): Class[_ <: Task] = classOf[MqttSinkTask]

  override def version(): String = manifest.version()

  override def stop(): Unit = {}

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")

    val props    = configProps.get.asScala.toMap
    val clientId = props.get(MqttConfigConstants.CLIENT_ID_CONFIG)

    def ensureUniqueClientId(idx: Int): Map[String, String] =
      maxTasks match {
        case 1 => props
        case _ => clientId.fold(props)(cId => props + (MqttConfigConstants.CLIENT_ID_CONFIG -> s"$cId-$idx"))
      }

    (1 to maxTasks).map(ensureUniqueClientId(_).asJava).toList.asJava
  }

  override def config(): ConfigDef = configDef
}
