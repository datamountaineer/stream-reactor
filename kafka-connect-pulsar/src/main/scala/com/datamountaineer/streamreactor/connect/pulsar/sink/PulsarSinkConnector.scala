/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.pulsar.sink

import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifest

import java.util
import io.lenses.streamreactor.connect.pulsar.config.PulsarConfigConstants
import io.lenses.streamreactor.connect.pulsar.config.PulsarSinkConfig
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class PulsarSinkConnector extends SinkConnector with StrictLogging {
  private val configDef = PulsarSinkConfig.config
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Pulsar sink connector.")
    Helpers.checkInputTopics(PulsarConfigConstants.KCQL_CONFIG, props.asScala.toMap)
    configProps = Some(props)
  }

  override def taskClass(): Class[_ <: Task] = classOf[PulsarSinkTask]

  override def version(): String = manifest.version()

  override def stop(): Unit = {}

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList.asJava
  }

  override def config(): ConfigDef = configDef
}
