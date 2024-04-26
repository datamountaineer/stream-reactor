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
package io.lenses.streamreactor.connect.jms.sink

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.jms.config.JMSConfig
import io.lenses.streamreactor.connect.jms.config.JMSConfigConstants
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * <h1>JMSSinkConnector</h1>
  * Kafka connect Jms Sink connector
  *
  * Sets up JmsSinkTask and configurations for the tasks.
  */
class JMSSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: Option[util.Map[String, String]] = None
  private val configDef = JMSConfig.config
  private val manifest  = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    */
  override def taskClass(): Class[_ <: Task] = classOf[JMSSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList.asJava
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Jms sink task.")
    Helpers.checkInputTopics(JMSConfigConstants.KCQL, props.asScala.toMap).leftMap(throw _)
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}
