/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.influx2

import com.datamountaineer.streamreactor.common.config.Helpers
import com.datamountaineer.streamreactor.common.utils.JarManifest

import java.util
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxConfig
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxConfigConstants
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * <h1>InfluxSinkConnector</h1>
  * Kafka connect InfluxDb Sink connector
  *
  * Sets up InfluxSinkTask and configurations for the tasks.
  */
class InfluxSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: Option[util.Map[String, String]] = None
  private val configDef = InfluxConfig.config
  private val manifest  = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    */
  override def taskClass(): Class[_ <: Task] = classOf[InfluxSinkTask]

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
    logger.info(s"Starting InfluxDb sink task with ${props.toString}.")
    Helpers.checkInputTopics(InfluxConfigConstants.KCQL_CONFIG, props.asScala.toMap)
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}
