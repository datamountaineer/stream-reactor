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
package io.lenses.streamreactor.connect.cassandra.sink

import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigConstants
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigSink
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Try

/**
  * <h1>CassandraSinkConnector</h1>
  * Kafka connect Cassandra Sink connector
  *
  * Sets up CassandraSinkTask and configurations for the tasks.
  */
class CassandraSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: util.Map[String, String] = _
  private val configDef = CassandraConfigSink.sinkConfig
  private val manifest  = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    */
  override def taskClass(): Class[_ <: Task] = classOf[CassandraSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps).toList.asJava
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    */
  override def start(props: util.Map[String, String]): Unit = {
    //check input topics
    Helpers.checkInputTopics(CassandraConfigConstants.KCQL, props.asScala.toMap).leftMap(throw _)
    configProps = props
    Try(new CassandraConfigSink(props.asScala.toMap)) match {
      case Failure(f) =>
        throw new ConnectException(s"Couldn't start Cassandra sink due to configuration error: ${f.getMessage}", f)
      case _ =>
    }
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}
