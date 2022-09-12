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

import com.datamountaineer.streamreactor.common.errors.RetryErrorPolicy
import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.datamountaineer.streamreactor.common.utils.ProgressCounter
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxConfig
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxConfigConstants
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxSettings
import com.datamountaineer.streamreactor.connect.influx2.writers.InfluxDbWriter
import com.datamountaineer.streamreactor.connect.influx2.writers.WriterFactoryFn
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * <h1>InfluxSinkTask</h1>
  *
  * Kafka Connect InfluxDb sink task. Called by framework to put records to the target database
  */
class InfluxSinkTask extends SinkTask with StrictLogging {

  var writer: Option[InfluxDbWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/influx-ascii.txt")).mkString + s" $version",
    )
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    InfluxConfig.config.parse(conf)
    val sinkConfig = InfluxConfig(conf)
    enableProgress = sinkConfig.getBoolean(InfluxConfigConstants.PROGRESS_COUNTER_ENABLED)
    val influxSettings = InfluxSettings(sinkConfig)

    //if error policy is retry set retry interval
    influxSettings.errorPolicy match {
      case RetryErrorPolicy() =>
        context.timeout(sinkConfig.getInt(InfluxConfigConstants.ERROR_RETRY_INTERVAL_CONFIG).toLong)
      case _ =>
    }
    writer = Some(WriterFactoryFn(influxSettings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit =
    if (records.size() == 0) {
      logger.info("Empty list of records received.")
    } else {
      require(writer.nonEmpty, "Writer is not set!")
      val seq = records.asScala.toVector
      writer.foreach(w => w.write(seq))

      if (enableProgress) {
        progressCounter.update(seq)
      }
    }

  /**
    * Clean up Influx connections
    */
  override def stop(): Unit = {
    logger.info("Stopping InfluxDb sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def version: String = manifest.version()

  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
}
