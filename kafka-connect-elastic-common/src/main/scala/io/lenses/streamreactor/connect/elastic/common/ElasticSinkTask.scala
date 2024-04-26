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
package io.lenses.streamreactor.connect.elastic.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.common.utils.ProgressCounter
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.config.ElasticConfigDef
import io.lenses.streamreactor.connect.elastic.common.config.ElasticSettings
import io.lenses.streamreactor.connect.elastic.common.config.ElasticSettingsReader
import io.lenses.streamreactor.connect.elastic.common.writers.ElasticClientCreator
import io.lenses.streamreactor.connect.elastic.common.writers.ElasticJsonWriter
import io.lenses.streamreactor.connect.elastic.common.writers.ElasticWriter
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

abstract class ElasticSinkTask[C <: ElasticSettings, CD <: ElasticConfigDef](
  configReader:  ElasticSettingsReader[C, CD],
  writerCreator: ElasticClientCreator[C],
  configDef:     CD,
  asciiArt:      String,
) extends SinkTask
    with StrictLogging
    with ErrorHandler {

  private var writer: Option[ElasticWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var writeTimeout: Option[Duration] = None

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, asciiArt)

    val conf = if (context.configs().isEmpty) props else context.configs()

    val settings: C = configReader.read(configDef, conf.asScala.toMap).leftMap(t =>
      throw new ConnectException("exception reading config", t),
    ).merge

    enableProgress = settings.common.progressCounter

    //if error policy is retry set retry interval
    settings.common.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(settings.common.errorRetryInterval)
      case _                  =>
    }

    //initialize error tracker
    initialize(settings.common.taskRetries, settings.common.errorPolicy)

    writeTimeout = settings.common.writeTimeout.seconds.some
    val elasticClientWrapper =
      writerCreator.create(settings).leftMap(t => throw new ConnectException("exception creating connection", t)).merge
    val elasticJsonWriter = new ElasticJsonWriter(elasticClientWrapper, settings.common)
    writer = Some(elasticJsonWriter)
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector

    val ioWrite   = writer.map(_.write(seq).attempt).getOrElse(IO(Right(())))
    val timeoutIo = writeTimeout.fold(ioWrite)(wT => ioWrite.timeout(wT))

    handleTry(timeoutIo.map(_.toTry).unsafeRunSync())
    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    */
  override def stop(): Unit = {
    logger.info("Stopping Elastic sink.")
    writer.foreach(_.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit =
    logger.info("Flushing Elastic Sink")

  override def version: String = manifest.version()
}
