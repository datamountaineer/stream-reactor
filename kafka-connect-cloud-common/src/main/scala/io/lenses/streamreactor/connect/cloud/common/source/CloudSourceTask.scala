/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.effect.unsafe.implicits.global
import cats.effect.FiberIO
import cats.effect.IO
import cats.effect.Ref
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSourceConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskIdCreator
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.SourceWatermark.readOffsetWatermark
import io.lenses.streamreactor.connect.cloud.common.source.distribution.CloudPartitionSearcher
import io.lenses.streamreactor.connect.cloud.common.source.distribution.PartitionSearcher
import io.lenses.streamreactor.connect.cloud.common.source.reader.PartitionDiscovery
import io.lenses.streamreactor.connect.cloud.common.source.reader.ReaderManager
import io.lenses.streamreactor.connect.cloud.common.source.reader.ReaderManagerState
import io.lenses.streamreactor.connect.cloud.common.source.state.CloudSourceTaskState
import io.lenses.streamreactor.connect.cloud.common.source.state.ReaderManagerBuilder
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryLister
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.MapUtils
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._
abstract class CloudSourceTask[MD <: FileMetadata, C <: CloudSourceConfig[MD], CT](
  sinkAsciiArtResource: String,
) extends SourceTask
    with LazyLogging
    with WithConnectorPrefix
    with JarManifestProvided {

  def validator: CloudLocationValidator

  private val contextOffsetFn: CloudLocation => Option[CloudLocation] =
    SourceContextReader.getCurrentOffset(() => context)

  @volatile
  private var cloudSourceTaskState: Option[CloudSourceTaskState] = None

  @volatile
  private var cancelledRef: Option[Ref[IO, Boolean]] = None

  private var partitionDiscoveryLoop: Option[FiberIO[Unit]] = None

  implicit var connectorTaskId: ConnectorTaskId = _

  /**
    * Start sets up readers for every configured connection in the properties
    */
  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, sinkAsciiArtResource)

    logger.debug(s"Received call to CloudSourceTask.start with ${props.size()} properties")

    val contextProperties: Map[String, String] =
      Option(context).flatMap(c => Option(c.configs()).map(_.asScala.toMap)).getOrElse(Map.empty)
    val mergedProperties: Map[String, String] = MapUtils.mergeProps(contextProperties, props.asScala.toMap)
    (for {
      result <- make(validator, connectorPrefix, mergedProperties, contextOffsetFn)
      fiber  <- result.partitionDiscoveryLoop.start
    } yield {
      cloudSourceTaskState   = result.some
      cancelledRef           = result.cancelledRef.some
      partitionDiscoveryLoop = fiber.some
    }).unsafeRunSync()
  }

  override def stop(): Unit = {
    logger.info(s"Stopping cloud source task")
    (cloudSourceTaskState, cancelledRef, partitionDiscoveryLoop) match {
      case (Some(state), Some(signal), Some(fiber)) => stopInternal(state, signal, fiber)
      case _                                        => logger.info("There is no state to stop.")
    }
    logger.info(s"Stopped cloud source task")
  }

  override def poll(): util.List[SourceRecord] =
    cloudSourceTaskState.fold(Collections.emptyList[SourceRecord]()) { state =>
      state.poll().unsafeRunSync().asJava
    }

  private def stopInternal(state: CloudSourceTaskState, signal: Ref[IO, Boolean], fiber: FiberIO[Unit]): Unit = {
    (for {
      _ <- signal.set(true)
      _ <- state.close()
      // Don't join the fiber if it's already been cancelled. It will take potentially the interval time to complete
      // and this can create issues on Connect. The task will be terminated and the resource cleaned up by the GC.
      //_ <- fiber.join.timeout(1.minute).attempt.void
    } yield ()).unsafeRunSync()
    cancelledRef           = None
    partitionDiscoveryLoop = None
    cloudSourceTaskState   = None
  }

  def createClient(config: C): Either[Throwable, CT]

  def make(
    validator:       CloudLocationValidator,
    connectorPrefix: String,
    props:           Map[String, String],
    contextOffsetFn: CloudLocation => Option[CloudLocation],
  ): IO[CloudSourceTaskState] =
    for {
      connectorTaskId <- IO.fromEither(new ConnectorTaskIdCreator(connectorPrefix).fromProps(props))
      config          <- IO.fromEither(convertPropsToConfig(connectorTaskId, props))
      client          <- IO.fromEither(createClient(config))
      storageInterface: StorageInterface[MD] <- IO.delay(createStorageInterface(connectorTaskId, config, client))

      directoryLister    <- IO.delay(createDirectoryLister(connectorTaskId, client))
      partitionSearcher  <- IO.delay(createPartitionSearcher(directoryLister, connectorTaskId, config))
      readerManagerState <- Ref[IO].of(ReaderManagerState(Seq.empty, Seq.empty))
      cancelledRef       <- Ref[IO].of(false)
    } yield {
      val readerManagerCreateFn: (CloudLocation, CloudLocation) => IO[ReaderManager] = (root, path) => {
        ReaderManagerBuilder(
          root,
          path,
          config.compressionCodec,
          storageInterface,
          connectorTaskId,
          contextOffsetFn,
          location => config.bucketOptions.find(sb => sb.sourceBucketAndPrefix == location),
          config.emptySourceBackoffSettings,
          config.writeWatermarkToHeaders,
        )(validator)
      }
      val partitionDiscoveryLoop = PartitionDiscovery.run(connectorTaskId,
                                                          config.partitionSearcher,
                                                          partitionSearcher.find,
                                                          readerManagerCreateFn,
                                                          readerManagerState,
                                                          cancelledRef,
      )
      CloudSourceTaskState(
        readerManagerState.get.map(_.readerManagers.map(rm => rm.path.toKey -> rm).toMap),
        cancelledRef,
        partitionDiscoveryLoop,
      )
    }

  def createStorageInterface(connectorTaskId: ConnectorTaskId, config: C, client: CT): StorageInterface[MD]

  def convertPropsToConfig(connectorTaskId: ConnectorTaskId, props: Map[String, String]): Either[Throwable, C]

  def createDirectoryLister(connectorTaskId: ConnectorTaskId, client: CT): DirectoryLister

  def getFilesLimit(config: C): CloudLocation => Either[Throwable, Int] = {
    cloudLocation =>
      config.bucketOptions.find(e => e.sourceBucketAndPrefix == cloudLocation).map(_.filesLimit).toRight(
        new IllegalStateException("Cannot find bucket in config to retrieve files limit"),
      )
  }

  def createPartitionSearcher(
    directoryLister: DirectoryLister,
    connectorTaskId: ConnectorTaskId,
    config:          C,
  ): PartitionSearcher =
    new CloudPartitionSearcher(
      getFilesLimit(config),
      directoryLister,
      config.bucketOptions.map(_.sourceBucketAndPrefix),
      config.partitionSearcher,
      connectorTaskId,
    )

  override def commitRecord(record: SourceRecord, metadata: RecordMetadata): Unit = {
    val _ = for {
      sourcePartition <- SourceWatermark.partitionMapToSourceRoot(
        record.sourcePartition().asScala.toMap,
      )(validator)

      offsetWatermark <- readOffsetWatermark(sourcePartition, record.sourceOffset().asScala.toMap)
    } yield {
      if (offsetWatermark.isLastLine) {
        logger.info(
          "CommitRecord - sourcePartition: {}, offsetWatermark: {}, isLastLine: {}",
          sourcePartition,
          offsetWatermark,
          offsetWatermark.isLastLine,
          cloudSourceTaskState,
        )
        cloudSourceTaskState.foreach(_.commitRecord(sourcePartition, offsetWatermark).unsafeRunSync())
      }
    }
  }
}
