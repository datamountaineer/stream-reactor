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
package io.lenses.streamreactor.connect.cloud.common.source.state

import cats.effect.IO
import cats.effect.Ref
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.EmptySourceBackoffSettings
import io.lenses.streamreactor.connect.cloud.common.source.files.CloudSourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.source.files.ExponentialBackoffSourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.source.files.SystemTimeProvider
import io.lenses.streamreactor.connect.cloud.common.source.reader.ReaderManager
import io.lenses.streamreactor.connect.cloud.common.source.reader.ResultReader
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.apache.kafka.connect.errors.ConnectException

import scala.concurrent.duration.DurationLong

/**
  * Responsible for creating an instance of {{{ReaderManager}}} for a given path.
  */
object ReaderManagerBuilder extends LazyLogging {
  def apply[M <: FileMetadata](
    root:                       CloudLocation,
    path:                       CloudLocation,
    compressionCodec:           CompressionCodec,
    storageInterface:           StorageInterface[M],
    connectorTaskId:            ConnectorTaskId,
    contextOffsetFn:            CloudLocation => Option[CloudLocation],
    findSboF:                   CloudLocation => Option[CloudSourceBucketOptions[M]],
    emptySourceBackoffSettings: EmptySourceBackoffSettings,
    writeWatermarkToHeaders:    Boolean,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): IO[ReaderManager] =
    for {
      sbo <- IO.fromEither(
        findSboF(root).toRight(
          new ConnectException(s"No root found for path:$path"),
        ),
      )
      ref       <- Ref[IO].of(Option.empty[ResultReader])
      adaptedSbo = sbo.copy[M](sourceBucketAndPrefix = path)
      listingFn  = adaptedSbo.createBatchListerFn(storageInterface)
      source = contextOffsetFn(path).fold {
        logger.info(s"[${connectorTaskId.show}] No previous state for path ${path.show}")
        new CloudSourceFileQueue[M](connectorTaskId, listingFn)
      } { location: CloudLocation =>
        logger.info(s"[${connectorTaskId.show}] Resuming from ${location.toString} for path ${path.show}")
        CloudSourceFileQueue.from[M](
          listingFn,
          storageInterface,
          location,
          connectorTaskId,
          adaptedSbo.postProcessAction,
        )
      }
      sourceFileQueue <- IO.fromEither(
        sourceFileQueueWithBackoff(
          source,
          emptySourceBackoffSettings,
          connectorTaskId,
        ),
      )
    } yield new ReaderManager(
      root,
      path,
      sbo.recordsLimit,
      sourceFileQueue,
      ResultReader.create(
        writeWatermarkToHeaders,
        sbo.format,
        compressionCodec,
        sbo.targetTopic,
        sbo.getPartitionExtractorFn,
        connectorTaskId,
        storageInterface,
        sbo.hasEnvelope,
      ),
      connectorTaskId,
      ref,
      storageInterface,
      sbo.postProcessAction,
    )

  private def sourceFileQueueWithBackoff(
    sourceFileQueue:            SourceFileQueue,
    emptySourceBackoffSettings: EmptySourceBackoffSettings,
    connectorTaskId:            ConnectorTaskId,
  ): Either[IllegalArgumentException, SourceFileQueue] =
    ExponentialBackoffSourceFileQueue.apply(
      sourceFileQueue,
      emptySourceBackoffSettings.maxBackoff.millis,
      emptySourceBackoffSettings.initialDelay.millis,
      emptySourceBackoffSettings.backoffMultiplier,
      SystemTimeProvider,
      connectorTaskId,
    )
}
