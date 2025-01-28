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
package io.lenses.streamreactor.connect.cloud.common.source.reader

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.ReaderBuilderContext
import io.lenses.streamreactor.connect.cloud.common.formats.reader.CloudStreamReader
import io.lenses.streamreactor.connect.cloud.common.formats.reader.EmptyCloudStreamReader
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.SourceWatermark
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.IteratorOps
import org.apache.kafka.connect.source.SourceRecord
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

import scala.annotation.tailrec
import scala.util.Try

class ResultReader(
  reader: CloudStreamReader,
) extends LazyLogging
    with AutoCloseable {

  /**
    * Retrieves the results for a particular reader, or None if no further results are available
    */
  def retrieveResults(limit: Int): Option[Vector[SourceRecord]] = {
    val results: Vector[SourceRecord] = accumulate(limit, reader, Vector.empty[SourceRecord])
    Option.when(results.nonEmpty)(results)
  }

  @tailrec
  private final def accumulate(
    limit:              Int,
    reader:             CloudStreamReader,
    accumulatedResults: Vector[SourceRecord],
  ): Vector[SourceRecord] =
    if (limit > 0 && reader.hasNext) {
      accumulate(limit - 1, reader, accumulatedResults :+ reader.next())
    } else {
      accumulatedResults
    }

  override def close(): Unit = reader.close()

  def source: CloudLocation = reader.getBucketAndPath

  def currentRecordIndex: Long = reader.currentRecordIndex
}

object ResultReader extends LazyLogging {

  def create[SM <: FileMetadata](
    writeWatermarkToHeaders: Boolean,
    format:                  FormatSelection,
    compressionCodec:        CompressionCodec,
    targetTopic:             String,
    partitionFn:             String => Option[Int],
    connectorTaskId:         ConnectorTaskId,
    storageInterface:        StorageInterface[SM],
    hasEnvelope:             Boolean,
  ): CloudLocation => Either[Throwable, ResultReader] = { pathWithLine =>
    for {
      path   <- pathWithLine.path.toRight(new IllegalStateException("No path found"))
      exists <- storageInterface.pathExists(pathWithLine.bucket, path).leftMap(_.toException)
      result <- if (!exists) {
        logger.warn(s"[${connectorTaskId.show}] Path ${pathWithLine.show} does not exist. It will skip the object.")
        new ResultReader(new EmptyCloudStreamReader(pathWithLine)).asRight[Throwable]
      } else {
        for {
          inputStream <- storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException)
          metadata    <- storageInterface.getMetadata(pathWithLine.bucket, path).leftMap(_.toException)
          _ <- Try(logger.info(
            s"[${connectorTaskId.show}] Reading next file: ${pathWithLine.show} from line ${pathWithLine.line}",
          )).toEither

          path <- pathWithLine.path.toRight(
            new IllegalStateException(s"Invalid state reached. Missing path for cloud location:${pathWithLine.show}}"),
          )

          partition = partitionFn(path).map(Int.box).orNull
          reader <- format.toStreamReader(
            ReaderBuilderContext(
              writeWatermarkToHeaders,
              inputStream,
              pathWithLine,
              metadata,
              compressionCodec,
              hasEnvelope,
              () => storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException),
              partition,
              Topic(targetTopic),
              SourceWatermark.partition(pathWithLine),
            ),
          )
          _ <- pathWithLine.line match {
            case Some(value) if value >= 0 =>
              // value + 1 is a fix for a bug introduced by the way the source watermark is calculated.
              // The readers keep track of the current record index, which is used to calculate the watermark.
              // But the index starts at -1. So the first record is at index 0. This means until this fix the last record
              // is processed twice in case of a restart.
              // DelegateIteratorCloudStreamReader has been introduced with this change here, but it had to be compatible with the previous state.
              // The alternative would have been to add another flag in the offset watermark and to know it was the new version or not.
              // However, this introduces more complexity.

              IteratorOps.skip(reader, value + 1)
            case _ => Right(())
          }
        } yield (new ResultReader(reader))
      }
    } yield result
  }
}
