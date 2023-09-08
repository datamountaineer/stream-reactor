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
package io.lenses.streamreactor.connect.aws.s3.sink.naming

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.SinkData
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.createFileAndParents
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.FatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.sink.config._
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorAdaptor.adaptErrorResponse
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.SinkDataExtractor

import java.io.File
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class S3KeyNamer(
  formatSelection:    FormatSelection,
  fnPad:              String => String,
  partitionSelection: PartitionSelection,
  fileNamer:          S3FileNamer,
) extends KeyNamer {

  private val DefaultPrefix = "streamreactor"

  private def prefix(bucketAndPrefix: S3Location): String = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  override def stagingFile(
    stagingDirectory: File,
    bucketAndPrefix:  S3Location,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalS3SinkError, File] =
    Try {
      val uuid = UUID.randomUUID().toString
      val file = stagingDirectory
        .toPath
        .resolve(prefix(bucketAndPrefix))
        .resolve(buildPartitionPrefix(partitionValues))
        .resolve(formatSelection.extension)
        .resolve(uuid)
        .toFile
      createFileAndParents(file)
      file
    }.toEither.left.map(ex => FatalS3SinkError(ex.getMessage, ex, topicPartition))

  private def buildPartitionPrefix(partitionValues: Map[PartitionField, String]): String =
    partitionSelection.partitions.map {
      (partition: PartitionField) =>
        partitionValues.get(partition) match {
          case Some(partVal) if partition.supportsPadding => partitionValuePrefix(partition) + fnPad(partVal)
          case Some(partVal)                              => partitionValuePrefix(partition) + partVal
          case None                                       => "[missing]"
        }
    }
      .mkString("/")

  private def partitionValuePrefix(partition: PartitionField): String =
    if (partitionSelection.partitionDisplay == KeysAndValues) s"${partition.valuePrefixDisplay()}=" else ""

  override def finalFilename(
    bucketAndPrefix:      S3Location,
    topicPartitionOffset: TopicPartitionOffset,
    partitionValues:      Map[PartitionField, String],
  ): Either[FatalS3SinkError, S3Location] =
    Try(
      bucketAndPrefix.withPath(
        s"${prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${fileNamer.fileName(fnPad, formatSelection.extension, topicPartitionOffset)}",
      ),
    ).toEither.left.map(ex => FatalS3SinkError(ex.getMessage, topicPartitionOffset.toTopicPartition))

  override def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]] =
    partitionSelection
      .partitions
      .traverse {
        case partition @ HeaderPartitionField(name) =>
          messageDetail.headers.get(name.head) match {
            case Some(value) =>
              partitionValueOrError(value, s"Header '${name.head}' is null.", topicPartition, partition)(
                getPartitionValueFromSinkData(_, name.tail),
              )
            case None =>
              FatalS3SinkError(s"Header '$name' not found in message", topicPartition).asLeft[(
                PartitionField,
                String,
              )]
          }

        case partition @ KeyPartitionField(name) =>
          partitionValueOrError(messageDetail.key, s"Key is null.", topicPartition, partition)(
            getPartitionValueFromSinkData(_, name),
          )

        case partition @ ValuePartitionField(name) =>
          partitionValueOrError(messageDetail.value, s"Value is null.", topicPartition, partition)(
            getPartitionValueFromSinkData(_, name),
          )

        case partition @ WholeKeyPartitionField() =>
          getPartitionByWholeKeyValue(messageDetail.key, topicPartition).map(partition -> _)
        case partition @ TopicPartitionField() => (partition -> topicPartition.topic.value).asRight[SinkError]
        case partition @ PartitionPartitionField() =>
          (partition -> fnPad(topicPartition.partition.toString)).asRight[SinkError]
        case partition @ DatePartitionField(_) =>
          messageDetail.timestamp match {
            case Some(value) => (partition -> partition.formatter.format(value)).asRight[SinkError]
            case None =>
              FatalS3SinkError(s"Timestamp not found in message", topicPartition).asLeft[(
                PartitionField,
                String,
              )]
          }
      }
      .map(_.toMap)

  private def partitionValueOrError(
    data:           SinkData,
    errorMsg:       String,
    topicPartition: TopicPartition,
    partition:      PartitionField,
  )(f:              SinkData => String,
  ): Either[SinkError, (PartitionField, String)] =
    data match {
      case NullSinkData(_) => FatalS3SinkError(errorMsg, topicPartition).asLeft[(PartitionField, String)]
      case other           => (partition -> f(other)).asRight
    }
  private def getPartitionByWholeKeyValue(data: SinkData, topicPartition: TopicPartition): Either[SinkError, String] =
    data match {
      case NullSinkData(_) =>
        FatalS3SinkError(s"Key is null, but requested to partition by whole key", topicPartition).asLeft[String]
      case other =>
        Try {
          getFieldStringValue(other, None).getOrElse("[missing]")
        } match {
          case Failure(_) =>
            FatalS3SinkError("Non primitive struct provided, PARTITIONBY _key requested in KCQL",
                             topicPartition,
            ).asLeft[String]
          case Success(value) => value.asRight
        }
    }

  private def getFieldStringValue(struct: SinkData, partitionName: Option[PartitionNamePath]) =
    adaptErrorResponse(SinkDataExtractor.extractPathFromSinkData(struct)(partitionName)).fold(Option.empty[String])(
      fieldVal =>
        Option(fieldVal
          .replace("/", "-")
          .replace("\\", "-")),
    )

  private def getPartitionValueFromSinkData(sinkData: SinkData, partitionName: PartitionNamePath): String =
    getFieldStringValue(sinkData, Option(partitionName)).getOrElse("[missing]")

}