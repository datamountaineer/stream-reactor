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
package io.lenses.streamreactor.connect.aws.s3.source.config

import cats.implicits.catsSyntaxOptionId
import cats.implicits.toTraverseOps
import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3Config.getString
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_ORDERING_TYPE
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_REGEX
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_TYPE
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.source.config.kcqlprops.S3SourcePropsSchema

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

object S3SourceConfig {

  def fromProps(
    props: util.Map[String, String],
  ): Either[Throwable, S3SourceConfig] =
    S3SourceConfig(S3SourceConfigDefBuilder(props))

  def apply(s3ConfigDefBuilder: S3SourceConfigDefBuilder): Either[Throwable, S3SourceConfig] = {
    val parsedValues = s3ConfigDefBuilder.getParsedValues
    for {
      sbo <- SourceBucketOptions(
        s3ConfigDefBuilder,
        PartitionExtractor(
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_TYPE).getOrElse("none"),
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_REGEX),
        ),
      )
    } yield S3SourceConfig(
      S3Config(parsedValues),
      sbo,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.getPartitionSearcherOptions(parsedValues),
      s3ConfigDefBuilder.batchDelete(),
    )

  }
}

case class S3SourceConfig(
  s3Config:          S3Config,
  bucketOptions:     Seq[SourceBucketOptions] = Seq.empty,
  compressionCodec:  CompressionCodec,
  partitionSearcher: PartitionSearcherOptions,
  batchDelete:       Boolean,
)

case class SourceBucketOptions(
  sourceBucketAndPrefix: CloudLocation,
  targetTopic:           String,
  format:                FormatSelection,
  recordsLimit:          Int,
  filesLimit:            Int,
  partitionExtractor:    Option[PartitionExtractor],
  orderingType:          OrderingType,
  hasEnvelope:           Boolean,
) {
  def createBatchListerFn(
    storageInterface: StorageInterface,
  ): Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]] =
    orderingType
      .getBatchLister
      .listBatch(
        storageInterface = storageInterface,
        bucket           = sourceBucketAndPrefix.bucket,
        prefix           = sourceBucketAndPrefix.prefix,
        numResults       = filesLimit,
      )

  def getPartitionExtractorFn: String => Option[Int] =
    partitionExtractor.fold((_: String) => Option.empty[Int])(_.extract)

}

object SourceBucketOptions {
  private val DEFAULT_RECORDS_LIMIT = 10000
  private val DEFAULT_FILES_LIMIT   = 1000

  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  def apply(
    config:             S3SourceConfigDefBuilder,
    partitionExtractor: Option[PartitionExtractor],
  ): Either[Throwable, Seq[SourceBucketOptions]] =
    config.getKCQL.map {
      kcql: Kcql =>
        for {
          source <- CloudLocation.splitAndValidate(kcql.getSource, allowSlash = true)
          format <- FormatSelection.fromKcql(kcql, S3SourcePropsSchema.schema)
          //extract the envelope. of not present default to false
          hasEnvelope <- extractEnvelope(Option(kcql.getProperties).map(_.asScala.toMap).getOrElse(Map.empty))

        } yield SourceBucketOptions(
          source,
          kcql.getTarget,
          format             = format,
          recordsLimit       = if (kcql.getLimit < 1) DEFAULT_RECORDS_LIMIT else kcql.getLimit,
          filesLimit         = if (kcql.getBatchSize < 1) DEFAULT_FILES_LIMIT else kcql.getBatchSize,
          partitionExtractor = partitionExtractor,
          orderingType = Try(config.getString(SOURCE_ORDERING_TYPE)).toOption.flatMap(
            OrderingType.withNameInsensitiveOption,
          ).getOrElse(OrderingType.AlphaNumeric),
          hasEnvelope = hasEnvelope.getOrElse(false),
        )
    }.toSeq.traverse(identity)

  def extractEnvelope(properties: Map[String, String]): Either[Throwable, Option[Boolean]] = {
    val envelope = properties.get(DataStorageSettings.StoreEnvelopeKey)
    envelope match {
      case Some(value) =>
        Try(value.toBoolean.some).toEither.left.map(_ =>
          new IllegalArgumentException(s"Invalid value for $S3Config.ENVELOPE_ENABLED. Must be a boolean"),
        )
      case None => Right(None)
    }
  }
}
