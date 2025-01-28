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
package io.lenses.streamreactor.connect.cloud.common.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.formats.reader.converters.BytesOutputRowConverter
import io.lenses.streamreactor.connect.cloud.common.formats.reader.converters.SchemaAndValueConverter
import io.lenses.streamreactor.connect.cloud.common.formats.reader.converters.SchemaAndValueEnvelopeConverter
import io.lenses.streamreactor.connect.cloud.common.formats.reader.converters.SchemalessEnvelopeConverter
import io.lenses.streamreactor.connect.cloud.common.formats.reader._
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.BROTLI
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.BZIP2
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.DEFLATE
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZ4
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZO
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.SNAPPY
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.XZ
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.ZSTD
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.ReadTextMode
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlPropsSchema

import java.io.InputStream
import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsScala

case class ObjectMetadata(size: Long, lastModified: Instant)

case class ReaderBuilderContext(
  writeWatermarkToHeaders: Boolean,
  stream:                  InputStream,
  bucketAndPath:           CloudLocation,
  metadata:                ObjectMetadata,
  compressionCodec:        CompressionCodec,
  hasEnvelope:             Boolean,
  recreateInputStreamF:    () => Either[Throwable, InputStream],
  targetPartition:         Integer,
  targetTopic:             Topic,
  watermarkPartition:      java.util.Map[String, String],
)

sealed trait FormatSelection {
  def toStreamReader(input: ReaderBuilderContext): Either[Throwable, CloudStreamReader]

  def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Map(UNCOMPRESSED -> false)

  def extension: String

  def supportsEnvelope: Boolean
}
case object FormatSelection {

  def fromKcql(
    kcql:            Kcql,
    kcqlPropsSchema: KcqlPropsSchema[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, FormatSelection] =
    Option(kcql.getStoredAs) match {
      case Some(storedAs) =>
        fromString(storedAs, () => ReadTextMode(kcqlPropsSchema.readPropsMap(kcql.getProperties.asScala.toMap)))
      case None =>
        Right(JsonFormatSelection)
    }

  def fromString(
    formatAsString: String,
    readTextMode:   () => Option[ReadTextMode],
  ): Either[Throwable, FormatSelection] = {
    val withoutTicks = formatAsString.replace("`", "")
    val split        = withoutTicks.split("_")

    Format.withNameInsensitiveOption(split(0)) match {
      case Some(Format.Json)    => JsonFormatSelection.asRight
      case Some(Format.Avro)    => AvroFormatSelection.asRight
      case Some(Format.Parquet) => ParquetFormatSelection.asRight
      case Some(Format.Text)    => TextFormatSelection(readTextMode()).asRight
      case Some(Format.Csv)     => CsvFormatSelection(parseCsvFormatOptions(split)).asRight
      case Some(Format.Bytes) if split.size > 1 =>
        new IllegalArgumentException(
          s"Unsupported format - $formatAsString.  Please note that the following formats are no longer supported: `Bytes_KeyAndValueWithSizes`, `Bytes_KeyWithSize`, `Bytes_ValueWithSize`, `Bytes_KeyOnly`, `Bytes_ValueOnly`.  For this type of data we recommend the AVRO/Parquet/Json envelope formats. You can still store the message value as byte using the BYTES mode with no suffix.",
        ).asLeft
      case Some(Format.Bytes) => BytesFormatSelection.asRight
      case None               => new IllegalArgumentException(s"Unsupported format - $formatAsString").asLeft
    }
  }

  private def parseCsvFormatOptions(split: Array[String]): Set[FormatOptions] =
    if (split.length > 1) {
      split.splitAt(1)._2.flatMap(FormatOptions.withNameInsensitiveOption).toSet
    } else {
      Set.empty
    }
}

case object JsonFormatSelection extends FormatSelection {
  override def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Map(
    UNCOMPRESSED -> true,
    GZIP         -> true,
  )

  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {
    implicit val compressionCodec: CompressionCodec = input.compressionCodec
    val inner = new JsonStreamReader(input.stream)
    val converter = if (input.hasEnvelope) {
      new SchemalessEnvelopeConverter(input.watermarkPartition,
                                      input.targetTopic,
                                      input.targetPartition,
                                      input.bucketAndPath,
                                      input.metadata.lastModified,
      )

    } else {
      converters.TextConverter(input)
    }

    new DelegateIteratorCloudStreamReader[String](inner, converter, input.bucketAndPath).asRight
  }

  override def extension: String = "json"

  override def supportsEnvelope: Boolean = true
}

case object AvroFormatSelection extends FormatSelection {
  override def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Map(
    UNCOMPRESSED -> false,
    DEFLATE      -> true,
    BZIP2        -> false,
    SNAPPY       -> false,
    XZ           -> true,
    ZSTD         -> true,
  )

  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {

    val inner = new AvroStreamReader(input.stream)
    val converter = if (input.hasEnvelope) {
      new SchemaAndValueEnvelopeConverter(input.watermarkPartition,
                                          input.targetTopic,
                                          input.targetPartition,
                                          input.bucketAndPath,
                                          input.metadata.lastModified,
      )
    } else {
      new SchemaAndValueConverter(
        input.writeWatermarkToHeaders,
        input.watermarkPartition,
        input.targetTopic,
        input.targetPartition,
        input.bucketAndPath,
        input.metadata.lastModified,
      )
    }
    new DelegateIteratorCloudStreamReader(inner, converter, input.bucketAndPath).asRight

  }

  override def extension: String = "avro"

  override def supportsEnvelope: Boolean = true
}

case object ParquetFormatSelection extends FormatSelection {
  override def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Set(
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
  ).map(_ -> false).toMap

  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {
    val inner = ParquetStreamReader(input.metadata.size, input.recreateInputStreamF)
    val converter = if (input.hasEnvelope) {
      new SchemaAndValueEnvelopeConverter(input.watermarkPartition,
                                          input.targetTopic,
                                          input.targetPartition,
                                          input.bucketAndPath,
                                          input.metadata.lastModified,
      )
    } else {
      new SchemaAndValueConverter(
        input.writeWatermarkToHeaders,
        input.watermarkPartition,
        input.targetTopic,
        input.targetPartition,
        input.bucketAndPath,
        input.metadata.lastModified,
      )
    }
    inner.map {
      is => new DelegateIteratorCloudStreamReader(is, converter, input.bucketAndPath)
    }
  }

  override def extension: String = "parquet"

  override def supportsEnvelope: Boolean = true
}
case class TextFormatSelection(readTextMode: Option[ReadTextMode]) extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {
    val inner = TextStreamReader(
      readTextMode,
      input.stream,
    )
    val converter = converters.TextConverter(input)
    new DelegateIteratorCloudStreamReader(
      inner,
      converter,
      input.bucketAndPath,
    ).asRight
  }

  override def extension: String = "text"

  override def supportsEnvelope: Boolean = false
}
case class CsvFormatSelection(formatOptions: Set[FormatOptions]) extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {
    val inner     = new CsvStreamReader(input.stream, hasHeaders = formatOptions.contains(WithHeaders))
    val converter = converters.TextConverter(input)
    new DelegateIteratorCloudStreamReader(
      inner,
      converter,
      input.bucketAndPath,
    ).asRight
  }

  override def extension: String = "csv"

  override def supportsEnvelope: Boolean = false
}
object BytesFormatSelection extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): Either[Throwable, CloudStreamReader] = {

    val inner = new BytesStreamFileReader(input.stream, input.metadata.size)
    val converter = new BytesOutputRowConverter(input.writeWatermarkToHeaders,
                                                input.watermarkPartition,
                                                input.targetTopic,
                                                input.targetPartition,
                                                input.bucketAndPath,
                                                input.metadata.lastModified,
    )
    new DelegateIteratorCloudStreamReader(
      inner,
      converter,
      input.bucketAndPath,
    ).asRight
  }

  override def extension: String = "bytes"

  override def supportsEnvelope: Boolean = false
}
