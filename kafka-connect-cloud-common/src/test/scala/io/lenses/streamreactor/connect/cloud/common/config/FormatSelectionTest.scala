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

import io.lenses.streamreactor.connect.cloud.common.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStreamWriter
import java.time.Instant

class FormatSelectionTest extends AnyFlatSpec with Matchers with EitherValues with MockitoSugar {

  "formatSelection.fromString" should "format for CSV with headers" in {
    FormatSelection.fromString("`CSV_WITHHEADERS`", () => Option.empty) should be(
      Right(CsvFormatSelection(Set(WithHeaders))),
    )
  }

  "formatSelection.fromString" should "format for CSV without headers" in {
    FormatSelection.fromString("`CSV`", () => Option.empty) should be(Right(CsvFormatSelection(Set.empty)))
  }

  "formatSelection.fromString" should "return error for deprecated format selections" in {
    FormatSelection.fromString("BYTES_VALUEONLY", () => Option.empty).left.value.getMessage should startWith(
      "Unsupported format - BYTES_VALUEONLY.  Please note",
    )
  }

  "formatSelection.toStreamReader" should "create JsonStreamReader with appropriate compression codec" in {
    JsonFormatSelection.availableCompressionCodecs.keys.foreach { codecName =>
      implicit val compressionCodec: CompressionCodec = codecName.toCodec()

      val jsonString    = SampleData.recordsAsJson.head
      val inputStream   = getInputStream(jsonString)
      val readerContext = mockReaderContext(inputStream)

      val streamReader = JsonFormatSelection.toStreamReader(readerContext).value

      streamReader.hasNext should be(true)
      streamReader.next().value should be(jsonString)
    }
  }

  private def mockReaderContext(inputStream: InputStream)(implicit codec: CompressionCodec): ReaderBuilderContext = {
    val topic         = Topic("topic")
    val metadata      = mock[ObjectMetadata]
    val cloudLocation = mock[CloudLocation]
    val readerContext = mock[ReaderBuilderContext]

    when(readerContext.writeWatermarkToHeaders).thenReturn(false)
    when(readerContext.compressionCodec).thenReturn(codec)
    when(readerContext.targetPartition).thenReturn(1)
    when(readerContext.bucketAndPath).thenReturn(cloudLocation)
    when(readerContext.hasEnvelope).thenReturn(false)
    when(readerContext.targetTopic).thenReturn(topic)
    when(readerContext.metadata).thenReturn(metadata)
    when(readerContext.stream).thenReturn(inputStream)
    when(metadata.lastModified).thenReturn(Instant.now())

    readerContext
  }

  private def getInputStream(recordJsonData: String)(implicit codec: CompressionCodec): InputStream = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = codec match {
      case CompressionCodec(GZIP, _, _)         => new GzipCompressorOutputStream(byteArrayOutputStream)
      case CompressionCodec(UNCOMPRESSED, _, _) => byteArrayOutputStream
      case _                                    => throw new IllegalArgumentException()
    }
    val writer = new OutputStreamWriter(outputStream)
    writer.write(recordJsonData)
    writer.flush()
    writer.close()
    new ByteArrayInputStream(byteArrayOutputStream.toByteArray)
  }
}
