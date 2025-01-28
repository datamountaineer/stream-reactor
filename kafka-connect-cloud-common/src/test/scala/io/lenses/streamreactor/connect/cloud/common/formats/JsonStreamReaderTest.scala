package io.lenses.streamreactor.connect.cloud.common.formats

import io.lenses.streamreactor.connect.cloud.common.formats.reader.JsonStreamReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer.JsonFormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.DEFLATE
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import org.apache.http.impl.io.EmptyInputStream
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.time.Instant

class JsonStreamReaderTest extends AnyFlatSpec with Matchers {

  "gzip compressed json records" should "be successfully read" in {
    implicit val compressionCodec: CompressionCodec = GZIP.toCodec()
    val jsonStreamReader = new JsonStreamReader(inputStreamWithCompressionCodec)

    assertRecords(jsonStreamReader)
  }

  "plain json records" should "be successfully read" in {
    implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()
    val jsonStreamReader = new JsonStreamReader(inputStreamWithCompressionCodec)

    assertRecords(jsonStreamReader)
  }

  "json reader" should "throw exception when compression codec isn't supported" in {
    implicit val compressionCodec: CompressionCodec = DEFLATE.toCodec()
    val exception = intercept[IllegalArgumentException] {
      new JsonStreamReader(EmptyInputStream.INSTANCE)
    }

    exception.getMessage should be("Invalid or missing `compressionCodec` specified.")
  }

  private def assertRecords(jsonStreamReader: JsonStreamReader): Unit =
    SampleData.recordsAsJson.take(3).foreach { expectedJson =>
      jsonStreamReader.hasNext should be(true)
      jsonStreamReader.next() should be(expectedJson)
    }

  private def inputStreamWithCompressionCodec(implicit compressionCodec: CompressionCodec) = {
    val byteArrayOutputStream = new CloudByteArrayOutputStream()
    val jsonFormatWriter      = new JsonFormatWriter(byteArrayOutputStream)
    SampleData.Users.take(3).map(createMessageDetail).foreach(jsonFormatWriter.write)
    jsonFormatWriter.complete()

    new ByteArrayInputStream(byteArrayOutputStream.toByteArray)
  }

  private def createMessageDetail(data: Struct): MessageDetail =
    MessageDetail(
      NullSinkData(None),
      StructSinkData(data),
      Map.empty,
      Some(Instant.now()),
      topic,
      0,
      Offset(0),
    )
}
