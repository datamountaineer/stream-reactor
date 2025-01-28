package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.compressors.gzip.GzipParameters
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.Tables.Table
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll

import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceJsonEnvelopeTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys
    with TempFileHelper {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  private val compressionCodecs = Table(
    "codec",
    CompressionCodec(UNCOMPRESSED),
    CompressionCodec(GZIP),
  )

  private val MyPrefix  = "backups"
  private val TopicName = "myTopic"

  private val EnvelopeJson: String =
    """{"key":{"id":"1"},"value":{"id":"1","name":"John Smith","email":"js@johnsmith.com","card":"1234567890","ip":"192.168.0.2","country":"UK","currency":"GBP","timestamp":"2020-01-01T00:00:00.000Z"},"headers":{"header1":"value1","header2":123456789},"metadata":{"timestamp":1234567890,"topic":"myTopic","partition":3,"offset":0}}""".stripMargin

  override def cleanUp(): Unit = ()

  "task" should "extract from json files containing the envelope" in {
    forAll(compressionCodecs) { codec =>
      uploadFile(codec)

      val task = new S3SourceTask()

      val props = (defaultProps ++ Map(
        "connect.s3.kcql"                                   -> s"insert into $TopicName select * from $BucketName:$MyPrefix/json STOREAS `JSON` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
        "connect.s3.source.partition.search.recurse.levels" -> "0",
        "connect.s3.source.partition.search.continuous"     -> "false",
        "connect.s3.compression.codec"                      -> codec.compressionCodec.entryName,
      )).asJava

      task.start(props)

      try {
        val sourceRecords = SourceRecordsLoop.loop(task, 10.seconds.toMillis, 1).value
        task.poll() should be(empty)
        val sourceRecord = sourceRecords.head
        sourceRecord.key() should be("""{"id":"1"}""")

        sourceRecord.value() shouldBe """{"id":"1","name":"John Smith","email":"js@johnsmith.com","card":"1234567890","ip":"192.168.0.2","country":"UK","currency":"GBP","timestamp":"2020-01-01T00:00:00.000Z"}"""

        sourceRecord.headers().asScala.map(h => h.key() -> h.value()).toMap should be(Map[String, Any](
          "header1" -> "value1",
          "header2" -> 123456789L,
        ))

        sourceRecord.sourcePartition().asScala shouldBe Map("container" -> BucketName, "prefix" -> s"$MyPrefix/json/")
        val sourceOffsetMap = sourceRecord.sourceOffset().asScala
        sourceOffsetMap("path") shouldBe s"$MyPrefix/json/0"
        sourceOffsetMap("line") shouldBe "0"
        sourceOffsetMap("ts").toString.toLong < Instant.now().toEpochMilli shouldBe true

        sourceRecord.topic() shouldBe TopicName
        sourceRecord.kafkaPartition() shouldBe 3
        sourceRecord.timestamp() shouldBe 1234567890L
      } finally {
        task.stop()
      }
    }
  }

  private def uploadFile(compressionCodec: CompressionCodec) = {
    val parameters = new GzipParameters()
    parameters.setCompressionLevel(4)

    val (filename, outputStream) = compressionCodec match {
      case CompressionCodec(UNCOMPRESSED, _, _) => "00001.json" -> { file: File => new FileOutputStream(file) }
      case CompressionCodec(GZIP, _, _) => "00001.gz" -> { file: File =>
          new GzipCompressorOutputStream(new FileOutputStream(file), parameters)
        }
      case _ => throw new IllegalArgumentException("Invalid or missing `compressionCodec` specified.")
    }

    withFile(filename) { file =>
      val bw = new BufferedWriter(new OutputStreamWriter(outputStream(file)))
      bw.write(EnvelopeJson)
      bw.flush()
      bw.close()
      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/json/0")
        .leftMap(e => new UploadException(e))
    }
  }
}
