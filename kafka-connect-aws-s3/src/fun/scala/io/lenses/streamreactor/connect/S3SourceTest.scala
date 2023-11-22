package io.lenses.streamreactor.connect
import _root_.io.lenses.streamreactor.connect.Configuration._
import _root_.io.lenses.streamreactor.connect.S3Utils._
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Container
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient._
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.services.s3.S3Client

class S3SourceTest
    extends AsyncFlatSpec
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers
    with LazyLogging
    with EitherValues
    with TableDrivenPropertyChecks {

  private lazy val container: S3Container = S3Container()
    .withNetwork(network)
  private val bucketName = "fancy.stuff.bucket"
  private val topicName  = "fancyStuffTopic"

  override val schemaRegistryContainer = None

  override val connectorModule: String = "aws-s3"

  def setUpData(s3Client: S3Client): IO[Unit] =
    ResourceCopier.copyResources(
      bucketName,
      s3Client,
      "/source/padded/00000000.txt",
      "padded/",
    ).recover(ex => fail(ex)) *> IO.unit

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "AWS S3 connector"

  it should "source records from s3" in {

    val resources = for {
      s3Client <- createS3ClientResource(container.identity, container.getEndpointUrl)
      _        <- createBucket(s3Client, bucketName)
      consumer <- createConsumerResource(Set(topicName))
      _ <- createConnector(
        sourceConfig("aws-s3-source",
                     container.identity,
                     container.getNetworkAliasUrl.toString,
                     bucketName,
                     "padded",
                     topicName = topicName,
        ),
      )
    } yield (s3Client, consumer)
    resources.use {
      case (s3Client, consumer) =>
        for {
          _    <- setUpData(s3Client)
          recs <- IO.delay(drain(consumer, 30000)) ///,
        } yield recs

    }.asserting { records =>
      records.size should be(30000)
      records.head.value() should be("00000000.txt:0")
      records.last.value() should be("00000005.txt:4,999")
    }

  }

}
