package io.lenses.streamreactor.connect

import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import _root_.io.lenses.streamreactor.connect.testcontainers.ElasticsearchContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxOptionId
import com.jayway.jsonpath.JsonPath
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager
import scala.concurrent.Future

abstract class OpenSearchTestBase(containerKey: String)
    extends AsyncFlatSpecLike
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers {

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override def connectorModule: String = "opensearch"

  override def useKeyStore: Boolean = true

  val container: ElasticsearchContainer = ElasticsearchContainer(containerKey).withNetwork(network)

  override val commonName: Option[String] = container.setup.key.some

  override def beforeAll(): Unit = {
    copyBinds(container.container, "/usr/share/opensearch/config/security/")
    container.start()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  protected def runTest(protocol: String, configuration: ConnectorConfiguration): Future[Assertion] = {
    val resources = for {
      producer  <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      connector <- createConnector(configuration, 60L)
    } yield (producer, connector)

    resources.use {
      case (producer, _) =>
        IO {
          // Write records to topic
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get()
          producer.flush()

          val client = HttpClient.newBuilder().sslContext(createTrustAllCertsSslContext).build()
          val request = HttpRequest.newBuilder()
            .GET().uri(
              new URI(
                s"$protocol://${container.hostNetwork.httpHostAddress}/orders/_search/?q=OP-DAX-P-20150201",
              ),
            ).build()

          eventually {
            val response = client.send(request, BodyHandlers.ofString())
            val body     = response.body
            assert(JsonPath.read[Int](body, "$.hits.total.value") == 1)
          }

          client.send(request, BodyHandlers.ofString())
        }.asserting {
          response =>
            val body = response.body
            JsonPath.read[Int](body, "$.hits.hits[0]._source.id") should be(1)
            JsonPath.read[String](body, "$.hits.hits[0]._source.product") should be("OP-DAX-P-20150201-95.7")
            JsonPath.read[Double](body, "$.hits.hits[0]._source.price") should be(94.2)
            JsonPath.read[Int](body, "$.hits.hits[0]._source.qty") should be(100)
        }
    }
  }

  private def createTrustAllCertsSslContext = {
    val trustAllCerts = new X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()

      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()

      override def getAcceptedIssuers: Array[X509Certificate] = null
    }
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, Array(trustAllCerts), new SecureRandom())
    sslContext
  }
}
