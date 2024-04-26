package io.lenses.streamreactor.connect
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.MongoDBContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConfigValue
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient._
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class MongoDBTest extends AsyncFlatSpec with AsyncIOSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: MongoDBContainer = MongoDBContainer().withNetwork(network)

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "mongodb"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "MongoDB connector"

  it should "sink records" in {
    val resources = for {
      client    <- container.hostNetwork.mongoClient
      producer  <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      connector <- createConnector(sinkConfig())
    } yield (client, producer, connector)
    resources.use {
      case (client, producer, _) =>
        IO {
          // Write records to topic
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get
          producer.flush()

          // Validate
          val database = client.getDatabase("connect")
          val orders   = database.getCollection("orders")

          eventually {
            val ordersCount = orders.countDocuments
            assert(ordersCount == 1)
          }
          orders.find.iterator().next
        }
    }.asserting {
      one =>
        one.get("id", classOf[java.lang.Long]) should be(1)
        one.get("product", classOf[String]) should be("OP-DAX-P-20150201-95.7")
    }
  }

  private def sinkConfig(): ConnectorConfiguration = ConnectorConfiguration(
    "mongo-sink",
    Map[String, ConfigValue[_]](
      "connector.class"          -> ConfigValue("io.lenses.streamreactor.connect.mongodb.sink.MongoSinkConnector"),
      "tasks.max"                -> ConfigValue(1),
      "topics"                   -> ConfigValue("orders"),
      "connect.mongo.kcql"       -> ConfigValue("INSERT INTO orders SELECT * FROM orders"),
      "connect.mongo.db"         -> ConfigValue("connect"),
      "connect.mongo.connection" -> ConfigValue(s"mongodb://mongo:${container.port}"),
    ),
  )
}
