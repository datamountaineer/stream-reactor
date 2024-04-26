package io.lenses.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConfigValue
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

class OpenSearchTest
    extends OpenSearchTestBase("open")
    with AsyncFlatSpecLike
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers {

  behavior of "OpenSearch connector"

  it should "sink records" in {
    runTest(
      "http",
      ConnectorConfiguration(
        "opensearch-sink",
        Map(
          "connector.class"                 -> ConfigValue("io.lenses.streamreactor.connect.opensearch.OpenSearchSinkConnector"),
          "tasks.max"                       -> ConfigValue(1),
          "topics"                          -> ConfigValue("orders"),
          "connect.opensearch.protocol"     -> ConfigValue("http"),
          "connect.opensearch.hosts"        -> ConfigValue(container.setup.key),
          "connect.opensearch.port"         -> ConfigValue(Integer.valueOf(container.port)),
          "connect.opensearch.cluster.name" -> ConfigValue(container.setup.key),
          "connect.opensearch.kcql"         -> ConfigValue("INSERT INTO orders SELECT * FROM orders AUTOCREATE"),
          "connect.progress.enabled"        -> ConfigValue(true),
        ),
      ),
    )
  }

}
