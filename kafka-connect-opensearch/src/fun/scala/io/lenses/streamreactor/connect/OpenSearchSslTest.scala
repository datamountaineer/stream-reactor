package io.lenses.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConfigValue
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import cats.effect.testing.scalatest.AsyncIOSpec
import org.apache.kafka.common.config.SslConfigs
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

class OpenSearchSslTest
    extends OpenSearchTestBase("open-ssl")
    with AsyncFlatSpecLike
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers {

  behavior of "OpenSearch connector with SSL"

  it should "sink records with ssl enabled" ignore {

    runTest(
      "https",
      ConnectorConfiguration(
        "opensearch-sink-ssl",
        Map(
          "connector.class"                         -> ConfigValue("io.lenses.streamreactor.connect.opensearch.OpenSearchSinkConnector"),
          "tasks.max"                               -> ConfigValue(1),
          "topics"                                  -> ConfigValue("orders"),
          "connect.opensearch.use.http.username"    -> ConfigValue("admin"),
          "connect.opensearch.use.http.password"    -> ConfigValue("admin"),
          "connect.opensearch.protocol"             -> ConfigValue("https"),
          "connect.opensearch.hosts"                -> ConfigValue(container.setup.key),
          "connect.opensearch.port"                 -> ConfigValue(Integer.valueOf(container.port)),
          "connect.opensearch.cluster.name"         -> ConfigValue(container.setup.key),
          "connect.opensearch.kcql"                 -> ConfigValue("INSERT INTO orders SELECT * FROM orders AUTOCREATE"),
          "connect.progress.enabled"                -> ConfigValue(true),
          SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG     -> ConfigValue("JKS"),
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> ConfigValue("/security/truststore.jks"),
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> ConfigValue("changeIt"),
          SslConfigs.SSL_KEYSTORE_TYPE_CONFIG       -> ConfigValue("JKS"),
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG   -> ConfigValue("/security/keystore.jks"),
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG   -> ConfigValue("changeIt"),
        ),
      ),
    )
  }

}
