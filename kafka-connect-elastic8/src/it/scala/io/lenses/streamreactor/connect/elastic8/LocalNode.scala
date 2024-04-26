package io.lenses.streamreactor.connect.elastic8

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticProperties
import com.sksamuel.elastic4s.http.JavaClient
import org.testcontainers.elasticsearch.ElasticsearchContainer

case class LocalNode(container: ElasticsearchContainer, client: ElasticClient) extends AutoCloseable {
  override def close(): Unit = {
    client.close()
    container.stop()
  }
}

object LocalNode {

  private val url = "docker.elastic.co/elasticsearch/elasticsearch:8.10.1"

  def apply(): LocalNode = {
    val container = new ElasticsearchContainer(url)
    container.withEnv("xpack.security.enabled", "false")
    container.start()
    LocalNode(
      container,
      createLocalNodeClient(
        container,
      ),
    )
  }

  private def createLocalNodeClient(localNode: ElasticsearchContainer): ElasticClient = {
    val esProps = ElasticProperties(s"http://${localNode.getHttpHostAddress}")
    ElasticClient(JavaClient(esProps))
  }
}
