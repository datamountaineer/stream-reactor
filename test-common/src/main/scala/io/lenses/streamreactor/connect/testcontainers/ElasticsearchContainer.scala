/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.testcontainers

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import org.scalatest.Assertions.fail
import org.testcontainers.elasticsearch.{ ElasticsearchContainer => JavaElasticsearchContainer }
import org.testcontainers.utility.DockerImageName

case class ElasticContainerSetup(
  key:                     String,
  imageUrl:                String,
  imageVersion:            String,
  compatibleSubstituteFor: Option[String],
  envs:                    Seq[(String, String)],
)
object ElasticsearchContainer {

  private val setup: Map[String, ElasticContainerSetup] =
    Seq(
      ElasticContainerSetup("elastic8",
                            "docker.elastic.co/elasticsearch/elasticsearch",
                            "8.10.1",
                            none,
                            Seq("xpack.security.enabled" -> "false"),
      ),
      ElasticContainerSetup("elastic8-ssl", "docker.elastic.co/elasticsearch/elasticsearch", "8.10.1", none, Seq.empty),
      ElasticContainerSetup(
        "open",
        "opensearchproject/opensearch",
        "2.10.0",
        "docker.elastic.co/elasticsearch/elasticsearch".some,
        Seq("plugins.security.disabled" -> "true"),
      ),
      ElasticContainerSetup(
        "open-ssl",
        "opensearchproject/opensearch",
        "2.10.0",
        "docker.elastic.co/elasticsearch/elasticsearch".some,
        Seq(
          "plugins.security.ssl.http.enabled"             -> "true",
          "plugins.security.ssl.http.keystore_type"       -> "jks",
          "plugins.security.ssl.http.keystore_filepath"   -> "security/keystore.jks",
          "plugins.security.ssl.http.keystore_password"   -> "changeIt",
          "plugins.security.ssl.http.truststore_type"     -> "jks",
          "plugins.security.ssl.http.truststore_filepath" -> "security/truststore.jks",
          "plugins.security.ssl.http.truststore_password" -> "changeIt",

//          "plugins.security.ssl.transport.keystore_type" -> "jks",
//          "plugins.security.ssl.transport.keystore_filepath" -> "security/keystore.jks",
//          "plugins.security.ssl.transport.keystore_password" -> "changeIt",
//          "plugins.security.ssl.transport.truststore_type" -> "jks",
//          "plugins.security.ssl.transport.truststore_filepath" -> "security/truststore.jks",
//          "plugins.security.ssl.transport.truststore_password" -> "changeIt",
        ),
      ),
    ).map(ec => ec.key -> ec).toMap
  def apply(containerKey: String): ElasticsearchContainer = {
    val version = setup.getOrElse(containerKey, fail("Container not found"))
    new ElasticsearchContainer(version)
  }

}
class ElasticsearchContainer(
  val setup: ElasticContainerSetup,
) extends SingleContainer[JavaElasticsearchContainer] {

  val port: Int = 9200

  override val container: JavaElasticsearchContainer = {
    val image = DockerImageName
      .parse(setup.imageUrl)
      .withTag(setup.imageVersion)
    val imageWithSub = setup.compatibleSubstituteFor.fold(image)(
      image.asCompatibleSubstituteFor,
    )
    new JavaElasticsearchContainer(imageWithSub)
  }

  container.withNetworkAliases(setup.key)

  setup.envs.foreach { case (k, v) => container.withEnv(k, v) }

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def httpHostAddress: String = container.getHttpHostAddress
  }

}
