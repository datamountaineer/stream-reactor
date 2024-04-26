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

import com.github.dockerjava.api.model.Ulimit
import io.lenses.streamreactor.connect.testcontainers.KafkaVersions.ConfluentVersion
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer.defaultRestPort
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

import java.time.Duration

class KafkaConnectContainer(
  dockerImage:             String,
  networkAlias:            String         = defaultNetworkAlias,
  restPort:                Int            = defaultRestPort,
  connectPluginPath:       Option[String] = None,
  kafkaContainer:          KafkaContainer,
  schemaRegistryContainer: Option[SchemaRegistryContainer],
  providedJars:            Seq[String],
) extends GenericContainer[KafkaConnectContainer](DockerImageName.parse(dockerImage))
    with RootContainerExec {
  require(kafkaContainer != null, "You must define the kafka container")
  require(schemaRegistryContainer != null, "You must define the schema registry container")

  withNetwork(kafkaContainer.getNetwork)
  withNetworkAliases(networkAlias)
  withExposedPorts(restPort)
  waitingFor(Wait.forHttp("/connectors")
    .forPort(restPort)
    .forStatusCode(200))
    .withStartupTimeout(Duration.ofSeconds(120))
  withCreateContainerCmdModifier { cmd =>
    val _ = cmd.getHostConfig.withUlimits(Array(new Ulimit("nofile", 65536L, 65536L)))
  }
  providedJars.foreach {
    jarLocation =>
      withCopyToContainer(
        MountableFile.forHostPath(jarLocation),
        s"/usr/share/java/kafka/${jarLocation.substring(jarLocation.lastIndexOf("/"))}",
      )
  }

  connectPluginPath.foreach(f => withCopyToContainer(MountableFile.forHostPath(f), "/usr/share/plugins"))

  if (schemaRegistryContainer.isDefined) {
    withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.schemaRegistryUrl)
    withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.schemaRegistryUrl)
  } else {
    withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
    withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
  }

  withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias)
  withEnv("CONNECT_REST_PORT", restPort.toString)
  withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.bootstrapServers)
  withEnv("CONNECT_GROUP_ID", "connect")
  withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config")
  withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offset")
  withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status")
  withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/plugins")

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def restEndpointUrl: String = s"http://$getHost:${getMappedPort(restPort)}"
  }

  def installPackage(pkg: String): ExecResult =
    rootExecInContainer(container = this, commands = Seq(s"microdnf", "install", pkg))

  def copyBinds(binds: Seq[(String, String)]): Unit =
    binds.foreach {
      case (k, v) =>
        addFileSystemBind(k, v, BindMode.READ_WRITE)
    }

}
object KafkaConnectContainer {
  private val dockerImage         = DockerImageName.parse("confluentinc/cp-kafka-connect")
  private val defaultNetworkAlias = "connect"
  private val defaultRestPort     = 8083

  def apply(
    confluentPlatformVersion: String                          = ConfluentVersion,
    networkAlias:             String                          = defaultNetworkAlias,
    restPort:                 Int                             = defaultRestPort,
    connectPluginPath:        Option[String]                  = None,
    kafkaContainer:           KafkaContainer,
    schemaRegistryContainer:  Option[SchemaRegistryContainer] = None,
    providedJars:             Seq[String]                     = Seq.empty,
  ): KafkaConnectContainer =
    new KafkaConnectContainer(dockerImage.withTag(confluentPlatformVersion).toString,
                              networkAlias,
                              restPort,
                              connectPluginPath,
                              kafkaContainer,
                              schemaRegistryContainer,
                              providedJars,
    )
}
