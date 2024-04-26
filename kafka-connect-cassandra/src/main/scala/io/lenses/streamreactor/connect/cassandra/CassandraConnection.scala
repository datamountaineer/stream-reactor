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
package io.lenses.streamreactor.connect.cassandra

import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigConstants
import io.lenses.streamreactor.connect.cassandra.config.SSLConfig
import io.lenses.streamreactor.connect.cassandra.config.SSLConfigContext
import io.lenses.streamreactor.connect.cassandra.config.LoadBalancingPolicy
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.LatencyAwarePolicy
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import com.datastax.driver.core._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.AbstractConfig

/**
  * Set up a Casssandra connection
  */

object CassandraConnection extends StrictLogging {
  def apply(connectorConfig: AbstractConfig): CassandraConnection = {
    val cluster  = getCluster(connectorConfig)
    val keySpace = connectorConfig.getString(CassandraConfigConstants.KEY_SPACE)
    val session  = getSession(keySpace, cluster)
    new CassandraConnection(cluster = cluster, session = session)
  }

  def getCluster(connectorConfig: AbstractConfig): Cluster = {
    val contactPoints: String = connectorConfig.getString(CassandraConfigConstants.CONTACT_POINTS)
    val port      = connectorConfig.getInt(CassandraConfigConstants.PORT)
    val fetchSize = connectorConfig.getInt(CassandraConfigConstants.FETCH_SIZE)

    val connectTimeout = connectorConfig.getInt(CassandraConfigConstants.CONNECT_TIMEOUT)
    val readTimeout    = connectorConfig.getInt(CassandraConfigConstants.READ_TIMEOUT)

    val loadBalancer = LoadBalancingPolicy.withName(
      connectorConfig.getString(CassandraConfigConstants.LOAD_BALANCING_POLICY).toUpperCase,
    ) match {
      case LoadBalancingPolicy.TOKEN_AWARE =>
        new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())

      case LoadBalancingPolicy.ROUND_ROBIN =>
        new RoundRobinPolicy()

      case LoadBalancingPolicy.DC_AWARE_ROUND_ROBIN =>
        DCAwareRoundRobinPolicy.builder().build()

      case LoadBalancingPolicy.LATENCY_AWARE =>
        LatencyAwarePolicy.builder(DCAwareRoundRobinPolicy.builder().build()).build()
    }

    val builder: Builder = Cluster
      .builder()
      .withoutJMXReporting()
      .addContactPoints(contactPoints.split(","): _*)
      .withPort(port)
      .withSocketOptions(new SocketOptions()
        .setConnectTimeoutMillis(connectTimeout)
        .setReadTimeoutMillis(readTimeout))
      .withLoadBalancingPolicy(loadBalancer)
      .withQueryOptions(new QueryOptions().setFetchSize(fetchSize))

    //get authentication mode, only support NONE and USERNAME_PASSWORD for now
    addAuthMode(connectorConfig, builder)

    //is ssl enable
    addSSL(connectorConfig, builder)
    builder.build()
  }

  /**
    * Get a Cassandra session
    *
    * @param keySpace A configuration to build the setting from
    * @param cluster  The cluster the get the session for
    */
  def getSession(keySpace: String, cluster: Cluster): Session =
    cluster.connect(keySpace)

  /**
    * Add authentication to the connection builder
    *
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder         The builder to add the authentication to
    * @return The builder with authentication added.
    */
  private def addAuthMode(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val username = connectorConfig.getString(CassandraConfigConstants.USERNAME)
    val password = connectorConfig.getPassword(CassandraConfigConstants.PASSWD).value

    if (username.length > 0 && password.length > 0) {
      builder.withCredentials(username.trim, password.toString.trim)
    } else {
      logger.warn("Username and password not set.")
    }
    builder
  }

  /**
    * Add SSL connection options to the connection builder
    *
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder         The builder to add the authentication to
    * @return The builder with SSL added.
    */
  private def addSSL(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val ssl = connectorConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED).asInstanceOf[Boolean]
    if (ssl) {
      logger.info("Setting up SSL context.")
      val sslConfig = SSLConfig(
        trustStorePath = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_PATH),
        trustStorePass = connectorConfig.getPassword(CassandraConfigConstants.TRUST_STORE_PASSWD).value,
        keyStorePath   = Some(connectorConfig.getString(CassandraConfigConstants.KEY_STORE_PATH)),
        keyStorePass   = Some(connectorConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value),
        useClientCert  = connectorConfig.getBoolean(CassandraConfigConstants.USE_CLIENT_AUTH),
        keyStoreType   = connectorConfig.getString(CassandraConfigConstants.KEY_STORE_TYPE),
        trustStoreType = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_TYPE),
      )

      val context = SSLConfigContext(sslConfig)
      //val cipherSuites: Array[String] = Array("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
      val sSLOptions = new RemoteEndpointAwareJdkSSLOptions.Builder
      //sSLOptions.withCipherSuites(cipherSuites)
      sSLOptions.withSSLContext(context)
      builder.withSSL(sSLOptions.build())
    } else {
      builder
    }
  }
}

/**
  * <h1>CassandraConnection</h1>
  *
  * Case class to hold a Cassandra cluster and session connection
  */
case class CassandraConnection(cluster: Cluster, session: Session)
