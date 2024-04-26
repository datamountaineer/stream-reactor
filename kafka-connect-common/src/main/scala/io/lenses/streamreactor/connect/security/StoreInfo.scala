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
package io.lenses.streamreactor.connect.security

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import org.apache.kafka.common.config.SslConfigs

import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

case class StoreInfo(
  storePath:     String,
  storeType:     Option[String],
  storePassword: Option[String] = None,
)

case class StoresInfo(
  trustStore: Option[StoreInfo] = None,
  keyStore:   Option[StoreInfo] = None,
) {
  def toSslContext: Option[SSLContext] = {
    val maybeTrustFactory: Option[TrustManagerFactory] = trustStore.map {
      case StoreInfo(path, storeType, password) =>
        trustManagers(path, storeType, password)
    }
    val maybeKeyFactory: Option[KeyManagerFactory] = keyStore.map {
      case StoreInfo(path, storeType, password) =>
        keyManagers(path, storeType, password)
    }

    if (maybeTrustFactory.nonEmpty || maybeKeyFactory.nonEmpty) {
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(
        maybeKeyFactory.map(_.getKeyManagers).orNull,
        maybeTrustFactory.map(_.getTrustManagers).orNull,
        null,
      )
      sslContext.some
    } else {
      none
    }
  }

  private def trustManagers(path: String, storeType: Option[String], password: Option[String]) = {
    val truststore       = KeyStore.getInstance(storeType.map(_.toUpperCase).getOrElse("JKS"))
    val truststoreStream = new FileInputStream(path)
    truststore.load(truststoreStream, password.getOrElse("").toCharArray)

    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(truststore)
    trustManagerFactory
  }

  private def keyManagers(path: String, storeType: Option[String], password: Option[String]): KeyManagerFactory = {
    val keyStore         = KeyStore.getInstance(storeType.map(_.toUpperCase).getOrElse("JKS"))
    val truststoreStream = new FileInputStream(path)
    keyStore.load(truststoreStream, password.getOrElse("").toCharArray)

    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password.getOrElse("").toCharArray)
    keyManagerFactory
  }
}

object StoresInfo {
  def apply(config: BaseConfig): StoresInfo = {
    val trustStore = for {
      storePath    <- Option(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
      storeType     = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG))
      storePassword = Option(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).map(_.value())
    } yield StoreInfo(storePath, storeType, storePassword)
    val keyStore = for {
      storePath    <- Option(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      storeType     = Option(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG))
      storePassword = Option(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).map(_.value())
    } yield StoreInfo(storePath, storeType, storePassword)

    StoresInfo(trustStore, keyStore)
  }
}
