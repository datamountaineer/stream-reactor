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
package io.lenses.streamreactor.connect.cassandra.config

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager

/**
  * Created by andrew@datamountaineer.com on 19/04/16.
  * stream-reactor
  */
class TestSSLConfigContext extends AnyWordSpec with Matchers with BeforeAndAfter {
  var sslConfig:         SSLConfig = null
  var sslConfigNoClient: SSLConfig = null

  before {
    val trustStorePath     = getClass.getResource("/stc_truststore.jks").getPath
    val keystorePath       = getClass.getResource("/stc_keystore.jks").getPath
    val trustStorePassword = "erZHDS9Eo0CcNo"
    val keystorePassword   = "8yJQLUnGkwZxOw"
    sslConfig         = SSLConfig(trustStorePath, trustStorePassword, Some(keystorePath), Some(keystorePassword), true)
    sslConfigNoClient = SSLConfig(trustStorePath, trustStorePassword, Some(keystorePath), Some(keystorePassword), false)
  }

  "SSLConfigContext" should {
    "should return an Array of KeyManagers" in {
      val keyManagers = SSLConfigContext.getKeyManagers(sslConfig)
      keyManagers.length shouldBe 1
      val entry = keyManagers.head
      entry shouldBe a[KeyManager]
    }

    "should return an Array of TrustManagers" in {
      val trustManager = SSLConfigContext.getTrustManagers(sslConfig)
      trustManager.length shouldBe 1
      val entry = trustManager.head
      entry shouldBe a[TrustManager]
    }

    "should return a SSLContext" in {
      val context = SSLConfigContext(sslConfig)
      context.getProtocol shouldBe "SSL"
      context shouldBe a[SSLContext]
    }
  }
}
