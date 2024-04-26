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
package io.lenses.streamreactor.connect.testcontainers.scalatest

import com.typesafe.scalalogging.LazyLogging
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

import java.io.FileOutputStream
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.security.cert.X509Certificate
import java.security.interfaces.RSAPrivateKey
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.Security
import java.util.Date

object KeyStoreUtils extends LazyLogging {
  Security.addProvider(new BouncyCastleProvider())

  def createKeystore(commonName: String): Path = {

    val tmpDir: Path = Files.createTempDirectory("security")

    val (certificate, privateKey) = KeyStoreUtils.generateSelfSignedCertificate(2048, 365, commonName)
    val _                         = KeyStoreUtils.createAndSaveKeystore(tmpDir, "changeIt", certificate, privateKey)
    val _                         = KeyStoreUtils.createAndSaveTruststore(tmpDir, "changeIt", certificate)
    logger.info(s"container -> Creating keystore at $tmpDir")
    tmpDir
  }

  def generateSelfSignedCertificate(
    keySize:                 Int,
    certificateValidityDays: Int,
    commonName:              String,
  ): (X509Certificate, RSAPrivateKey) = {
    val keyPairGen = KeyPairGenerator.getInstance("RSA", "BC")
    keyPairGen.initialize(keySize)
    val keyPair = keyPairGen.generateKeyPair()

    val notBefore = new Date()
    val notAfter  = new Date(System.currentTimeMillis() + certificateValidityDays * 24L * 60 * 60 * 1000)

    val publicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic.getEncoded)

    val certBuilder = new X509v3CertificateBuilder(
      new X500Name(s"CN=$commonName"),
      BigInteger.valueOf(System.currentTimeMillis()),
      notBefore,
      notAfter,
      new X500Name(s"CN=$commonName"),
      publicKeyInfo,
    )

    val contentSigner =
      new JcaContentSignerBuilder("SHA256WithRSAEncryption").setProvider("BC").build(keyPair.getPrivate)
    val certHolder = certBuilder.build(contentSigner)
    val cert       = new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder)

    (cert, keyPair.getPrivate.asInstanceOf[RSAPrivateKey])
  }

  def createAndSaveKeystore(
    tmpDir:      Path,
    password:    String,
    certificate: X509Certificate,
    privateKey:  RSAPrivateKey,
  ): String = {

    val keyStore = KeyStore.getInstance("JKS")
    keyStore.load(null, password.toCharArray)

    // Store the private key and certificate in the keystore
    keyStore.setKeyEntry("alias", privateKey, password.toCharArray, Array(certificate))

    val keyStorePath = tmpDir.resolve("keystore.jks").toString
    // Save the keystore to a file
    val keystoreOutputStream = new FileOutputStream(keyStorePath)
    keyStore.store(keystoreOutputStream, password.toCharArray)
    keystoreOutputStream.close()

    keyStorePath
  }

  def createAndSaveTruststore(tmpDir: Path, password: String, certificate: X509Certificate): String = {

    val trustStore = KeyStore.getInstance("JKS")
    trustStore.load(null, password.toCharArray)

    // Add the trusted certificate to the truststore
    trustStore.setCertificateEntry("alias", certificate)
    val trustStorePath = tmpDir.resolve("truststore.jks").toString

    // Save the truststore to a file
    val truststoreOutputStream = new FileOutputStream(trustStorePath)
    trustStore.store(truststoreOutputStream, password.toCharArray)
    truststoreOutputStream.close()

    trustStorePath
  }

}
