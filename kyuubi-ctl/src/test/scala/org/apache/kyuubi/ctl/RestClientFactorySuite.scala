/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.ctl

import java.io.{File, FileOutputStream}
import java.security.KeyStore

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.client.KyuubiRestClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.CtlConf._
import org.apache.kyuubi.ctl.opt.{CliConfig, CommonOpts}

class RestClientFactorySuite extends KyuubiFunSuite {

  private def instanceHostUrls(baseUrl: String, kyuubiInstance: String): Seq[String] = {
    val client = KyuubiRestClient.builder(baseUrl).build()
    try {
      var hostUrls: Seq[String] = Seq.empty
      RestClientFactory.withKyuubiInstanceRestClient(client, kyuubiInstance) { instanceClient =>
        hostUrls = instanceClient.getHostUrls.asScala.toSeq
      }
      hostUrls
    } finally {
      client.close()
    }
  }

  test("kyuubi instance uses http scheme of the rest base url") {
    assert(instanceHostUrls("http://kyuubi:10099", "kyuubi-1:10099") ===
      Seq("http://kyuubi-1:10099", "http://kyuubi:10099"))
  }

  test("kyuubi instance uses https scheme of the rest base url") {
    assert(instanceHostUrls("https://kyuubi:10099", "kyuubi-1:10099") ===
      Seq("https://kyuubi-1:10099", "https://kyuubi:10099"))
    assert(instanceHostUrls("HTTPS://kyuubi:10099", "kyuubi-1:10099").head ===
      "https://kyuubi-1:10099")
  }

  test("rest base url is used when the kyuubi instance is unknown") {
    assert(instanceHostUrls("https://kyuubi:10099", null) === Seq("https://kyuubi:10099"))
  }

  // the truststore is loaded when the rest client is built
  private def createTrustStore(password: String): File = {
    val trustStore = new File(Utils.createTempDir().toFile, "truststore.p12")
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null, null)
    val out = new FileOutputStream(trustStore)
    try {
      keyStore.store(out, password.toCharArray)
    } finally {
      out.close()
    }
    trustStore
  }

  test("truststore settings are passed to the rest client") {
    val trustStore = createTrustStore("truststore-password")
    val conf = KyuubiConf(loadSysDefault = false)
      .set(CTL_REST_CLIENT_SSL_TRUSTSTORE_PATH, trustStore.getAbsolutePath)
      .set(CTL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD, "truststore-password")
      .set(CTL_REST_CLIENT_SSL_TRUSTSTORE_TYPE, "PKCS12")
    Seq("basic", "spnego").foreach { authSchema =>
      val cliConfig = CliConfig(commonOpts =
        CommonOpts(hostUrl = "https://kyuubi:10099", authSchema = authSchema))
      RestClientFactory.withKyuubiRestClient(cliConfig, null, conf) { client =>
        assert(client.getConf.getTrustStorePath === trustStore.getAbsolutePath)
        assert(client.getConf.getTrustStorePassword === "truststore-password")
        assert(client.getConf.getTrustStoreType === "PKCS12")
      }
    }
  }

  test("truststore password is read from the credential provider") {
    val providerPath = s"jceks://file${Utils.createTempDir().toAbsolutePath}/ctl.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(
      CTL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD.key,
      "alias-password".toCharArray)
    provider.flush()

    val trustStore = createTrustStore("alias-password")
    val conf = KyuubiConf(loadSysDefault = false)
      .set(CTL_REST_CLIENT_SSL_TRUSTSTORE_PATH, trustStore.getAbsolutePath)
      .set(CTL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD, "literal-password")
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val cliConfig = CliConfig(commonOpts = CommonOpts(hostUrl = "https://kyuubi:10099"))
    RestClientFactory.withKyuubiRestClient(cliConfig, null, conf) { client =>
      assert(client.getConf.getTrustStorePassword === "alias-password")
    }
  }

  test("all the server certificates are trusted without truststore") {
    val cliConfig = CliConfig(commonOpts = CommonOpts(hostUrl = "https://kyuubi:10099"))
    val conf = KyuubiConf(loadSysDefault = false)
    RestClientFactory.withKyuubiRestClient(cliConfig, null, conf) { client =>
      assert(client.getConf.getTrustStorePath === null)
    }
  }
}
