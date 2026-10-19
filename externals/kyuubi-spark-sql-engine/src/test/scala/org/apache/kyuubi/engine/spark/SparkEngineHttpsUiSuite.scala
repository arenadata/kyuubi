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

package org.apache.kyuubi.engine.spark

import java.net.URL
import javax.net.ssl.{HttpsURLConnection, SSLContext}

import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_URL, KYUUBI_ENGINE_URL_SCHEME}

class SparkEngineHttpsUiSuite extends WithSparkSQLEngine {

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.ui.enabled" -> "true",
    "spark.ui.port" -> "0",
    // the web UI url carries the driver host, and the test certificates cover localhost
    "spark.driver.host" -> "localhost",
    "spark.ssl.ui.enabled" -> "true",
    "spark.ssl.ui.keyStore" -> goodKeyStore.toString,
    "spark.ssl.ui.keyStoreType" -> storeTypeOf(goodKeyStore),
    "spark.ssl.ui.keyStorePassword" -> passwordOf(goodKeyStore))

  test("an engine with a web UI served over HTTPS publishes the scheme apart from its url") {
    val uiWebUrl = spark.sparkContext.uiWebUrl.get
    assert(uiWebUrl.startsWith("https://"))
    val attributes = engine.frontendServices.head.attributes
    assert(attributes(KYUUBI_ENGINE_URL) === uiWebUrl.stripPrefix("https://"))
    assert(attributes(KYUUBI_ENGINE_URL_SCHEME) === "https")

    // the registered address serves the web UI over HTTPS, as the engine UI proxy expects
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustManagers(trustStore), null)
    val connection = new URL(s"https://${attributes(KYUUBI_ENGINE_URL)}/jobs/")
      .openConnection().asInstanceOf[HttpsURLConnection]
    connection.setSSLSocketFactory(sslContext.getSocketFactory)
    try assert(connection.getResponseCode === 200)
    finally connection.disconnect()
  }
}
