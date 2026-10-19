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

package org.apache.kyuubi.server

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.Testcontainers
import org.testcontainers.containers.Container.ExecResult
import org.testcontainers.utility.MountableFile

import org.apache.kyuubi.{RestFrontendTestHelper, Utils}
import org.apache.kyuubi.RestSslTestHelper._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * Accesses the REST API and the Web UI served over https from a curl client running in a
 * Docker container, which verifies the server certificate against the exported CA.
 */
class KyuubiRestSslDockerizedClientSuite extends RestFrontendTestHelper {

  private val curlImage = sys.env.getOrElse("KYUUBI_IT_CURL_IMAGE", "curlimages/curl:8.11.1")
  private val caCertPath = "/tmp/kyuubi-ca.pem"

  private lazy val keyStore = generateKeyStore(Utils.createTempDir().toFile)

  override protected lazy val conf: KyuubiConf = sslConf(
    KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("NONE"))
      .set(FRONTEND_REST_BIND_HOST.key, "localhost"),
    keyStore)

  private var curl: GenericContainer = _

  private def restPort: Int =
    fe.connectionUrl.substring(fe.connectionUrl.lastIndexOf(':') + 1).toInt

  private def containerUrl(scheme: String, path: String): String =
    s"$scheme://$CONTAINER_HOST_ALIAS:$restPort$path"

  private def curlExec(args: String*): ExecResult =
    curl.execInContainer((Seq("curl", "-sS", "--max-time", "30") ++ args): _*)

  private def httpCode(args: String*): ExecResult =
    curlExec((Seq("-o", "/dev/null", "-w", "%{http_code}") ++ args): _*)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Testcontainers.exposeHostPorts(restPort)
    val caCert = exportCertificate(keyStore)
    curl = GenericContainer(dockerImage = curlImage, command = Seq("sleep", "infinity"))
    curl.configure { container =>
      container.withAccessToHost(true)
      // readable by the non-root user of the curl image regardless of the umask of the host
      container.withCopyFileToContainer(
        MountableFile.forHostPath(caCert.getAbsolutePath, Integer.valueOf("644", 8)),
        caCertPath)
    }
    curl.start()
  }

  override def afterAll(): Unit = {
    if (curl != null) {
      Utils.tryLogNonFatalError(curl.stop())
      curl = null
    }
    super.afterAll()
  }

  test("REST API is served over https with a certificate trusted by the client") {
    val result = curlExec("--cacert", caCertPath, containerUrl("https", "/api/v1/ping"))
    assert(result.getExitCode === 0, result.getStderr)
    assert(result.getStdout === "pong")
  }

  test("server certificate is verified by the client") {
    // curl exit code 60: peer certificate cannot be authenticated with known CA certificates
    val result = curlExec(containerUrl("https", "/api/v1/ping"))
    assert(result.getExitCode === 60, result.getStderr)
  }

  test("Web UI is served over https") {
    val redirect = curlExec(
      "--cacert",
      caCertPath,
      "-o",
      "/dev/null",
      "-w",
      "%{http_code} %{redirect_url}",
      containerUrl("https", "/"))
    assert(redirect.getExitCode === 0, redirect.getStderr)
    assert(redirect.getStdout === s"302 ${containerUrl("https", "/ui")}")

    val ui = curlExec("--cacert", caCertPath, containerUrl("https", "/ui/"))
    assert(ui.getExitCode === 0, ui.getStderr)
    assert(ui.getStdout.contains("<html"))
  }

  test("plain http is not served") {
    // the same port is reachable over https, so a failure below is not a broken port forwarding
    val https = httpCode("--cacert", caCertPath, containerUrl("https", "/api/v1/ping"))
    assert(https.getStdout === "200", https.getStderr)

    val http = httpCode(containerUrl("http", "/api/v1/ping"))
    assert(http.getStdout != "200")
  }
}
