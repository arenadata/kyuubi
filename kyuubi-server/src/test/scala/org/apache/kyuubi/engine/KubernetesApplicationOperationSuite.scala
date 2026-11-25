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

package org.apache.kyuubi.engine

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference

import com.sun.net.httpserver.HttpServer
import io.fabric8.kubernetes.api.model.{ContainerState, ContainerStateWaiting}
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClientBuilder}

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.{FAILED, PENDING}

class KubernetesApplicationOperationSuite extends KyuubiFunSuite {

  test("Kubernetes HTTP client lists pods with the server runtime dependencies") {
    val requestedPath = new AtomicReference[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", exchange => {
      requestedPath.set(exchange.getRequestURI.getPath)
      val response = """{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[
        |{"apiVersion":"v1","kind":"Pod","metadata":{"name":"release45-driver"}}]}"""
        .stripMargin.getBytes(UTF_8)
      exchange.getResponseHeaders.set("Content-Type", "application/json")
      exchange.sendResponseHeaders(200, response.length)
      try exchange.getResponseBody.write(response) finally exchange.close()
    })
    server.start()
    val config = new ConfigBuilder()
      .withMasterUrl(s"http://127.0.0.1:${server.getAddress.getPort}")
      .withNamespace("release45")
      .withRequestTimeout(5000)
      .build()
    try {
      val client = new KubernetesClientBuilder().withConfig(config).build()
      try {
        val pods = client.pods().inNamespace("release45").list()
        assert(pods.getItems.size() == 1)
        assert(pods.getItems.get(0).getMetadata.getName == "release45-driver")
        assert(requestedPath.get() == "/api/v1/namespaces/release45/pods")
      } finally client.close()
    } finally server.stop(0)
  }

  test("test check kubernetes info") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST.key, "1,2")
    kyuubiConf.set(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST.key, "ns1,ns2")

    val operation = new KubernetesApplicationOperation()
    operation.initialize(kyuubiConf, None)

    operation.checkKubernetesInfo(KubernetesInfo(None, None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns1")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns2")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), Some("ns1")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), Some("ns2")))

    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("3"), Some("ns1")))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns3")))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("3"), None))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(None, Some("ns3")))
    }

    kyuubiConf.unset(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST.key)
    operation.checkKubernetesInfo(KubernetesInfo(Some("3"), None))
    kyuubiConf.unset(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST.key)
    operation.checkKubernetesInfo(KubernetesInfo(None, Some("ns3")))
  }

  test("build spark app url") {
    val sparkAppUrlPattern1 = "http://{{SPARK_APP_ID}}.ingress.balabala"
    val sparkAppUrlPattern2 =
      "http://{{SPARK_DRIVER_SVC}}.{{KUBERNETES_NAMESPACE}}.svc:{{SPARK_UI_PORT}}"
    val sparkAppUrlPattern3 =
      "http://{{SPARK_DRIVER_SVC}}.{{KUBERNETES_NAMESPACE}}.svc" +
        ".{{KUBERNETES_CONTEXT}}.k8s.io:{{SPARK_UI_PORT}}"
    val sparkAppUrlPattern4 = "http://{{SPARK_DRIVER_POD_IP}}:{{SPARK_UI_PORT}}"

    val sparkAppId = "spark-123"
    val sparkDriverSvc = "spark-456-driver-svc"
    val kubernetesContext = "1"
    val kubernetesNamespace = "kyuubi"
    val sparkUiPort = 4040

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern1,
      sparkAppId,
      Some(sparkDriverSvc),
      None,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) === s"http://$sparkAppId.ingress.balabala")

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern2,
      sparkAppId,
      Some(sparkDriverSvc),
      None,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) === s"http://$sparkDriverSvc.$kubernetesNamespace.svc:$sparkUiPort")

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern3,
      sparkAppId,
      Some(sparkDriverSvc),
      None,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) ===
      s"http://$sparkDriverSvc.$kubernetesNamespace.svc.$kubernetesContext.k8s.io:$sparkUiPort")

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern4,
      sparkAppId,
      None,
      Some("10.69.234.1"),
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) ===
      s"http://10.69.234.1:$sparkUiPort")
  }

  test("get kubernetes client initialization info") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(
      KyuubiConf.KUBERNETES_CLIENT_INITIALIZE_LIST.key,
      "c1:ns1,c1:ns2,c2:ns1,c2:ns2,c1:,:ns1")

    val operation = new KubernetesApplicationOperation()
    assert(operation.getKubernetesClientInitializeInfo(kyuubiConf) ===
      Array(
        KubernetesInfo(Some("c1"), Some("ns1")),
        KubernetesInfo(Some("c1"), Some("ns2")),
        KubernetesInfo(Some("c2"), Some("ns1")),
        KubernetesInfo(Some("c2"), Some("ns2")),
        KubernetesInfo(Some("c1"), None),
        KubernetesInfo(None, Some("ns1"))))
  }

  test("containerStateToApplicationState waiting reasons") {
    // Only valid pending reasons: ContainerCreating and PodInitializing
    val pendingWaitingReasons = Set("ContainerCreating", "PodInitializing")

    pendingWaitingReasons.foreach { reason =>
      val containerState = new ContainerState()
      val waiting = new ContainerStateWaiting()
      waiting.setReason(reason)
      containerState.setWaiting(waiting)

      val result = KubernetesApplicationOperation.containerStateToApplicationState(containerState)
      assert(result === PENDING)
    }
  }

  test("containerStateToApplicationState failure reasons and empty reason") {
    val failureReasons = Set(
      "ErrImagePull",
      "ImagePullBackOff",
      "CrashLoopBackOff",
      "CreateContainerConfigError")

    failureReasons.foreach { reason =>
      val containerState = new ContainerState()
      val waiting = new ContainerStateWaiting()
      waiting.setReason(reason)
      containerState.setWaiting(waiting)

      val result = KubernetesApplicationOperation.containerStateToApplicationState(containerState)
      assert(result === FAILED)
    }

    // Empty/null reason should be treated as PENDING (still initializing)
    val containerStateEmpty = new ContainerState()
    val waitingEmpty = new ContainerStateWaiting()
    waitingEmpty.setReason(null)
    containerStateEmpty.setWaiting(waitingEmpty)
    val resultEmpty =
      KubernetesApplicationOperation.containerStateToApplicationState(containerStateEmpty)
    assert(resultEmpty === PENDING)
  }
}
