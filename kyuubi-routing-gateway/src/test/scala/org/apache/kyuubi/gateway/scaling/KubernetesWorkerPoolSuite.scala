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

package org.apache.kyuubi.gateway.scaling

import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, IntOrString, ServicePort, ServicePortBuilder}

import org.apache.kyuubi.KyuubiFunSuite

/**
 * Where a pool's workers are reached, as the Service governing them says - the
 * coordinator in front of them may listen somewhere else entirely.
 */
class KubernetesWorkerPoolSuite extends KyuubiFunSuite {

  private def port(
      name: String,
      number: Int,
      target: IntOrString = null,
      appProtocol: String = null): ServicePort =
    new ServicePortBuilder()
      .withName(name)
      .withPort(number)
      .withTargetPort(Option(target).getOrElse(new IntOrString(number)))
      .withAppProtocol(appProtocol)
      .build()

  private def containerPort(name: String, number: Int): ContainerPort =
    new ContainerPortBuilder().withName(name).withContainerPort(number).build()

  private def listening(
      ports: Seq[ServicePort],
      coordinator: (String, Int),
      containers: Seq[ContainerPort] = Seq.empty): (String, Int) =
    KubernetesWorkerPool.listening(ports, containers, coordinator._1, coordinator._2)

  test("one plain port elsewhere than the coordinator's is reached over HTTP") {
    // The Kerberos stand: coordinator on HTTPS 8443, workers on HTTP 8080.
    assert(listening(Seq(port("worker-headless", 8080)), ("https", 8443)) === ("http", 8080))
  }

  test("workers listening where the coordinator does are taken to speak as it does") {
    assert(listening(Seq(port("trino", 8443)), ("https", 8443)) === ("https", 8443))
    assert(listening(Seq(port("trino", 8080)), ("http", 8080)) === ("http", 8080))
  }

  test("a port's appProtocol says what it speaks") {
    val ports = Seq(port("metrics", 9209), port("worker", 8443, appProtocol = "https"))
    assert(listening(ports, ("http", 8080)) === ("https", 8443))
  }

  test("a port named for its scheme is taken, the coordinator's scheme first") {
    val ports = Seq(port("http", 8080), port("HTTPS", 8443))
    assert(listening(ports, ("https", 8443)) === ("https", 8443))
    assert(listening(ports, ("http", 8080)) === ("http", 8080))
    assert(listening(Seq(port("metrics", 9209), port("http", 8080)), ("https", 8443)) ===
      ("http", 8080))
  }

  test("the pod's port counts, not the Service's") {
    assert(listening(Seq(port("worker", 80, new IntOrString(8080))), ("https", 8443)) ===
      ("http", 8080))
    val containers = Seq(containerPort("metrics", 9209), containerPort("worker", 8080))
    assert(
      listening(Seq(port("worker", 80, new IntOrString("worker"))), ("https", 8443), containers)
        === ("http", 8080),
      "a named target port is resolved through the pod template")
  }

  test("with nothing to go on, the coordinator's address stands") {
    assert(listening(Seq(port("a", 9209), port("b", 8080)), ("https", 8443)) === ("https", 8443))
    assert(listening(Seq.empty, ("https", 8443)) === ("https", 8443))
  }
}
