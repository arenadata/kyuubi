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

package org.apache.kyuubi.gateway.capacity

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.cluster.ClusterRef

class TrinoClusterClientsSuite extends KyuubiFunSuite {

  private val PasswordKey = KyuubiConf.ENGINE_TRINO_CONNECTION_PASSWORD.key

  test("a cluster keeps its client from one pass to the next") {
    val clients = new TrinoClusterClients(KyuubiConf(false), "reconciler")
    val a = ClusterRef("a", "trino", "http://a:8080")

    assert(clients(a) eq clients(a), "a Kerberos client would log in again on every pass")
    assert(!(clients(a) eq clients(ClusterRef("b", "trino", "http://b:8080"))))
    assert(
      !(clients(a) eq clients(a.copy(sessionConf = Map("kyuubi.engine.trino.x" -> "y")))),
      "a cluster whose settings changed is reached with the new settings")
  }

  test("the cluster's own settings and scheme reach the client, as for a session") {
    val clients = new TrinoClusterClients(KyuubiConf(false), "reconciler")
    val secured = Map(PasswordKey -> "secret")

    // The session builder refuses a password over plain HTTP. Seeing that
    // refusal proves the cluster's settings and its URL's scheme were used.
    val e = intercept[IllegalArgumentException] {
      clients(ClusterRef("a", "trino", "http://a:8080", sessionConf = secured))
    }
    assert(e.getMessage.contains("HTTPS"))
    clients(ClusterRef("a", "trino", "https://a:8443", sessionConf = secured))
  }

  test("the gateway's connection settings apply to every cluster") {
    val conf = KyuubiConf(false).set(PasswordKey, "secret")
    val clients = new TrinoClusterClients(conf, "reconciler")

    intercept[IllegalArgumentException](clients(ClusterRef("a", "trino", "http://a:8080")))
  }
}
