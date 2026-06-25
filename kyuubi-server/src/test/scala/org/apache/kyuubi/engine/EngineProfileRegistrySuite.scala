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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class EngineProfileRegistrySuite extends KyuubiFunSuite {

  test("names and bucket materialization") {
    val conf = KyuubiConf(false)
    conf.set("kyuubi.engine.profile.spark4.type", "SPARK_SQL")
    conf.set("kyuubi.engine.profile.spark4.env.SPARK_HOME", "/usr/lib/spark4")
    conf.set("kyuubi.engine.profile.spark4.env.SPARK_CONF_DIR", "/etc/spark4/conf")
    conf.set("kyuubi.engine.profile.spark4.conf.spark.executor.memory", "1g")
    conf.set("kyuubi.engine.profile.trino.type", "trino")
    conf.set("kyuubi.engine.profile.trino.session.engine.trino.connection.url", "http://c:8080")

    val registry = EngineProfileRegistry(conf)
    assert(registry.names === Set("spark4", "trino"))

    val spark4 = registry.get("spark4").get
    assert(spark4.name === "spark4")
    assert(spark4.conf("kyuubi.engine.type") === "SPARK_SQL")
    assert(spark4.conf("kyuubi.engineEnv.SPARK_HOME") === "/usr/lib/spark4")
    assert(spark4.conf("kyuubi.engineEnv.SPARK_CONF_DIR") === "/etc/spark4/conf")
    // engine-native conf.<rest> is mapped verbatim
    assert(spark4.conf("spark.executor.memory") === "1g")

    val trino = registry.get("trino").get
    // type is normalized to upper case, session.<rest> maps to kyuubi.session.<rest>
    assert(trino.conf("kyuubi.engine.type") === "TRINO")
    assert(trino.conf("kyuubi.session.engine.trino.connection.url") === "http://c:8080")
  }

  test("undefined profile returns None") {
    val registry = EngineProfileRegistry(KyuubiConf(false))
    assert(registry.get("missing").isEmpty)
    assert(registry.names.isEmpty)
  }

  test("invalid engine type fails fast") {
    val conf = KyuubiConf(false)
    conf.set("kyuubi.engine.profile.bad.type", "NOT_AN_ENGINE")
    val e = intercept[IllegalArgumentException](EngineProfileRegistry(conf))
    assert(e.getMessage.contains("Invalid engine type"))
  }

  test("unknown bucket fails fast") {
    val conf = KyuubiConf(false)
    conf.set("kyuubi.engine.profile.bad.foo.bar", "baz")
    val e = intercept[IllegalArgumentException](EngineProfileRegistry(conf))
    assert(e.getMessage.contains("Unknown engine profile bucket"))
  }
}
