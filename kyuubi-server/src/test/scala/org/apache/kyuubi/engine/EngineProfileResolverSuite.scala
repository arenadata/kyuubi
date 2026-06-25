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

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf

class EngineProfileResolverSuite extends KyuubiFunSuite {

  private def newConf(): KyuubiConf = {
    val conf = KyuubiConf(false)
    conf.set("kyuubi.engine.profile.spark3.type", "SPARK_SQL")
    conf.set("kyuubi.engine.profile.spark3.env.SPARK_HOME", "/usr/lib/spark3")
    conf.set("kyuubi.engine.profile.spark4.type", "SPARK_SQL")
    conf.set("kyuubi.engine.profile.spark4.env.SPARK_HOME", "/usr/lib/spark4")
    conf.set("kyuubi.engine.profile.trino.type", "TRINO")
    conf.set("kyuubi.engine.profile.trino.session.engine.trino.connection.url", "http://c:8080")
    conf
  }

  private def resolve(
      conf: KyuubiConf,
      user: String,
      groups: Seq[String],
      requestConf: Map[String, String]): Option[EngineProfile] = {
    EngineProfileResolver.resolve(
      conf,
      EngineProfileRegistry(conf),
      user,
      groups,
      requestConf)
  }

  test("tier 1 - explicit session parameter wins") {
    val conf = newConf()
    conf.set("kyuubi.engine.SPARK_SQL.profile.default", "spark3")
    conf.set("___alice___.kyuubi.engine.profile", "trino")
    val profile = resolve(
      conf,
      "alice",
      Seq("analysts"),
      Map("kyuubi.engine.profile" -> "spark4")).get
    assert(profile.name === "spark4")
    assert(profile.conf("kyuubi.engineEnv.SPARK_HOME") === "/usr/lib/spark4")
  }

  test("tier 2 - user default profile wins over group default and global default") {
    val conf = newConf()
    conf.set("kyuubi.engine.SPARK_SQL.profile.default", "spark3")
    conf.set("___alice___.kyuubi.engine.profile", "spark4")
    conf.set("___analysts___.kyuubi.engine.profile", "trino")
    val profile = resolve(conf, "alice", Seq("analysts"), Map.empty).get
    assert(profile.name === "spark4")
  }

  test("tier 2 - group default profile when no user default") {
    val conf = newConf()
    conf.set("___analysts___.kyuubi.engine.profile", "trino")
    val profile = resolve(conf, "alice", Seq("analysts"), Map.empty).get
    assert(profile.name === "trino")
    assert(profile.conf("kyuubi.engine.type") === "TRINO")
  }

  test("tier 3 - per-engine-type default from request engine type") {
    val conf = newConf()
    conf.set("kyuubi.engine.SPARK_SQL.profile.default", "spark3")
    val profile = resolve(
      conf,
      "alice",
      Seq.empty,
      Map("kyuubi.engine.type" -> "SPARK_SQL")).get
    assert(profile.name === "spark3")
  }

  test("tier 3 - per-engine-type default from user/group default engine type") {
    val conf = newConf()
    conf.set("kyuubi.engine.SPARK_SQL.profile.default", "spark3")
    conf.set("___analysts___.kyuubi.engine.type", "SPARK_SQL")
    val profile = resolve(conf, "alice", Seq("analysts"), Map.empty).get
    assert(profile.name === "spark3")
  }

  test("no profile resolves - returns None for backward compatibility") {
    val conf = newConf()
    // default engine type is SPARK_SQL but no profile.default configured
    assert(resolve(conf, "alice", Seq.empty, Map.empty).isEmpty)
  }

  test("unknown profile name fails fast") {
    val conf = newConf()
    val e = intercept[KyuubiSQLException] {
      resolve(
        conf,
        "alice",
        Seq.empty,
        Map("kyuubi.engine.profile" -> "does-not-exist"))
    }
    assert(e.getMessage.contains("does-not-exist"))
  }

  test("unknown profile name falls back to None under LOG policy") {
    val conf = newConf()
    conf.set("kyuubi.engine.profiles.unknown.strategy", "LOG")
    assert(resolve(
      conf,
      "alice",
      Seq.empty,
      Map("kyuubi.engine.profile" -> "does-not-exist")).isEmpty)
  }
}
