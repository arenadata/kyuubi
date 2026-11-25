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

package org.apache.kyuubi.engine.spark.connect

import org.apache.kyuubi.KyuubiFunSuite

class SparkConnectTlsSuite extends KyuubiFunSuite {

  // the Maven property of the Spark profile, passed on by the scalatest-maven-plugin
  private val supportProperty = "spark.connect.tls.support"

  test("the engine serves Spark Connect with TLS exactly when the Spark profile expects it") {
    val expected = sys.props.get(supportProperty)
    assume(expected.isDefined, s"$supportProperty is only set when Maven runs the tests")
    assert(SparkConnectTls.supported === expected.contains("native"))
  }
}
