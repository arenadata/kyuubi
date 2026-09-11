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
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.sizing.{QuerySizer, SizingPolicy}
import org.apache.kyuubi.operation.OperationHandle

class SessionAdmissionSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val cluster = ClusterRef("c", "trino", "http://c:8080")
  private val capacity = ClusterCapacity(10 * GB, workers = 2, maxWorkers = 2)

  /** Each query is sized at a full cluster, so the second one never fits. */
  private def fixture = {
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory)
    val gate = new AdmissionGate(
      accountant,
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capacity),
      (_, _) => s"""{"estimates":[{"memoryCost":${20 * GB}.0}],"children":[]}""")
    (accountant, new SessionAdmission(gate, cluster))
  }

  test("a started statement holds its reservation until the operation closes") {
    val (accountant, admission) = fixture
    val handle = admission.admitAndRun("SELECT 1")(() => OperationHandle())
    assert(admission.outstanding === 1)
    assert(accountant.reservedBytes("c") === 20 * GB)

    admission.finished(handle)
    assert(admission.outstanding === 0)
    assert(accountant.reservedBytes("c") === 0)
  }

  test("a refusal names what would help") {
    val (_, admission) = fixture
    admission.admitAndRun("SELECT 1")(() => OperationHandle())
    val refused = intercept[AdmissionRefused](
      admission.admitAndRun("SELECT 2")(() => OperationHandle()))
    assert(refused.denial === AdmissionDenial.Busy)
    assert(refused.getMessage.contains("Retry when the queries in flight finish"))
  }

  test("a failed start does not leak the reservation") {
    val (accountant, admission) = fixture
    intercept[RuntimeException](
      admission.admitAndRun("SELECT 1")(() => throw new RuntimeException("engine refused")))
    assert(accountant.reservedBytes("c") === 0,
      "a reservation must not outlive the start it was taken for")

    // The proof it was really released: the next statement is admitted.
    assert(admission.admitAndRun("SELECT 2")(() => OperationHandle()) != null)
  }

  test("closing an unknown handle is harmless") {
    val (accountant, admission) = fixture
    admission.finished(OperationHandle())
    assert(accountant.reservedBytes("c") === 0)
  }

  test("closing the session releases what is still in flight") {
    val (accountant, admission) = fixture
    admission.admitAndRun("SELECT 1")(() => OperationHandle())
    assert(accountant.reservedBytes("c") === 20 * GB)

    // A client that disconnects mid-query would otherwise leave the cluster
    // looking permanently smaller than it is.
    admission.releaseAll()
    assert(accountant.reservedBytes("c") === 0)
    assert(admission.outstanding === 0)
  }

  test("refusal messages distinguish the three denials") {
    assert(AdmissionRefused.message("c", AdmissionDenial.Busy).contains("no free capacity"))
    assert(AdmissionRefused.message("c", AdmissionDenial.NeedsScaleUp(6)).contains("6 workers"))

    val tooLarge = AdmissionRefused.message("c", AdmissionDenial.TooLarge(20, 10))
    assert(tooLarge.contains("20 workers") && tooLarge.contains("at most 10"))
    assert(tooLarge.contains("Neither waiting nor scaling will help"))
  }
}
