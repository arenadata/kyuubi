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

package org.apache.kyuubi.system.tests

import java.util.UUID

import io.qameta.allure.Feature
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}

import org.apache.kyuubi.system.tests.model.EngineProfile

@Feature("Engine profiles end-to-end")
class EngineProfilesIT extends KyuubiSystemContainerizedIT {

  private var warehouseTable: String = _
  private var postgresTable: String = _

  @AfterEach
  def cleanup(): Unit = {
    if (warehouseTable != null) {
      try {
        warehouseService.exec(s"DROP TABLE IF EXISTS hive.default.$warehouseTable")
      } catch {
        case _: RuntimeException => // best-effort cleanup
      }
      warehouseTable = null
    }
    if (postgresTable != null) {
      try {
        postgresService.exec(s"DROP TABLE IF EXISTS $postgresTable")
      } catch {
        case _: RuntimeException => // best-effort cleanup
      }
      postgresTable = null
    }
  }

  @Test
  def testEngineProfilesReadSeededData(): Unit = {
    warehouseTable = uniqueTable("wh")
    postgresTable = uniqueTable("pg")

    seedWarehouse(warehouseTable)
    seedPostgres(postgresTable)

    assertWarehouseReadable(EngineProfile.SPARK3, warehouseTable)
    assertWarehouseReadable(EngineProfile.SPARK4, warehouseTable)
    assertWarehouseReadable(EngineProfile.HIVE, warehouseTable)
    assertWarehouseReadable(EngineProfile.TRINO, warehouseTable)
    assertPostgresReadable(postgresTable)
  }

  private def seedWarehouse(table: String): Unit = {
    warehouseService.exec(s"CREATE TABLE hive.default.$table (id bigint, label varchar)")
    warehouseService.exec(
      s"INSERT INTO hive.default.$table VALUES (1, '${EngineProfilesIT.LABEL}')")
  }

  private def seedPostgres(table: String): Unit = {
    postgresService.exec(s"CREATE TABLE $table (id INT PRIMARY KEY, label TEXT NOT NULL)")
    postgresService.exec(
      s"INSERT INTO $table (id, label) VALUES (1, '${EngineProfilesIT.LABEL}')")
  }

  private def assertWarehouseReadable(profile: EngineProfile, table: String): Unit = {
    val sql =
      if (profile == EngineProfile.TRINO) {
        s"SELECT id, label FROM hive.default.$table ORDER BY id"
      } else {
        s"SELECT id, label FROM $table ORDER BY id"
      }
    assertRow(kyuubiService.query(profile, sql))
  }

  private def assertPostgresReadable(table: String): Unit = {
    assertRow(
      kyuubiService.query(
        EngineProfile.POSTGRES,
        s"SELECT id, label FROM $table ORDER BY id"))
  }

  private def assertRow(rows: Seq[Map[String, AnyRef]]): Unit = {
    assertFalse(rows.isEmpty, "Expected at least one row")
    assertEquals(1, rows.size, "Expected exactly one seeded row")
    val row = rows.head
    assertEquals(1L, row("id").asInstanceOf[Number].longValue())
    assertEquals(EngineProfilesIT.LABEL, String.valueOf(row("label")))
  }

  private def uniqueTable(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.replace("-", "").substring(0, 12)
  }
}

object EngineProfilesIT {
  private val LABEL = "kyuubi-system-tests"
}
