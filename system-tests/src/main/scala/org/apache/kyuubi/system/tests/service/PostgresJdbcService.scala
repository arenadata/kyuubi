/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.system.tests.service

import java.sql.{Connection, DriverManager, SQLException}

import org.slf4j.LoggerFactory

import org.apache.kyuubi.system.tests.model.Component

class PostgresJdbcService(compose: DockerComposeService) {

  private val log = LoggerFactory.getLogger(classOf[PostgresJdbcService])

  Class.forName("org.postgresql.Driver")

  def exec(sql: String): Unit = {
    log.info(s"Postgres exec: $sql")
    try {
      withConnection { connection =>
        val statement = connection.createStatement()
        try {
          statement.execute(sql)
        } finally {
          statement.close()
        }
      }
    } catch {
      case e: SQLException =>
        throw new IllegalStateException(s"Failed to execute on postgres-app: $sql", e)
    }
  }

  private def withConnection[T](f: Connection => T): T = {
    val connection = open()
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  private def open() = {
    val host = compose.getServiceHost(Component.POSTGRES_APP)
    val port = compose.getServicePort(Component.POSTGRES_APP)
    val url = s"jdbc:postgresql://$host:$port/app"
    DriverManager.getConnection(url, PostgresJdbcService.USER, PostgresJdbcService.PASSWORD)
  }
}

object PostgresJdbcService {
  private val USER = "app"
  private val PASSWORD = "app"
}
