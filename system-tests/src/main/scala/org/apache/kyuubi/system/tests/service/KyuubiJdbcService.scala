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

package org.apache.kyuubi.system.tests.service

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.{LinkedHashMap => JLinkedHashMap, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.slf4j.LoggerFactory

import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.system.tests.model.{Component, EngineProfile}
import org.apache.kyuubi.system.tests.util.constant.ConfConstants.ENGINE_PROFILE

class KyuubiJdbcService(compose: DockerComposeService) {

  private val log = LoggerFactory.getLogger(classOf[KyuubiJdbcService])

  Class.forName(classOf[KyuubiHiveDriver].getName)

  def jdbcUrl(database: String = "default"): String = {
    val host = compose.getServiceHost(Component.KYUUBI)
    val port = compose.getServicePort(Component.KYUUBI)
    s"jdbc:hive2://$host:$port/$database"
  }

  def jdbcUrl(profile: EngineProfile): String = {
    s"${jdbcUrl()}?$ENGINE_PROFILE=${profile.profileName}"
  }

  def withConnection[T](
      user: String = KyuubiJdbcService.USER,
      sessionConf: Map[String, String] = Map.empty)(f: Connection => T): T = {
    val connection = openConnection(sessionConf, user)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def openConnection(
      sessionConf: Map[String, String] = Map.empty,
      user: String = KyuubiJdbcService.USER): Connection = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", "")
    // Hive JDBC maps `hiveconf:*` Properties into OpenSession as set:hiveconf:*
    // (plain keys are ignored; URL `?key=value` is the other supported path).
    sessionConf.foreach { case (k, v) =>
      props.setProperty("hiveconf:" + k, v)
    }
    DriverManager.getConnection(jdbcUrl(), props)
  }

  def exec(profile: EngineProfile, sql: String): Unit = {
    exec(Map(ENGINE_PROFILE -> profile.profileName), sql)
  }

  def exec(
      sessionConf: Map[String, String],
      sql: String,
      user: String = KyuubiJdbcService.USER): Unit = {
    log.info(s"Kyuubi[$user][${sessionConf.mkString(",")}] exec: $sql")
    try {
      withConnection(user, sessionConf) { connection =>
        val statement = connection.createStatement()
        try {
          statement.execute(sql)
        } finally {
          statement.close()
        }
      }
    } catch {
      case e: SQLException =>
        throw new IllegalStateException(s"Failed to execute via Kyuubi: $sql", e)
    }
  }

  def query(profile: EngineProfile, sql: String): Seq[Map[String, AnyRef]] = {
    query(Map(ENGINE_PROFILE -> profile.profileName), sql)
  }

  def query(
      sessionConf: Map[String, String],
      sql: String,
      user: String = KyuubiJdbcService.USER): Seq[Map[String, AnyRef]] = {
    log.info(s"Kyuubi[$user][${sessionConf.mkString(",")}] query: $sql")
    try {
      withConnection(user, sessionConf) { connection =>
        query(connection, sql)
      }
    } catch {
      case e: SQLException =>
        throw new IllegalStateException(s"Failed to query via Kyuubi: $sql", e)
    }
  }

  def query(connection: Connection, sql: String): Seq[Map[String, AnyRef]] = {
    val statement = connection.createStatement()
    try {
      val resultSet = statement.executeQuery(sql)
      try {
        KyuubiJdbcService.toRows(resultSet)
      } finally {
        resultSet.close()
      }
    } finally {
      statement.close()
    }
  }
}

object KyuubiJdbcService {
  val USER = "anonymous"

  private def toRows(resultSet: ResultSet): Seq[Map[String, AnyRef]] = {
    val meta = resultSet.getMetaData
    val columnCount = meta.getColumnCount
    val rows = ArrayBuffer.empty[Map[String, AnyRef]]
    while (resultSet.next()) {
      val row = new JLinkedHashMap[String, AnyRef]()
      var i = 1
      while (i <= columnCount) {
        row.put(meta.getColumnLabel(i).toLowerCase, resultSet.getObject(i))
        i += 1
      }
      rows += row.asScala.toMap
    }
    rows.toSeq
  }
}
