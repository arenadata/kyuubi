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

package org.apache.kyuubi.server.flight

import java.util.Locale

/**
 * SQL classification for Flight SQL prepared statements.
 *
 * Kyuubi cannot describe a statement's result without running it, but Flight SQL clients
 * (notably the Arrow Flight SQL JDBC driver) decide between the query path and the update path
 * from the dataset schema returned by CreatePreparedStatement: empty schema -> update. Queries
 * are therefore executed at prepare time to obtain their schema, while everything else is
 * deferred to execution so that preparing never has side effects. A `WITH` statement whose
 * main statement is DML (`WITH s AS (...) INSERT ...`) is not a query.
 */
object KyuubiFlightSqlStatements {

  // Not `FROM`: in Hive `FROM t INSERT INTO ...` is an update.
  private val QUERY_KEYWORDS =
    Set("select", "with", "values", "show", "describe", "desc", "explain", "table")

  // DML that may follow a WITH clause (Hive, Impala, Spark: `WITH s AS (...) INSERT ...`)
  private val DML_KEYWORDS = Set("insert", "upsert", "update", "delete", "merge")

  /**
   * True when `sql` produces a result set: its first keyword is a query keyword and, for
   * `WITH`, the statement after the common table expressions is not DML.
   */
  def isQuery(sql: String): Boolean = firstKeyword(sql) match {
    case Some("with") => !topLevelWords(sql).exists(DML_KEYWORDS.contains)
    case Some(keyword) => QUERY_KEYWORDS.contains(keyword)
    case None => false
  }

  /**
   * Words outside parentheses, lower case. Comments, string literals and quoted identifiers
   * are skipped, so the bodies of common table expressions and quoted names never match.
   */
  private[flight] def topLevelWords(sql: String): Seq[String] = {
    val words = Seq.newBuilder[String]
    val n = sql.length
    var depth = 0
    var i = 0
    while (i < n) {
      val c = sql.charAt(i)
      if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        val eol = sql.indexOf('\n', i)
        i = if (eol < 0) n else eol + 1
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        val end = sql.indexOf("*/", i + 2)
        i = if (end < 0) n else end + 2
      } else if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(sql, i)
      } else if (c == '(') {
        depth += 1
        i += 1
      } else if (c == ')') {
        depth = math.max(depth - 1, 0)
        i += 1
      } else if (Character.isLetter(c) || c == '_') {
        var j = i
        while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j += 1
        if (depth == 0) words += sql.substring(i, j).toLowerCase(Locale.ROOT)
        i = j
      } else {
        i += 1
      }
    }
    words.result()
  }

  /** Index after the literal or quoted identifier starting at `start` (doubled or \-escaped). */
  private def skipQuoted(sql: String, start: Int): Int = {
    val quote = sql.charAt(start)
    val n = sql.length
    var i = start + 1
    while (i < n) {
      val c = sql.charAt(i)
      if (c == '\\' && quote != '`') {
        i += 2
      } else if (c == quote) {
        if (i + 1 < n && sql.charAt(i + 1) == quote) i += 2 else return i + 1
      } else {
        i += 1
      }
    }
    n
  }

  /** First keyword, lower case, skipping whitespace, `--`/`/* */` comments and `(`. */
  def firstKeyword(sql: String): Option[String] = {
    if (sql == null) return None
    var i = 0
    val n = sql.length
    var skipping = true
    while (skipping && i < n) {
      val c = sql.charAt(i)
      if (Character.isWhitespace(c) || c == '(') {
        i += 1
      } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        val eol = sql.indexOf('\n', i)
        i = if (eol < 0) n else eol + 1
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        val end = sql.indexOf("*/", i + 2)
        i = if (end < 0) n else end + 2
      } else {
        skipping = false
      }
    }
    var j = i
    while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j += 1
    if (j > i) Some(sql.substring(i, j).toLowerCase(Locale.ROOT)) else None
  }
}
