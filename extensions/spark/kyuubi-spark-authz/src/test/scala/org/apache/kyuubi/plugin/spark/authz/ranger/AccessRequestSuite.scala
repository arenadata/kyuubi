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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.lang.reflect.{Constructor, Method}
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.QueryPlanningTracker
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._

class AccessRequestSuite extends AnyFunSuite {
// scalastyle:on

  private val bob = UserGroupInformation.createRemoteUser("bob")
  private val resource = AccessResource(ObjectType.TABLE, defaultDb, "src", null)

  private val statement = "SELECT * FROM default.src"
  private val dataFrameCalls = "spark.table(\"default.src\").filter(\"key > 0\")"

  // Only the Spark builds that keep the query texts in the tracker have these: the texts of a
  // query and the method that gives them to its tracker.
  private val textsOfQuery: Try[(Constructor[_], Method)] = Try {
    val texts = Class.forName(classOf[QueryPlanningTracker].getName + "$QueryTexts")
    val setTexts = classOf[QueryPlanningTracker].getMethod("setTexts", texts)
    (texts.getConstructor(classOf[Option[_]], classOf[Function0[_]]), setTexts)
  }

  /** Builds the DataFrame text for the tracker and counts how many times it is asked to. */
  private class DataFrameTextBuilder(text: => Option[String]) extends (() => Option[String]) {
    val calls = new AtomicInteger

    override def apply(): Option[String] = {
      calls.incrementAndGet()
      text
    }
  }

  /** A tracker with the given texts, or a tracker without texts on Spark builds without them. */
  private def trackerWith(
      sqlText: Option[String],
      dataFrameText: () => Option[String]): QueryPlanningTracker = {
    val tracker = new QueryPlanningTracker
    textsOfQuery.foreach { case (texts, setTexts) =>
      setTexts.invoke(tracker, texts.newInstance(sqlText, dataFrameText))
    }
    tracker
  }

  private def buildAccessRequest(): AccessRequest =
    AccessRequest(resource, bob, OperationType.QUERY, AccessType.SELECT)

  private def requestDataWith(tracker: QueryPlanningTracker): String =
    QueryPlanningTracker.withTracker(tracker)(buildAccessRequest()).getRequestData

  // Spark without the texts in the tracker: the requests are built as before.
  private def ifTexts(expected: String): String = if (textsOfQuery.isSuccess) expected else null

  test("no request data outside of query planning") {
    assert(buildAccessRequest().getRequestData === null)
  }

  test("request data is the statement text kept in the query planning tracker") {
    val tracker = trackerWith(Some(statement), () => None)
    assert(requestDataWith(tracker) === ifTexts(statement))
  }

  test("request data is the DataFrame text when the query has no statement text") {
    val dataFrameText = new DataFrameTextBuilder(Some(dataFrameCalls))
    val tracker = trackerWith(None, dataFrameText)
    // A query makes a request per object it accesses, each with the text of the query.
    QueryPlanningTracker.withTracker(tracker) {
      assert(buildAccessRequest().getRequestData === ifTexts(dataFrameCalls))
      assert(buildAccessRequest().getRequestData === ifTexts(dataFrameCalls))
    }
    assert(dataFrameText.calls.get === (if (textsOfQuery.isSuccess) 1 else 0))
  }

  test("the statement text wins over the DataFrame text") {
    val dataFrameText = new DataFrameTextBuilder(Some(dataFrameCalls))
    val tracker = trackerWith(Some(statement), dataFrameText)
    assert(requestDataWith(tracker) === ifTexts(statement))
    // The DataFrame text is not even built for a SQL statement.
    assert(dataFrameText.calls.get === 0)
  }

  test("no request data when the DataFrame text cannot be built") {
    val noText = new DataFrameTextBuilder(None)
    val failedText = new DataFrameTextBuilder(throw new IllegalStateException("unsupported plan"))
    Seq(noText, failedText).foreach { dataFrameText =>
      assert(requestDataWith(trackerWith(None, dataFrameText)) === null)
      assert(dataFrameText.calls.get === (if (textsOfQuery.isSuccess) 1 else 0))
    }
  }

  test("request data comes from the query planned by the current thread only") {
    val tracker = trackerWith(None, new DataFrameTextBuilder(Some(dataFrameCalls)))
    QueryPlanningTracker.withTracker(tracker) {
      // E.g. another session that builds its requests while this query is being planned.
      var otherRequestData = "not built"
      val other = new Thread(() => otherRequestData = buildAccessRequest().getRequestData)
      other.start()
      other.join()
      assert(otherRequestData === null)
      assert(buildAccessRequest().getRequestData === ifTexts(dataFrameCalls))
    }
  }
}
