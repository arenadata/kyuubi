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

package org.apache.kyuubi.server.api

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.api.HtmlTemplate.text

class HtmlTemplateSuite extends KyuubiFunSuite {

  test("escape text values") {
    val template = new HtmlTemplate("test", "<p>{{value}}</p>")
    assert(template.render("value" -> text("""<a href="x">'Tom' & Jerry</a>""")).markup ===
      "<p>&lt;a href=&quot;x&quot;&gt;&#39;Tom&#39; &amp; Jerry&lt;/a&gt;</p>")
  }

  test("put the markup rendered from another template as is") {
    val code = new HtmlTemplate("code", "<code>{{value}}</code>").render("value" -> text("a<b"))
    val template = new HtmlTemplate("test", "<p>{{code}}</p>")
    assert(template.render("code" -> code).markup === "<p><code>a&lt;b</code></p>")
  }

  test("replace every occurrence of a placeholder") {
    val template = new HtmlTemplate("test", "<title>{{title}}</title><h1>{{title}}</h1>")
    assert(template.render("title" -> text("Denied")).markup ===
      "<title>Denied</title><h1>Denied</h1>")
  }

  test("keep the values as they are") {
    val template = new HtmlTemplate("test", "<p>{{value}}</p>")
    assert(template.render("value" -> text("""$1 \1 {{value}}""")).markup ===
      """<p>$1 \1 {{value}}</p>""")
  }

  test("require the values for exactly the placeholders of a template") {
    val template = new HtmlTemplate("test", "{{a}} {{b}}")
    val missing = intercept[IllegalArgumentException](template.render("a" -> text("1")))
    assert(missing.getMessage.contains("takes the values a, b, not a"))
    intercept[IllegalArgumentException] {
      template.render("a" -> text("1"), "b" -> text("2"), "c" -> text("3"))
    }
    assert(new HtmlTemplate("test", "<br>").render().markup === "<br>")
  }

  test("load a template from the class path without its license comment") {
    val template = HtmlTemplate("engine-ui-proxy/invalid-address.html")
    assert(template.render().markup ===
      "The URL must include an engine host and port, for example\n" +
      "<code>/engine-ui/spark.example.com:4040/</code>.")
  }

  test("fail to load a template that is not on the class path") {
    val e = intercept[IllegalStateException](HtmlTemplate("engine-ui-proxy/missing.html"))
    assert(e.getMessage.contains("engine-ui-proxy/missing.html"))
  }
}
