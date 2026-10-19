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

import java.nio.charset.StandardCharsets

import scala.util.matching.Regex

import org.apache.kyuubi.server.api.HtmlTemplate.Html

/**
 * An HTML template. A {{name}} placeholder is replaced with the value given for it, which is
 * either escaped text or the markup rendered from another template.
 *
 * @param name   the name of the template in error messages
 * @param source the markup of the template
 */
private[api] class HtmlTemplate(name: String, source: String) {

  private val placeholders =
    HtmlTemplate.PLACEHOLDER.findAllMatchIn(source).map(_.group(1)).toSet

  /**
   * @throws IllegalArgumentException unless the values are given for exactly the placeholders of
   *                                  this template
   */
  def render(values: (String, Html)*): Html = {
    val valueOf = values.toMap
    require(
      valueOf.keySet == placeholders,
      s"The HTML template $name takes the values ${placeholders.toSeq.sorted.mkString(", ")}, " +
        s"not ${valueOf.keys.toSeq.sorted.mkString(", ")}")
    new Html(HtmlTemplate.PLACEHOLDER.replaceAllIn(
      source,
      placeholder => Regex.quoteReplacement(valueOf(placeholder.group(1)).markup)))
  }
}

private[api] object HtmlTemplate {

  /** Markup that is safe to put into a page: escaped text or a rendered template. */
  final class Html private[HtmlTemplate] (val markup: String) {
    override def toString: String = markup
  }

  private val PLACEHOLDER = """\{\{(\w+)\}\}""".r

  private val LICENSE_COMMENT = """(?s)\A\s*<!--.*?-->""".r

  /**
   * Loads a template from the class path. The license comment the file starts with is not a part
   * of the template.
   *
   * @param resource the name of the template file relative to this package
   */
  def apply(resource: String): HtmlTemplate = {
    val in = Option(classOf[HtmlTemplate].getResourceAsStream(resource)).getOrElse(
      throw new IllegalStateException(s"The HTML template $resource is not on the class path"))
    val source =
      try new String(in.readAllBytes(), StandardCharsets.UTF_8)
      finally in.close()
    new HtmlTemplate(resource, LICENSE_COMMENT.replaceFirstIn(source, "").trim)
  }

  /** The text, with the characters that HTML reads as markup escaped. */
  def text(value: String): Html =
    new Html(
      value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;"))
}
